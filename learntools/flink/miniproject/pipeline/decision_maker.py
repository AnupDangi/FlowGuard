"""
Decision Logic
Translates risk scores into actionable decisions.
Applies threshold-based rules and business logic.
"""
from pyflink.datastream.functions import MapFunction
from typing import Dict, Any
from enum import Enum


class FraudDecision(str, Enum):
    """Possible fraud decision outcomes"""
    ALLOW = "ALLOW"
    FLAG = "FLAG"
    BLOCK = "BLOCK"


class DecisionMaker(MapFunction):
    """
    Applies threshold-based decision rules to risk scores.
    
    Decision thresholds:
        - ALLOW: Risk score < low_threshold (safe transaction)
        - FLAG: low_threshold <= Risk score < high_threshold (manual review)
        - BLOCK: Risk score >= high_threshold (reject immediately)
    """

    def __init__(self, low_threshold: int = 40, high_threshold: int = 70):
        """
        Args:
            low_threshold: Minimum score to flag for review (default: 40)
            high_threshold: Minimum score to block transaction (default: 70)
        """
        self.low_threshold = low_threshold
        self.high_threshold = high_threshold

    def map(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply decision rules based on total risk score.
        
        Expected input:
            - total_risk_score: Combined risk from all detectors
            
        Output:
            - fraud_decision: ALLOW/FLAG/BLOCK
            - decision_reason: Human-readable explanation
            - requires_manual_review: Boolean flag
        """
        risk_score = event.get("total_risk_score", 0)

        # Apply threshold-based rules
        if risk_score >= self.high_threshold:
            decision = FraudDecision.BLOCK
            requires_review = False
            reason = self._build_reason(event, "high_risk")
        elif risk_score >= self.low_threshold:
            decision = FraudDecision.FLAG
            requires_review = True
            reason = self._build_reason(event, "moderate_risk")
        else:
            decision = FraudDecision.ALLOW
            requires_review = False
            reason = "Normal behavior"

        # Add decision fields to event
        event["fraud_decision"] = decision.value
        event["decision_reason"] = reason
        event["requires_manual_review"] = requires_review
        event["decision_timestamp"] = event.get("event_time")

        return event

    def _build_reason(self, event: Dict[str, Any], risk_level: str) -> str:
        """
        Build human-readable explanation for the decision.
        Identifies which rules were triggered.
        """
        reasons = []

        # Check velocity violation
        if event.get("velocity_threshold_exceeded", False):
            count = event.get("velocity_order_count", 0)
            reasons.append(f"High order velocity: {count} orders in 60s")

        # Check device violation
        if event.get("device_threshold_exceeded", False):
            users = event.get("device_unique_users", 0)
            reasons.append(f"Suspicious device: {users} users in 5min")

        # Check ML flag (future)
        ml_score = event.get("ml_risk_score", 0)
        if ml_score > 50:
            reasons.append(f"ML model flagged: score={ml_score}")

        if reasons:
            return f"{risk_level.replace('_', ' ').title()}: {', '.join(reasons)}"
        else:
            return f"{risk_level.replace('_', ' ').title()}: Multiple indicators"


class DecisionRouter(MapFunction):
    """
    Routes decisions to appropriate downstream systems.
    
    Examples:
        - ALLOW: Pass to order processing system
        - FLAG: Send to fraud analyst queue
        - BLOCK: Log and notify user
    """

    def map(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add routing metadata based on decision.
        """
        decision = event.get("fraud_decision", "ALLOW")

        routing_config = {
            FraudDecision.ALLOW.value: {
                "destination": "order_processing",
                "priority": "normal",
                "notification_required": False
            },
            FraudDecision.FLAG.value: {
                "destination": "fraud_analyst_queue",
                "priority": "high",
                "notification_required": True
            },
            FraudDecision.BLOCK.value: {
                "destination": "blocked_transactions_log",
                "priority": "urgent",
                "notification_required": True
            }
        }

        event["routing"] = routing_config.get(decision, routing_config[FraudDecision.ALLOW.value])

        return event
