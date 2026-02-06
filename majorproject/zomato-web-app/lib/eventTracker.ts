/**
 * Event tracking utility for FlowGuard
 * Sends events to Events Gateway (port 8000)
 */

const EVENTS_GATEWAY_URL = 'http://localhost:8000';

interface OrderEventPayload {
  event_id: string;
  user_id: number;
  order_id: string;
  item_id: number;
  item_name: string;
  price: number;
  timestamp: string;
}

interface ClickEventPayload {
  event_id: string;
  user_id: number;
  ad_id: string;
  is_click: boolean;
  timestamp: string;
}

// Generate session-based user ID
export function getUserId(): string {
  // Check if running in browser (not SSR)
  if (typeof window === 'undefined') {
    return 'user_ssr';
  }
  
  let userId = localStorage.getItem('flowguard_user_id');
  if (!userId) {
    userId = `user_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    localStorage.setItem('flowguard_user_id', userId);
  }
  return userId;
}

// Get numeric user ID (hash of string ID)
function getNumericUserId(): number {
  const userId = getUserId();
  // Simple hash to convert string to number
  let hash = 0;
  for (let i = 0; i < userId.length; i++) {
    hash = ((hash << 5) - hash) + userId.charCodeAt(i);
    hash |= 0; // Convert to 32bit integer
  }
  return Math.abs(hash);
}

// Generate event ID
function generateEventId(): string {
  return `evt_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

// Generate order ID
function generateOrderId(): string {
  return `order_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

// Send order event
export async function trackOrderEvent(itemId: number, itemName: string, price: number): Promise<boolean> {
  try {
    const payload: OrderEventPayload = {
      event_id: generateEventId(),
      user_id: getNumericUserId(),
      order_id: generateOrderId(),
      item_id: itemId,
      item_name: itemName,
      price: price,
      timestamp: new Date().toISOString(),
    };

    const response = await fetch(`${EVENTS_GATEWAY_URL}/api/v1/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      console.error('Failed to track order:', await response.text());
      return false;
    }

    console.log('✅ Order event tracked:', payload);
    return true;
  } catch (error) {
    console.error('Error tracking order:', error);
    return false;
  }
}

// Send click event
export async function trackClickEvent(adId: string, isClick: boolean): Promise<boolean> {
  try {
    const payload: ClickEventPayload = {
      event_id: generateEventId(),
      user_id: getNumericUserId(),
      ad_id: adId,
      is_click: isClick,
      timestamp: new Date().toISOString(),
    };

    const response = await fetch(`${EVENTS_GATEWAY_URL}/api/v1/clicks`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      console.error('Failed to track click:', await response.text());
      return false;
    }

    console.log('✅ Click event tracked:', payload);
    return true;
  } catch (error) {
    console.error('Error tracking click:', error);
    return false;
  }
}
