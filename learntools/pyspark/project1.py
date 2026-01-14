## Goal: Learn execution plans, shuffles, partitions, skew, caching

"""
Problem 1: 
    A user-event analytics pipeline that looks simple but hides expensive operations.
    
    Raw Events (CSV) file for now 
    ↓
    Filter
    ↓
    GroupBy (⚠ shuffle)
    ↓
    Join with Users (⚠ shuffle / broadcast)
    ↓
    Aggregation
    ↓
    Parquet Output
"""
