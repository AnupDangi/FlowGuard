#!/usr/bin/env python3
"""Load food items data into PostgreSQL database."""
import json
import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "food_catalog",
    "user": "flowguard",
    "password": "flowguard123"
}

def load_food_data():
    """Load food items from JSON into database."""
    try:
        # Connect to database
        print("📡 Connecting to PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        
        # Check current count
        cursor.execute("SELECT COUNT(*) as count FROM foods")
        result = cursor.fetchone()
        current_count = result["count"]
        
        if current_count > 0:
            print(f"ℹ️  Database already has {current_count} food items")
            response = input("Do you want to reload data? (y/n): ")
            if response.lower() != 'y':
                print("❌ Aborted")
                return
            
            # Clear existing data
            print("🗑️  Clearing existing data...")
            cursor.execute("TRUNCATE TABLE foods RESTART IDENTITY CASCADE")
            conn.commit()
        
        # Load JSON file
        json_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "data", "food_items.json"
        )
        
        if not os.path.exists(json_path):
            print(f"❌ Food items JSON not found at {json_path}")
            return 1
        
        print(f"📂 Loading data from {json_path}...")
        with open(json_path, "r") as f:
            food_items = json.load(f)
        
        # Insert into database
        insert_query = """
            INSERT INTO foods (food_id, name, category, price, description, image_url, is_available)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        for item in food_items:
            cursor.execute(insert_query, (
                item["food_id"],
                item["name"],
                item["category"],
                item["price"],
                item.get("description", ""),
                item["image_url"],
                item.get("is_available", True)
            ))
        
        conn.commit()
        print(f"✅ Successfully loaded {len(food_items)} food items into database")
        
        # Verify
        cursor.execute("SELECT COUNT(*) as count FROM foods")
        result = cursor.fetchone()
        print(f"✅ Verification: Database now has {result['count']} food items")
        
        cursor.close()
        conn.close()
        return 0
    
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(load_food_data())
