"""Test client for Food Catalog API."""
import requests

BASE_URL = "http://localhost:8001"


def test_health():
    """Test health endpoint."""
    resp = requests.get(f"{BASE_URL}/health")
    print("✅ Health Check:", resp.json())


def test_get_all_foods():
    """Test getting all foods."""
    resp = requests.get(f"{BASE_URL}/api/foods")
    data = resp.json()
    print(f"\n✅ Total Foods: {data['total']}")
    print(f"✅ Sample: {data['items'][0]['name']} - ₹{data['items'][0]['price']}")


def test_get_by_category():
    """Test filtering by category."""
    resp = requests.get(f"{BASE_URL}/api/foods?category=Biryani")
    data = resp.json()
    print(f"\n✅ Biryani items: {data['total']}")
    for item in data['items']:
        print(f"   - {item['name']}: ₹{item['price']}")


def test_get_categories():
    """Test getting categories."""
    resp = requests.get(f"{BASE_URL}/api/foods/categories/list")
    categories = resp.json()
    print(f"\n✅ Categories: {', '.join(categories)}")


def test_get_by_id():
    """Test getting food by ID."""
    resp = requests.get(f"{BASE_URL}/api/foods/1")
    item = resp.json()
    print(f"\n✅ Food #1: {item['name']} ({item['category']}) - ₹{item['price']}")


if __name__ == "__main__":
    print("="*60)
    print("Testing Food Catalog API")
    print("="*60)
    
    try:
        test_health()
        test_get_all_foods()
        test_get_categories()
        test_get_by_category()
        test_get_by_id()
        
        print("\n" + "="*60)
        print("✅ All tests passed!")
        print("="*60)
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
