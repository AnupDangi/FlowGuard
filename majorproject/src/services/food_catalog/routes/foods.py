"""Foods API routes - GET only."""
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..db import get_db_cursor
from ..models import FoodItem, FoodListResponse


router = APIRouter(prefix="/api/foods", tags=["Foods"])


@router.get("", response_model=FoodListResponse)
async def get_foods(
    category: Optional[str] = Query(None, description="Filter by category"),
    available_only: bool = Query(True, description="Show only available items")
):
    """
    Get all food items from the catalog.
    
    - **category**: Optional filter by category (e.g., 'Biryani', 'Pizza')
    - **available_only**: Show only available items (default: true)
    """
    try:
        with get_db_cursor() as cursor:
            # Build query with filters
            query = "SELECT * FROM foods WHERE 1=1"
            params = []
            
            if available_only:
                query += " AND is_available = %s"
                params.append(True)
            
            if category:
                query += " AND category = %s"
                params.append(category)
            
            query += " ORDER BY food_id"
            
            # Execute query
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Convert to Pydantic models
            items = [FoodItem(**dict(row)) for row in rows]
            
            return FoodListResponse(total=len(items), items=items)
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )


@router.get("/{food_id}", response_model=FoodItem)
async def get_food_by_id(food_id: int):
    """
    Get a specific food item by ID.
    
    - **food_id**: Unique food identifier
    """
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT * FROM foods WHERE food_id = %s", (food_id,))
            row = cursor.fetchone()
            
            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Food item with id={food_id} not found"
                )
            
            return FoodItem(**dict(row))
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )


@router.get("/categories/list", response_model=list[str])
async def get_categories():
    """
    Get all available food categories.
    
    Returns distinct categories from the foods table.
    """
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT DISTINCT category FROM foods ORDER BY category")
            rows = cursor.fetchall()
            return [row["category"] for row in rows]
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )
