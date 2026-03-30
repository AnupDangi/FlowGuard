"""Auth router for custom JWT-based authentication."""

from datetime import datetime
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, EmailStr, Field

from src.services.events_gateway.db import get_db_cursor
from src.services.events_gateway.security import (
    hash_password,
    verify_password,
    create_access_token,
    get_current_user,
)
from src.services.events_gateway.metrics.runtime_metrics import inc_counter

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


class SignupRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)
    name: str = Field(..., min_length=2, max_length=100)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)


def ensure_auth_tables():
    """Ensure auth tables exist for existing DBs."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                name VARCHAR(255) NOT NULL,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_is_active ON users(is_active)")


@router.post("/signup")
async def signup(payload: SignupRequest) -> Dict[str, Any]:
    with get_db_cursor() as cursor:
        cursor.execute("SELECT id FROM users WHERE email = %s", (payload.email,))
        if cursor.fetchone():
            raise HTTPException(status_code=409, detail="Email already registered")

        cursor.execute(
            """
            INSERT INTO users (email, password_hash, name, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, true, %s, %s)
            RETURNING id, email, name, is_active, created_at
            """,
            (
                payload.email,
                hash_password(payload.password),
                payload.name,
                datetime.utcnow(),
                datetime.utcnow(),
            ),
        )
        user = cursor.fetchone()

    token = create_access_token(user["id"], user["email"])
    inc_counter("auth.signup.success")
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": user["id"],
            "email": user["email"],
            "name": user["name"],
            "is_active": user["is_active"],
            "created_at": user["created_at"],
        },
    }


@router.post("/login")
async def login(payload: LoginRequest) -> Dict[str, Any]:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT id, email, name, password_hash, is_active, created_at
            FROM users
            WHERE email = %s
            """,
            (payload.email,),
        )
        user = cursor.fetchone()

    if not user or not verify_password(payload.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    if not user["is_active"]:
        raise HTTPException(status_code=403, detail="User is inactive")

    token = create_access_token(user["id"], user["email"])
    inc_counter("auth.login.success")
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": user["id"],
            "email": user["email"],
            "name": user["name"],
            "is_active": user["is_active"],
            "created_at": user["created_at"],
        },
    }


@router.get("/me")
async def me(current_user=Depends(get_current_user)):
    return {"user": current_user}
