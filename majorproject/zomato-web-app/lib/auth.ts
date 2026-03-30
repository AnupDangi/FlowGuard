const EVENTS_GATEWAY_URL = 'http://localhost:8000';
const AUTH_STORAGE_KEY = 'flowguard_auth';

export type AuthUser = {
  id: number;
  email: string;
  name: string;
};

type AuthPayload = {
  access_token: string;
  token_type: string;
  user: AuthUser;
};

export function getStoredAuth(): AuthPayload | null {
  if (typeof window === 'undefined') return null;
  const raw = localStorage.getItem(AUTH_STORAGE_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function saveAuth(auth: AuthPayload) {
  if (typeof window === 'undefined') return;
  localStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(auth));
}

export function clearAuth() {
  if (typeof window === 'undefined') return;
  localStorage.removeItem(AUTH_STORAGE_KEY);
}

export async function signup(email: string, password: string, name: string): Promise<AuthPayload> {
  const res = await fetch(`${EVENTS_GATEWAY_URL}/api/v1/auth/signup`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password, name }),
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  const data = await res.json();
  saveAuth(data);
  return data;
}

export async function login(email: string, password: string): Promise<AuthPayload> {
  const res = await fetch(`${EVENTS_GATEWAY_URL}/api/v1/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password }),
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  const data = await res.json();
  saveAuth(data);
  return data;
}
