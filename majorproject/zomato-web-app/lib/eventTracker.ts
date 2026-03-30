/**
 * Event tracking utility for FlowGuard
 * Sends events to Events Gateway (port 8000)
 */

const EVENTS_GATEWAY_URL = 'http://localhost:8000';
const AUTH_STORAGE_KEY = 'flowguard_auth';

function getAuthToken(): string | null {
  if (typeof window === 'undefined') return null;
  const raw = localStorage.getItem(AUTH_STORAGE_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw).access_token || null;
  } catch {
    return null;
  }
}

function getSessionId(): string {
  if (typeof window === 'undefined') return 'session_ssr';
  let sid = localStorage.getItem('flowguard_session_id');
  if (!sid) {
    sid = `sess_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
    localStorage.setItem('flowguard_session_id', sid);
  }
  return sid;
}

function authHeaders() {
  const token = getAuthToken();
  return {
    'Content-Type': 'application/json',
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  };
}

// Send order event - returns orderId for navigation
export async function trackOrderEvent(
  itemId: number, 
  itemName: string, 
  price: number,
  itemCategory: string = '',
  itemDescription: string = '',
  itemImageUrl: string = ''
): Promise<{ orderId: string; success: boolean }> {
  try {
    const payload = {
      item_id: itemId,
      item_name: itemName,
      price: price,
      timestamp: new Date().toISOString(),
      metadata: {
        category: itemCategory,
        source_page: 'home',
        session_id: getSessionId(),
      },
    };

    const response = await fetch(`${EVENTS_GATEWAY_URL}/api/v1/orders/`, {
      method: 'POST',
      headers: authHeaders(),
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      console.error('Failed to track order:', await response.text());
      return { orderId: '', success: false };
    }

    const result = await response.json();
    const orderId = result.order_id;  // Server-generated UUID
    
    console.log('✅ Order placed:', result);
    
    // Store order details in localStorage for order page
    if (typeof window !== 'undefined') {
      const orderDetails = {
        order_id: orderId,
        food_id: itemId,
        name: itemName,
        category: itemCategory,
        price: price,
        description: itemDescription,
        image_url: itemImageUrl,
        user_id: 'auth_user',
        timestamp: payload.timestamp
      };
      localStorage.setItem(`order_${orderId}`, JSON.stringify(orderDetails));
    }
    
    return { orderId, success: true };
  } catch (error) {
    console.error('Error tracking order:', error);
    return { orderId: '', success: false };
  }
}

// Send click or impression event.
export async function trackClickEvent(
  adId: string,
  isClick: boolean,
  category: string = '',
  sourcePage: string = 'home',
): Promise<boolean> {
  try {
    const itemId = adId.startsWith('food_') ? parseInt(adId.split('_')[1], 10) : null;
    
    const payload = {
      ad_id: adId,
      item_id: itemId,
      session_id: getSessionId(),
      is_click: isClick,
      event_type: isClick ? 'click' : 'impression',
      timestamp: new Date().toISOString(),
      metadata: {
        category,
        source_page: sourcePage,
      },
    };

    const response = await fetch(`${EVENTS_GATEWAY_URL}/api/v1/clicks/`, {
      method: 'POST',
      headers: authHeaders(),
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

export async function trackViewEvent(
  itemId: number,
  category: string,
  sourcePage: string = 'food_detail',
): Promise<boolean> {
  try {
    const payload = {
      event_type: 'view',
      event_time: new Date().toISOString(),
      item_id: itemId,
      category,
      session_id: getSessionId(),
      source_page: sourcePage,
      metadata: {},
    };
    const response = await fetch(`${EVENTS_GATEWAY_URL}/api/v1/behavior/`, {
      method: 'POST',
      headers: authHeaders(),
      body: JSON.stringify(payload),
    });
    return response.ok;
  } catch (error) {
    console.error('Error tracking view:', error);
    return false;
  }
}
