'use client';

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { trackOrderEvent, trackClickEvent } from "@/lib/eventTracker";
import { clearAuth, getStoredAuth } from "@/lib/auth";

interface FoodItem {
  food_id: number;
  name: string;
  category: string;
  price: number;
  description?: string;
  image_url: string;
  is_available: boolean;
}

export default function Home() {
  const router = useRouter();
  const [foods, setFoods] = useState<FoodItem[]>([]);
  const [personalizedAds, setPersonalizedAds] = useState<FoodItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedCategory, setSelectedCategory] = useState<string | null>(null);
  const [categories, setCategories] = useState<string[]>([]);
  const [userLabel, setUserLabel] = useState<string>('');

  useEffect(() => {
    const auth = getStoredAuth();
    if (!auth?.access_token) {
      router.replace('/auth');
      return;
    }
    setUserLabel(auth.user?.email || `user_${auth.user?.id}`);

    // Fetch categories
    fetch('http://localhost:8001/api/foods/categories/list')
      .then(res => res.json())
      .then(data => setCategories(data))
      .catch(err => console.error('Error fetching categories:', err));

    // Fetch foods
    fetchFoods();
    fetchPersonalized();
  }, []);

  const fetchPersonalized = () => {
    const auth = getStoredAuth();
    if (!auth?.access_token) return;
    fetch('http://localhost:8000/api/v1/ads/personalized?limit=4', {
      headers: {
        'Authorization': `Bearer ${auth.access_token}`,
      },
    })
      .then(res => res.json())
      .then(data => {
        setPersonalizedAds((data.items || []).map((x: any) => ({
          food_id: x.food_id,
          name: x.name,
          category: x.category,
          price: x.price,
          image_url: x.image_url,
          is_available: true,
          description: `${x.reason} (score ${x.score})`,
        })));
      })
      .catch(err => console.error('Error fetching personalized ads:', err));
  };

  const fetchFoods = (category?: string) => {
    setLoading(true);
    const url = category
      ? `http://localhost:8001/api/foods?category=${category}`
      : 'http://localhost:8001/api/foods';

    fetch(url)
      .then(res => res.json())
      .then(data => {
        setFoods(data.items);
        setLoading(false);
      })
      .catch(err => {
        console.error('Error fetching foods:', err);
        setLoading(false);
      });
  };

  const handleCategoryClick = (category: string | null) => {
    setSelectedCategory(category);
    fetchFoods(category || undefined);
  };

  const handleOrderClick = async (food: FoodItem, e: React.MouseEvent) => {
    e.stopPropagation(); // Prevent card click from firing

    // Send order event to Events Gateway and navigate to order page
    const { orderId, success } = await trackOrderEvent(
      food.food_id,
      food.name,
      food.price,
      food.category,
      food.description || '',
      food.image_url
    );

    if (success) {
      router.push(`/order/${orderId}`);
    } else {
      alert('Failed to place order. Please try again.');
    }
  };

  const handleFoodClick = (food: FoodItem) => {
    // Navigate to detail page — impression is tracked there on page load
    router.push(`/food/${food.food_id}`);
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 py-4 flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold text-red-600">🍕 FlowGuard Food</h1>
            <p className="text-gray-600 text-sm">Delicious food, lightning-fast delivery</p>
          </div>
          <div className="text-right flex flex-col items-end gap-1">
            <p className="text-xs text-gray-400">User</p>
            <p className="text-xs font-mono text-gray-600">{userLabel || '...'}</p>
            <button
              onClick={() => { clearAuth(); router.replace('/auth'); }}
              className="text-xs text-red-600 underline"
            >
              Logout
            </button>
          </div>
        </div>
      </header>

      {/* Category Filter */}
      <div className="bg-white border-b sticky top-[72px] z-10">
        <div className="max-w-7xl mx-auto px-4 py-3">
          <div className="flex gap-2 overflow-x-auto">
            {/* ... categories logic ... */}
            <button
              onClick={() => handleCategoryClick(null)}
              className={`px-4 py-2 rounded-full text-sm font-medium whitespace-nowrap transition ${selectedCategory === null
                ? 'bg-red-600 text-white'
                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
            >
              All
            </button>
            {categories.map(cat => (
              <button
                key={cat}
                onClick={() => handleCategoryClick(cat)}
                className={`px-4 py-2 rounded-full text-sm font-medium whitespace-nowrap transition ${selectedCategory === cat
                  ? 'bg-red-600 text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
              >
                {cat}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Sponsored Ads Carousel */}
      {!loading && personalizedAds.length > 0 && (
        <SponsoredCarousel ads={personalizedAds} onAdClick={handleFoodClick} />
      )}

      {/* Food Grid */}
      <main className="max-w-7xl mx-auto px-4 py-8">
        {loading ? (
          <div className="text-center py-12">
            <div className="inline-block animate-spin rounded-full h-12 w-12 border-4 border-red-600 border-t-transparent"></div>
            <p className="mt-4 text-gray-600">Loading delicious food...</p>
          </div>
        ) : (
          <>
            <div className="mb-6">
              <h2 className="text-2xl font-semibold text-gray-900">
                {selectedCategory || 'All Items'}
                <span className="text-gray-500 text-lg ml-2">({foods.length})</span>
              </h2>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
              {foods.map(food => (
                <div
                  key={food.food_id}
                  className="bg-white rounded-xl shadow-sm hover:shadow-lg transition-all overflow-hidden group cursor-pointer"
                  onClick={() => handleFoodClick(food)}
                >
                  <div className="relative h-48 bg-gray-100">
                    <img
                      src={food.image_url}
                      alt={food.name}
                      className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300"
                    />
                    {!food.is_available && (
                      <div className="absolute inset-0 bg-black bg-opacity-50 flex items-center justify-center">
                        <span className="bg-red-600 text-white px-4 py-2 rounded-full font-semibold">
                          Unavailable
                        </span>
                      </div>
                    )}
                  </div>

                  <div className="p-4">
                    <div className="flex items-start justify-between mb-2">
                      <h3 className="font-semibold text-gray-900 text-lg">{food.name}</h3>
                    </div>

                    <div className="flex items-center justify-between mb-3">
                      <span className="text-xs bg-gray-100 text-gray-700 px-2 py-1 rounded">
                        {food.category}
                      </span>
                      <span className="text-lg font-bold text-gray-900">
                        ₹{food.price}
                      </span>
                    </div>

                    <button
                      onClick={(e) => handleOrderClick(food, e)}
                      disabled={!food.is_available}
                      className={`w-full py-2 rounded-lg font-semibold transition ${food.is_available
                        ? 'bg-red-600 text-white hover:bg-red-700'
                        : 'bg-gray-200 text-gray-400 cursor-not-allowed'
                        }`}
                    >
                      {food.is_available ? 'Order Now' : 'Out of Stock'}
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </>
        )}
      </main>

      {/* Footer */}
      <footer className="bg-white border-t mt-12">
        <div className="max-w-7xl mx-auto px-4 py-6 text-center text-gray-600 text-sm">
          <p>FlowGuard - Real time Data Tracking </p>
          <p className="text-xs text-gray-400 mt-1">
            PostgreSQL → Food Catalog API (port 8001) → Next.js UI
          </p>
        </div>
      </footer>
    </div>
  );
}

function SponsoredCarousel({ ads, onAdClick }: { ads: FoodItem[]; onAdClick: (f: FoodItem) => void }) {
  const [currentIndex, setCurrentIndex] = useState(0);

  useEffect(() => {
    if (ads[currentIndex]) {
      trackClickEvent(`food_${ads[currentIndex].food_id}`, false, ads[currentIndex].category, 'home');
    }
  }, [currentIndex, ads]);

  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentIndex(prev => (prev + 1) % ads.length);
    }, 3000); // Rotate every 3 seconds
    return () => clearInterval(interval);
  }, [ads.length]);

  if (ads.length === 0) return null;

  const currentAd = ads[currentIndex];

  return (
    <div className="max-w-7xl mx-auto px-4 mt-6">
      <div className="relative bg-gradient-to-r from-gray-900 to-gray-800 rounded-2xl overflow-hidden shadow-2xl h-64 flex items-center">
        {/* Background Image (Blurred) */}
        <div
          className="absolute inset-0 bg-cover bg-center opacity-30 blur-sm transition-all duration-1000"
          style={{ backgroundImage: `url(https://loremflickr.com/800/600/${encodeURIComponent(currentAd.name)}/food)` }}
        ></div>

        <div className="relative z-10 flex w-full p-8 items-center justify-between">
          <div className="text-white max-w-lg">
            <span className="bg-yellow-400 text-yellow-900 text-xs font-bold px-2 py-1 rounded uppercase tracking-wide mb-2 inline-block">
              Sponsored
            </span>
            <h2 className="text-3xl font-extrabold mb-2 leading-tight drop-shadow-md">
              {currentAd.name}
            </h2>
            <p className="text-gray-200 mb-6 drop-shadow">
              Order now and get lightning fast delivery!
            </p>
            <button
              onClick={() => {
                trackClickEvent(`food_${currentAd.food_id}`, true, currentAd.category, 'home');
                onAdClick(currentAd);
              }}
              className="bg-red-600 hover:bg-red-700 text-white font-bold py-2 px-6 rounded-full transition shadow-lg transform hover:scale-105"
            >
              Order Now &rarr;
            </button>
          </div>

          <div className="hidden md:block w-48 h-48 relative">
            <img
              src={`https://loremflickr.com/400/400/${encodeURIComponent(currentAd.name)}/food`}
              alt={currentAd.name}
              className="w-full h-full object-cover rounded-full border-4 border-white shadow-lg animate-pulse-slow"
            />
            <div className="absolute top-0 right-0 bg-white text-black font-bold rounded-full w-12 h-12 flex items-center justify-center shadow">
              ₹{currentAd.price}
            </div>
          </div>
        </div>

        {/* Indicators */}
        <div className="absolute bottom-4 left-1/2 transform -translate-x-1/2 flex space-x-2">
          {ads.map((_, idx) => (
            <button
              key={idx}
              onClick={() => setCurrentIndex(idx)}
              className={`w-2 h-2 rounded-full transition-all ${idx === currentIndex ? "bg-white w-6" : "bg-white/50"
                }`}
            />
          ))}
        </div>
      </div>
    </div >
  );
}
