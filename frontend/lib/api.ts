import { API_BASE_URL } from './constants';
import type { HealthStatus } from '@/types';

/**
 * Generic API fetch helper with error handling.
 */
async function apiFetch<T>(endpoint: string): Promise<T> {
  const res = await fetch(`${API_BASE_URL}${endpoint}`, {
    cache: 'no-store',
  });

  if (!res.ok) {
    throw new Error(`API Error: ${res.status} ${res.statusText}`);
  }

  return res.json();
}

/**
 * Check backend health status.
 */
export async function getHealthStatus(): Promise<HealthStatus> {
  return apiFetch<HealthStatus>('/health');
}

/**
 * Get latest trades (placeholder – will return real data when Tuấn implements DB layer).
 */
export async function getLatestTrades(limit: number = 10) {
  return apiFetch(`/api/trades/latest?limit=${limit}`);
}

/**
 * Get OHLCV data for a symbol (placeholder – needs Spark processing).
 */
export async function getOHLCV(symbol: string, timeframe: string = '1m') {
  return apiFetch(`/api/ohlcv/${symbol}?timeframe=${timeframe}`);
}

/**
 * Get trades for a specific symbol.
 */
export async function getTradesBySymbol(symbol: string, limit: number = 100) {
  return apiFetch(`/api/trades/${symbol}?limit=${limit}`);
}
