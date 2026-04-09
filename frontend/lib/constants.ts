import type { TimeframeOption } from '@/types';

// ===== API Configuration =====

export const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
export const WS_URL = process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:8000/ws/crypto';

// ===== WebSocket =====

export const WS_RECONNECT_BASE_DELAY = 1000; // 1 second
export const WS_RECONNECT_MAX_DELAY = 30000; // 30 seconds
export const WS_MAX_RECONNECT_ATTEMPTS = 20;
export const WS_MESSAGE_BUFFER_SIZE = 100;

// ===== Trading =====

export const SYMBOL = 'BTCUSDT';
export const SYMBOL_DISPLAY = 'BTC/USDT';
export const BASE_CURRENCY = 'BTC';
export const QUOTE_CURRENCY = 'USDT';

export const TIMEFRAMES: TimeframeOption[] = [
  { label: '1m', value: '1m', seconds: 60 },
  { label: '5m', value: '5m', seconds: 300 },
  { label: '15m', value: '15m', seconds: 900 },
  { label: '1h', value: '1h', seconds: 3600 },
  { label: '4h', value: '4h', seconds: 14400 },
  { label: '1D', value: '1d', seconds: 86400 },
];

export const DEFAULT_TIMEFRAME = '1m';

// ===== Chart =====

export const MAX_CANDLES_DISPLAY = 200;
export const MAX_TRADES_DISPLAY = 100;

// ===== Navigation =====

export interface NavItem {
  label: string;
  href: string;
  icon: string;
  description: string;
}

export const NAV_ITEMS: NavItem[] = [
  {
    label: 'Dashboard',
    href: '/',
    icon: '📊',
    description: 'Tổng quan thị trường',
  },
  {
    label: 'Trading',
    href: '/trading',
    icon: '📈',
    description: 'Biểu đồ nến & giao dịch',
  },
  {
    label: 'Analytics',
    href: '/analytics',
    icon: '🔬',
    description: 'Phân tích kỹ thuật & ML',
  },
  {
    label: 'System',
    href: '/system',
    icon: '⚙️',
    description: 'Giám sát hệ thống',
  },
];

// ===== Design Tokens =====

export const COLORS = {
  up: '#0ecb81',
  down: '#f6465d',
  accent: '#f0b90b',
  neutral: '#848e9c',
};
