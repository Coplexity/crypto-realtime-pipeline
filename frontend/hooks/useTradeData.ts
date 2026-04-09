'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import type { RawTrade, TradeDisplay, CandleData, DashboardStats } from '@/types';
import { MAX_TRADES_DISPLAY, MAX_CANDLES_DISPLAY } from '@/lib/constants';
import { formatTime } from '@/lib/formatters';

interface UseTradeDataReturn {
  trades: TradeDisplay[];
  candles: CandleData[];
  stats: DashboardStats;
  tradesPerSecond: number;
}

const DEFAULT_STATS: DashboardStats = {
  currentPrice: 0,
  priceChange24h: 0,
  priceChangePercent24h: 0,
  high24h: 0,
  low24h: Infinity,
  volume24h: 0,
  tradeCount24h: 0,
  avgPrice1m: 0,
};

/**
 * Aggregates raw trade data into display trades, candles, and statistics.
 * Builds OHLCV candles from tick data in real-time.
 */
export function useTradeData(
  lastTrade: RawTrade | null,
  candleIntervalSeconds: number = 60
): UseTradeDataReturn {
  const [trades, setTrades] = useState<TradeDisplay[]>([]);
  const [candles, setCandles] = useState<CandleData[]>([]);
  const [stats, setStats] = useState<DashboardStats>(DEFAULT_STATS);
  const [tradesPerSecond, setTradesPerSecond] = useState(0);

  // Refs for mutable state that doesn't need re-renders
  const currentCandleRef = useRef<CandleData | null>(null);
  const priceHistoryRef = useRef<number[]>([]);
  const tradeCounterRef = useRef(0);
  const firstPriceRef = useRef<number | null>(null);
  const highPriceRef = useRef(0);
  const lowPriceRef = useRef(Infinity);
  const volumeRef = useRef(0);
  const tradeCountRef = useRef(0);

  // TPS calculation
  useEffect(() => {
    const interval = setInterval(() => {
      setTradesPerSecond(tradeCounterRef.current);
      tradeCounterRef.current = 0;
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  const buildCandle = useCallback((trade: RawTrade): CandleData[] => {
    const tradeTime = trade.trade_time || Date.now();
    const candleStart = Math.floor(tradeTime / (candleIntervalSeconds * 1000)) * candleIntervalSeconds;

    const current = currentCandleRef.current;

    if (!current || current.time !== candleStart) {
      // New candle period
      const completedCandles: CandleData[] = [];
      if (current) {
        completedCandles.push({ ...current });
      }

      currentCandleRef.current = {
        time: candleStart,
        open: trade.price,
        high: trade.price,
        low: trade.price,
        close: trade.price,
        volume: trade.quantity,
      };

      return completedCandles;
    }

    // Update existing candle
    current.high = Math.max(current.high, trade.price);
    current.low = Math.min(current.low, trade.price);
    current.close = trade.price;
    current.volume += trade.quantity;

    return [];
  }, [candleIntervalSeconds]);

  // Process incoming trades
  useEffect(() => {
    if (!lastTrade) return;

    tradeCounterRef.current += 1;
    tradeCountRef.current += 1;

    // Track first price for change calculation
    if (firstPriceRef.current === null) {
      firstPriceRef.current = lastTrade.price;
    }

    // Update high/low
    highPriceRef.current = Math.max(highPriceRef.current, lastTrade.price);
    if (lowPriceRef.current === Infinity) {
      lowPriceRef.current = lastTrade.price;
    }
    lowPriceRef.current = Math.min(lowPriceRef.current, lastTrade.price);
    volumeRef.current += lastTrade.quantity;

    // Add to price history (keep last 60 for avg)
    priceHistoryRef.current.push(lastTrade.price);
    if (priceHistoryRef.current.length > 60) {
      priceHistoryRef.current.shift();
    }

    // Create display trade
    const displayTrade: TradeDisplay = {
      id: lastTrade.trade_id || Date.now(),
      price: lastTrade.price,
      quantity: lastTrade.quantity,
      total: lastTrade.price * lastTrade.quantity,
      time: formatTime(lastTrade.trade_time || Date.now()),
      side: lastTrade.is_buyer_maker ? 'sell' : 'buy',
      timestamp: lastTrade.trade_time || Date.now(),
    };

    // Update trades list
    setTrades((prev) => {
      const next = [displayTrade, ...prev];
      return next.slice(0, MAX_TRADES_DISPLAY);
    });

    // Build candles
    const completedCandles = buildCandle(lastTrade);
    if (completedCandles.length > 0) {
      setCandles((prev) => {
        const next = [...prev, ...completedCandles];
        return next.slice(-MAX_CANDLES_DISPLAY);
      });
    }

    // Update stats
    const avgPrice =
      priceHistoryRef.current.reduce((s, p) => s + p, 0) /
      priceHistoryRef.current.length;

    const priceChange = lastTrade.price - (firstPriceRef.current || lastTrade.price);
    const priceChangePct = firstPriceRef.current
      ? ((lastTrade.price - firstPriceRef.current) / firstPriceRef.current) * 100
      : 0;

    setStats({
      currentPrice: lastTrade.price,
      priceChange24h: priceChange,
      priceChangePercent24h: priceChangePct,
      high24h: highPriceRef.current,
      low24h: lowPriceRef.current,
      volume24h: volumeRef.current,
      tradeCount24h: tradeCountRef.current,
      avgPrice1m: avgPrice,
    });
  }, [lastTrade, buildCandle]);

  // Also include the current (in-progress) candle for live chart
  const allCandles = currentCandleRef.current
    ? [...candles, currentCandleRef.current]
    : candles;

  return {
    trades,
    candles: allCandles,
    stats,
    tradesPerSecond,
  };
}
