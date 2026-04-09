'use client';

import { useEffect, useRef, useState } from 'react';
import { formatUSD, formatPercent, formatVolume } from '@/lib/formatters';
import { SYMBOL_DISPLAY } from '@/lib/constants';
import type { DashboardStats } from '@/types';

interface LivePriceProps {
  stats: DashboardStats;
}

export default function LivePrice({ stats }: LivePriceProps) {
  const [isFlashing, setIsFlashing] = useState(false);
  const prevPriceRef = useRef(stats.currentPrice);
  const directionRef = useRef<'up' | 'down' | 'neutral'>('neutral');

  useEffect(() => {
    if (stats.currentPrice !== prevPriceRef.current) {
      if (stats.currentPrice > prevPriceRef.current) {
        directionRef.current = 'up';
      } else if (stats.currentPrice < prevPriceRef.current) {
        directionRef.current = 'down';
      }
      prevPriceRef.current = stats.currentPrice;
      setIsFlashing(true);
      const timer = setTimeout(() => setIsFlashing(false), 300);
      return () => clearTimeout(timer);
    }
  }, [stats.currentPrice]);

  const direction = stats.priceChangePercent24h >= 0 ? 'up' : 'down';
  const priceDirection = directionRef.current;

  if (stats.currentPrice === 0) {
    return (
      <div style={{ padding: '24px' }}>
        <div style={{ color: 'var(--text-tertiary)', fontSize: '14px' }}>
          Đang chờ dữ liệu từ Kafka...
        </div>
      </div>
    );
  }

  return (
    <div style={{ padding: '24px' }} id="live-price-display">
      <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '4px' }}>
        <span style={{ fontSize: '20px' }}>₿</span>
        <span style={{ fontSize: '16px', fontWeight: 700, color: 'var(--text-primary)' }}>
          {SYMBOL_DISPLAY}
        </span>
        <span
          className={`status-badge ${direction === 'up' ? 'healthy' : 'down'}`}
          style={{ fontSize: '10px' }}
        >
          {direction === 'up' ? '↑ BULLISH' : '↓ BEARISH'}
        </span>
      </div>

      <div className={`live-price ${priceDirection} ${isFlashing ? 'price-flash' : ''}`}>
        {formatUSD(stats.currentPrice)}
      </div>

      <div style={{ display: 'flex', gap: '20px', marginTop: '12px', flexWrap: 'wrap' }}>
        <div className={`metric-change ${direction}`}>
          <span>{direction === 'up' ? '▲' : '▼'}</span>
          <span>{formatPercent(stats.priceChangePercent24h)}</span>
        </div>

        <div style={{ display: 'flex', gap: '20px', fontSize: '12px', fontFamily: 'var(--font-mono)' }}>
          <span>
            <span style={{ color: 'var(--text-tertiary)' }}>High: </span>
            <span style={{ color: 'var(--up)' }}>{formatUSD(stats.high24h)}</span>
          </span>
          <span>
            <span style={{ color: 'var(--text-tertiary)' }}>Low: </span>
            <span style={{ color: 'var(--down)' }}>{formatUSD(stats.low24h)}</span>
          </span>
          <span>
            <span style={{ color: 'var(--text-tertiary)' }}>Vol: </span>
            <span style={{ color: 'var(--text-secondary)' }}>{formatVolume(stats.volume24h)} BTC</span>
          </span>
        </div>
      </div>
    </div>
  );
}
