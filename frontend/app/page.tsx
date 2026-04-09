'use client';

import AppShell, { useAppContext } from '@/components/AppShell';
import MetricCard from '@/components/common/MetricCard';
import Card from '@/components/common/Card';
import LivePrice from '@/components/trading/LivePrice';
import TradeHistory from '@/components/trading/TradeHistory';
import MiniSparkline from '@/components/charts/MiniSparkline';
import { formatUSD, formatVolume, formatNumber } from '@/lib/formatters';
import { useEffect, useState } from 'react';

function DashboardContent() {
  const { stats, trades, tradesPerSecond } = useAppContext();
  const [priceHistory, setPriceHistory] = useState<number[]>([]);

  // Build price history for sparkline
  useEffect(() => {
    if (stats.currentPrice > 0) {
      setPriceHistory((prev) => {
        const next = [...prev, stats.currentPrice];
        return next.slice(-60); // Last 60 data points
      });
    }
  }, [stats.currentPrice]);

  const direction = stats.priceChangePercent24h >= 0 ? 'up' : 'down';

  return (
    <>
      {/* Live Price Banner */}
      <Card noPadding>
        <LivePrice stats={stats} />
      </Card>

      {/* Metric Cards */}
      <div className="grid-overview" style={{ marginTop: '16px' }}>
        <MetricCard
          label="Session High"
          value={stats.high24h > 0 ? formatUSD(stats.high24h) : '—'}
          icon="📈"
          changeDirection="up"
          id="metric-high"
        />
        <MetricCard
          label="Session Low"
          value={stats.low24h < Infinity ? formatUSD(stats.low24h) : '—'}
          icon="📉"
          changeDirection="down"
          id="metric-low"
        />
        <MetricCard
          label="Volume (BTC)"
          value={formatVolume(stats.volume24h)}
          icon="📊"
          id="metric-volume"
        />
        <MetricCard
          label="Trades/sec"
          value={`${tradesPerSecond}`}
          change={`${formatNumber(stats.tradeCount24h)} total`}
          changeDirection="neutral"
          icon="⚡"
          id="metric-tps"
        />
      </div>

      {/* Two column layout */}
      <div className="grid-2col" style={{ marginTop: '0' }}>
        {/* Price Sparkline */}
        <Card title="Price Trend (Last 60 ticks)">
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '16px' }}>
            {priceHistory.length > 2 ? (
              <MiniSparkline
                data={priceHistory}
                width={480}
                height={120}
                color={direction === 'up' ? '#0ecb81' : '#f6465d'}
                fillOpacity={0.08}
              />
            ) : (
              <div className="empty-state" style={{ padding: '24px' }}>
                <div className="empty-state-icon">📈</div>
                <div>Đang thu thập dữ liệu giá...</div>
              </div>
            )}
            <div style={{ display: 'flex', gap: '24px', fontSize: '12px', fontFamily: 'var(--font-mono)' }}>
              <span>
                <span style={{ color: 'var(--text-tertiary)' }}>Avg 1m: </span>
                <span style={{ color: 'var(--accent)', fontWeight: 600 }}>
                  {stats.avgPrice1m > 0 ? formatUSD(stats.avgPrice1m) : '—'}
                </span>
              </span>
              <span>
                <span style={{ color: 'var(--text-tertiary)' }}>Spread: </span>
                <span style={{ color: 'var(--text-secondary)' }}>
                  {stats.high24h > 0 && stats.low24h < Infinity
                    ? formatUSD(stats.high24h - stats.low24h)
                    : '—'}
                </span>
              </span>
            </div>
          </div>
        </Card>

        {/* Recent Trades */}
        <Card title="Recent Trades" noPadding>
          <div style={{ height: '260px' }}>
            <TradeHistory trades={trades} maxDisplay={20} />
          </div>
        </Card>
      </div>

      {/* Architecture Info */}
      <Card title="System Architecture" className="" id="architecture-info">
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: '8px', textAlign: 'center' }}>
          {[
            { icon: '🌐', label: 'Binance API', sub: 'WebSocket Stream' },
            { icon: '📨', label: 'Apache Kafka', sub: 'Message Queue' },
            { icon: '⚡', label: 'Apache Spark', sub: 'Structured Streaming' },
            { icon: '🗃️', label: 'MongoDB + Redis', sub: 'NoSQL Storage' },
            { icon: '📊', label: 'Next.js', sub: 'Dashboard' },
          ].map((item, i) => (
            <div key={item.label} style={{ position: 'relative' }}>
              <div
                style={{
                  padding: '16px 8px',
                  background: 'var(--bg-secondary)',
                  borderRadius: 'var(--radius-md)',
                  border: '1px solid var(--border)',
                }}
              >
                <div style={{ fontSize: '24px', marginBottom: '8px' }}>{item.icon}</div>
                <div style={{ fontSize: '12px', fontWeight: 700, color: 'var(--text-primary)' }}>
                  {item.label}
                </div>
                <div style={{ fontSize: '10px', color: 'var(--text-tertiary)', marginTop: '2px' }}>
                  {item.sub}
                </div>
              </div>
              {i < 4 && (
                <div
                  style={{
                    position: 'absolute',
                    right: '-16px',
                    top: '50%',
                    transform: 'translateY(-50%)',
                    fontSize: '16px',
                    color: 'var(--accent)',
                    zIndex: 1,
                  }}
                >
                  →
                </div>
              )}
            </div>
          ))}
        </div>
      </Card>
    </>
  );
}

export default function DashboardPage() {
  return (
    <AppShell title="Dashboard" description="Tổng quan thị trường real-time">
      <DashboardContent />
    </AppShell>
  );
}
