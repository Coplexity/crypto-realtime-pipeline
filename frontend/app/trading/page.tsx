'use client';

import { useState } from 'react';
import AppShell, { useAppContext } from '@/components/AppShell';
import Card from '@/components/common/Card';
import CandlestickChart from '@/components/charts/CandlestickChart';
import TradeHistory from '@/components/trading/TradeHistory';
import LivePrice from '@/components/trading/LivePrice';
import { formatUSD, formatQuantity } from '@/lib/formatters';
import type { Timeframe } from '@/types';

function TradingContent() {
  const { stats, trades, candles } = useAppContext();
  const [timeframe, setTimeframe] = useState<Timeframe>('1m');

  return (
    <div className="grid-trading">
      {/* Left: Chart */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '16px', minHeight: 0 }}>
        {/* Price bar */}
        <Card noPadding>
          <LivePrice stats={stats} />
        </Card>

        {/* Candlestick chart */}
        <Card noPadding style={{ flex: 1, minHeight: 0 }}>
          <div style={{ height: '100%', position: 'relative' }}>
            <CandlestickChart
              candles={candles}
              timeframe={timeframe}
              onTimeframeChange={setTimeframe}
            />
          </div>
        </Card>
      </div>

      {/* Right: Trade History */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '16px', minHeight: 0 }}>
        {/* Quick stats */}
        <Card title="Order Book" noPadding>
          <div style={{ padding: '14px' }}>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', fontSize: '12px' }}>
              {/* Bid/Ask simulation from recent trades */}
              <div style={{ textAlign: 'center', marginBottom: '8px' }}>
                <div style={{ fontSize: '11px', color: 'var(--text-tertiary)', marginBottom: '4px' }}>Spread</div>
                <div style={{ fontFamily: 'var(--font-mono)', fontWeight: 700, color: 'var(--accent)' }}>
                  {stats.high24h > 0 && stats.low24h < Infinity
                    ? `$${(stats.high24h - stats.low24h).toFixed(2)}`
                    : '—'}
                </div>
              </div>
              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: '1fr 1fr 1fr',
                  gap: '4px',
                  padding: '8px',
                  background: 'var(--bg-secondary)',
                  borderRadius: 'var(--radius-sm)',
                  fontFamily: 'var(--font-mono)',
                  fontSize: '11px',
                }}
              >
                <span style={{ color: 'var(--text-tertiary)' }}>Price</span>
                <span style={{ color: 'var(--text-tertiary)', textAlign: 'center' }}>Size</span>
                <span style={{ color: 'var(--text-tertiary)', textAlign: 'right' }}>Total</span>
                
                {/* Simulated ask entries from trades */}
                {trades.filter(t => t.side === 'sell').slice(0, 5).map((t, i) => (
                  <div key={`ask-${i}`} style={{ display: 'contents' }}>
                    <span style={{ color: 'var(--down)' }}>{formatUSD(t.price)}</span>
                    <span style={{ textAlign: 'center', color: 'var(--text-secondary)' }}>{formatQuantity(t.quantity)}</span>
                    <span style={{ textAlign: 'right', color: 'var(--text-secondary)' }}>{formatUSD(t.total)}</span>
                  </div>
                ))}

                {/* Divider */}
                <div style={{ gridColumn: '1 / -1', borderBottom: '1px solid var(--border)', margin: '4px 0' }} />

                {/* Simulated bid entries from trades */}
                {trades.filter(t => t.side === 'buy').slice(0, 5).map((t, i) => (
                  <div key={`bid-${i}`} style={{ display: 'contents' }}>
                    <span style={{ color: 'var(--up)' }}>{formatUSD(t.price)}</span>
                    <span style={{ textAlign: 'center', color: 'var(--text-secondary)' }}>{formatQuantity(t.quantity)}</span>
                    <span style={{ textAlign: 'right', color: 'var(--text-secondary)' }}>{formatUSD(t.total)}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </Card>

        {/* Trade History */}
        <Card title="Trade History" noPadding style={{ flex: 1, minHeight: 0 }}>
          <div style={{ height: '100%', overflow: 'hidden' }}>
            <TradeHistory trades={trades} maxDisplay={50} />
          </div>
        </Card>
      </div>
    </div>
  );
}

export default function TradingPage() {
  return (
    <AppShell title="Trading" description="Biểu đồ nến & lịch sử giao dịch BTC/USDT">
      <TradingContent />
    </AppShell>
  );
}
