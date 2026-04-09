'use client';

import { useMemo, useEffect, useState } from 'react';
import AppShell, { useAppContext } from '@/components/AppShell';
import Card from '@/components/common/Card';
import MetricCard from '@/components/common/MetricCard';
import RSIIndicator from '@/components/analytics/RSIIndicator';
import VWAPDisplay from '@/components/analytics/VWAPDisplay';
import PredictionCard from '@/components/analytics/PredictionCard';
import MiniSparkline from '@/components/charts/MiniSparkline';
import { formatUSD, formatNumber, formatVolume } from '@/lib/formatters';

/**
 * Simple RSI calculation from price data.
 * RSI = 100 - (100 / (1 + RS))
 * RS = Average Gain / Average Loss over N periods
 */
function calculateRSI(prices: number[], period: number = 14): number {
  if (prices.length < period + 1) return 50; // Default neutral

  const changes = [];
  for (let i = 1; i < prices.length; i++) {
    changes.push(prices[i] - prices[i - 1]);
  }

  const recentChanges = changes.slice(-period);
  const gains = recentChanges.filter((c) => c > 0);
  const losses = recentChanges.filter((c) => c < 0).map((c) => Math.abs(c));

  const avgGain = gains.length > 0 ? gains.reduce((s, g) => s + g, 0) / period : 0;
  const avgLoss = losses.length > 0 ? losses.reduce((s, l) => s + l, 0) / period : 0;

  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

/**
 * VWAP calculation: sum(price * volume) / sum(volume)
 */
function calculateVWAP(trades: { price: number; quantity: number }[]): number {
  if (trades.length === 0) return 0;
  const totalPV = trades.reduce((s, t) => s + t.price * t.quantity, 0);
  const totalV = trades.reduce((s, t) => s + t.quantity, 0);
  return totalV > 0 ? totalPV / totalV : 0;
}

function AnalyticsContent() {
  const { stats, trades } = useAppContext();
  const [priceHistory, setPriceHistory] = useState<number[]>([]);
  const [volumeHistory, setVolumeHistory] = useState<number[]>([]);

  // Build histories
  useEffect(() => {
    if (stats.currentPrice > 0) {
      setPriceHistory((prev) => [...prev, stats.currentPrice].slice(-200));
    }
  }, [stats.currentPrice]);

  useEffect(() => {
    if (stats.volume24h > 0) {
      setVolumeHistory((prev) => [...prev, stats.volume24h].slice(-60));
    }
  }, [stats.volume24h]);

  // Calculate indicators
  const rsi = useMemo(() => calculateRSI(priceHistory, 14), [priceHistory]);
  const vwap = useMemo(() => calculateVWAP(trades), [trades]);

  // Statistics
  const priceStd = useMemo(() => {
    if (priceHistory.length < 2) return 0;
    const mean = priceHistory.reduce((s, p) => s + p, 0) / priceHistory.length;
    const variance = priceHistory.reduce((s, p) => s + Math.pow(p - mean, 2), 0) / priceHistory.length;
    return Math.sqrt(variance);
  }, [priceHistory]);

  const buyRatio = useMemo(() => {
    if (trades.length === 0) return 50;
    const buys = trades.filter((t) => t.side === 'buy').length;
    return (buys / trades.length) * 100;
  }, [trades]);

  return (
    <>
      {/* Top Metrics */}
      <div className="grid-overview">
        <MetricCard
          label="RSI (14)"
          value={rsi.toFixed(1)}
          changeDirection={rsi > 70 ? 'down' : rsi < 30 ? 'up' : 'neutral'}
          change={rsi > 70 ? 'Overbought' : rsi < 30 ? 'Oversold' : 'Neutral'}
          icon="📐"
          id="metric-rsi"
        />
        <MetricCard
          label="VWAP"
          value={vwap > 0 ? formatUSD(vwap) : '—'}
          changeDirection={stats.currentPrice > vwap ? 'up' : 'down'}
          change={stats.currentPrice > vwap ? 'Above VWAP' : 'Below VWAP'}
          icon="⚖️"
          id="metric-vwap"
        />
        <MetricCard
          label="Volatility (σ)"
          value={priceStd > 0 ? `$${priceStd.toFixed(2)}` : '—'}
          icon="📊"
          id="metric-volatility"
        />
        <MetricCard
          label="Buy/Sell Ratio"
          value={`${buyRatio.toFixed(1)}%`}
          changeDirection={buyRatio > 55 ? 'up' : buyRatio < 45 ? 'down' : 'neutral'}
          change={buyRatio > 55 ? 'Bullish' : buyRatio < 45 ? 'Bearish' : 'Balanced'}
          icon="⚡"
          id="metric-ratio"
        />
      </div>

      <div className="grid-analytics">
        {/* Left Column: Charts & Details */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          {/* Price Distribution */}
          <Card title="Price Trend Analysis">
            <div style={{ display: 'flex', justifyContent: 'center', marginBottom: '16px' }}>
              <MiniSparkline
                data={priceHistory.length > 2 ? priceHistory : [0, 0]}
                width={560}
                height={140}
                color={stats.priceChangePercent24h >= 0 ? '#0ecb81' : '#f6465d'}
                fillOpacity={0.06}
              />
            </div>
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(4, 1fr)',
              gap: '12px',
              fontSize: '12px',
            }}>
              {[
                { label: 'Mean', value: stats.avgPrice1m > 0 ? formatUSD(stats.avgPrice1m) : '—' },
                { label: 'Std Dev', value: priceStd > 0 ? `$${priceStd.toFixed(2)}` : '—' },
                { label: 'Samples', value: formatNumber(priceHistory.length) },
                { label: 'Range', value: stats.high24h > 0 ? formatUSD(stats.high24h - stats.low24h) : '—' },
              ].map((item) => (
                <div key={item.label} style={{
                  padding: '10px',
                  background: 'var(--bg-secondary)',
                  borderRadius: 'var(--radius-sm)',
                  textAlign: 'center',
                }}>
                  <div style={{ color: 'var(--text-tertiary)', fontSize: '10px', marginBottom: '4px', textTransform: 'uppercase' }}>
                    {item.label}
                  </div>
                  <div style={{ fontFamily: 'var(--font-mono)', fontWeight: 600, color: 'var(--text-primary)' }}>
                    {item.value}
                  </div>
                </div>
              ))}
            </div>
          </Card>

          {/* Volume Analysis */}
          <Card title="Volume Analysis">
            <div style={{ display: 'flex', justifyContent: 'center', marginBottom: '16px' }}>
              <MiniSparkline
                data={volumeHistory.length > 2 ? volumeHistory : [0, 0]}
                width={560}
                height={80}
                color="#f0b90b"
                fillOpacity={0.08}
              />
            </div>
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(3, 1fr)',
              gap: '12px',
              fontSize: '12px',
            }}>
              {[
                { label: 'Total Volume', value: formatVolume(stats.volume24h) + ' BTC' },
                { label: 'Total Trades', value: formatNumber(stats.tradeCount24h) },
                { label: 'Avg Trade Size', value: stats.tradeCount24h > 0 ? formatVolume(stats.volume24h / stats.tradeCount24h) + ' BTC' : '—' },
              ].map((item) => (
                <div key={item.label} style={{
                  padding: '10px',
                  background: 'var(--bg-secondary)',
                  borderRadius: 'var(--radius-sm)',
                  textAlign: 'center',
                }}>
                  <div style={{ color: 'var(--text-tertiary)', fontSize: '10px', marginBottom: '4px', textTransform: 'uppercase' }}>
                    {item.label}
                  </div>
                  <div style={{ fontFamily: 'var(--font-mono)', fontWeight: 600, color: 'var(--text-primary)' }}>
                    {item.value}
                  </div>
                </div>
              ))}
            </div>
          </Card>

          {/* Buy/Sell Distribution */}
          <Card title="Buy/Sell Distribution">
            <div style={{ padding: '8px 0' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '8px' }}>
                <div
                  style={{
                    flex: buyRatio,
                    height: '24px',
                    background: 'var(--up)',
                    borderRadius: '4px 0 0 4px',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    fontSize: '11px',
                    fontWeight: 700,
                    color: '#0b0e11',
                    minWidth: buyRatio > 10 ? 'auto' : '30px',
                  }}
                >
                  {buyRatio > 15 ? `BUY ${buyRatio.toFixed(1)}%` : ''}
                </div>
                <div
                  style={{
                    flex: 100 - buyRatio,
                    height: '24px',
                    background: 'var(--down)',
                    borderRadius: '0 4px 4px 0',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    fontSize: '11px',
                    fontWeight: 700,
                    color: '#fff',
                    minWidth: (100 - buyRatio) > 10 ? 'auto' : '30px',
                  }}
                >
                  {(100 - buyRatio) > 15 ? `SELL ${(100 - buyRatio).toFixed(1)}%` : ''}
                </div>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px', color: 'var(--text-tertiary)' }}>
                <span>Buyers: {trades.filter(t => t.side === 'buy').length}</span>
                <span>Sellers: {trades.filter(t => t.side === 'sell').length}</span>
              </div>
            </div>
          </Card>
        </div>

        {/* Right Column: Indicators */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          <Card title="RSI Indicator" noPadding>
            <RSIIndicator value={rsi} />
          </Card>

          <Card title="VWAP" noPadding>
            <VWAPDisplay vwap={vwap} currentPrice={stats.currentPrice} />
          </Card>

          <Card title="ML Price Prediction" noPadding>
            <PredictionCard prediction={null} />
          </Card>
        </div>
      </div>
    </>
  );
}

export default function AnalyticsPage() {
  return (
    <AppShell title="Analytics" description="Phân tích kỹ thuật & Dự báo ML">
      <AnalyticsContent />
    </AppShell>
  );
}
