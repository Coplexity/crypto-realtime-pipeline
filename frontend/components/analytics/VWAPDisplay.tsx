'use client';

import { formatUSD } from '@/lib/formatters';

interface VWAPDisplayProps {
  vwap: number;
  currentPrice: number;
}

export default function VWAPDisplay({ vwap, currentPrice }: VWAPDisplayProps) {
  const isAbove = currentPrice > vwap;
  const deviation = vwap > 0 ? ((currentPrice - vwap) / vwap) * 100 : 0;

  return (
    <div style={{ padding: '24px' }} id="vwap-display">
      <div style={{ textAlign: 'center', marginBottom: '20px' }}>
        <div style={{ fontSize: '12px', color: 'var(--text-tertiary)', marginBottom: '6px', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
          VWAP (Volume Weighted Average Price)
        </div>
        <div style={{ fontSize: '32px', fontWeight: 800, fontFamily: 'var(--font-mono)', color: 'var(--accent)' }}>
          {vwap > 0 ? formatUSD(vwap) : '—'}
        </div>
      </div>

      {vwap > 0 && (
        <>
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              padding: '12px 16px',
              background: isAbove ? 'var(--up-bg)' : 'var(--down-bg)',
              borderRadius: 'var(--radius-md)',
              marginBottom: '12px',
            }}
          >
            <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
              Price vs VWAP
            </span>
            <span
              style={{
                fontSize: '14px',
                fontWeight: 700,
                fontFamily: 'var(--font-mono)',
                color: isAbove ? 'var(--up)' : 'var(--down)',
              }}
            >
              {isAbove ? '▲' : '▼'} {deviation >= 0 ? '+' : ''}{deviation.toFixed(3)}%
            </span>
          </div>

          <div style={{ fontSize: '11px', color: 'var(--text-tertiary)', lineHeight: '1.6' }}>
            {isAbove ? (
              <>
                Giá hiện tại <strong style={{ color: 'var(--up)' }}>cao hơn</strong> VWAP, cho thấy xu hướng tăng giá.
                Các trader thường xem đây là tín hiệu bullish.
              </>
            ) : (
              <>
                Giá hiện tại <strong style={{ color: 'var(--down)' }}>thấp hơn</strong> VWAP, cho thấy xu hướng giảm giá.
                Các trader thường xem đây là tín hiệu bearish.
              </>
            )}
          </div>
        </>
      )}

      {vwap === 0 && (
        <div style={{ textAlign: 'center', color: 'var(--text-tertiary)', fontSize: '13px' }}>
          Đang tính VWAP từ dữ liệu Spark...
        </div>
      )}
    </div>
  );
}
