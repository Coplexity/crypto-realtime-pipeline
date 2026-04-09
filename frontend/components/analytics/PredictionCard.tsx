'use client';

import type { PredictionResult } from '@/types';
import { formatUSD } from '@/lib/formatters';

interface PredictionCardProps {
  prediction: PredictionResult | null;
}

export default function PredictionCard({ prediction }: PredictionCardProps) {
  if (!prediction) {
    return (
      <div style={{ padding: '24px', textAlign: 'center' }}>
        <div style={{ fontSize: '32px', marginBottom: '12px', opacity: 0.5 }}>🤖</div>
        <div style={{ fontSize: '14px', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: '8px' }}>
          ML Prediction
        </div>
        <div style={{ fontSize: '12px', color: 'var(--text-tertiary)', lineHeight: '1.6' }}>
          Mô hình Spark MLlib đang xử lý dữ liệu.
          <br />
          Dự báo sẽ hiển thị khi có đủ dữ liệu huấn luyện.
        </div>
        <div
          style={{
            marginTop: '16px',
            padding: '8px 12px',
            background: 'var(--accent-muted)',
            borderRadius: 'var(--radius-sm)',
            fontSize: '11px',
            fontFamily: 'var(--font-mono)',
            color: 'var(--accent)',
          }}
        >
          Model: Linear Regression (MLlib)
        </div>
      </div>
    );
  }

  const dirColor = prediction.direction === 'up' ? 'var(--up)' : prediction.direction === 'down' ? 'var(--down)' : 'var(--accent)';
  const dirIcon = prediction.direction === 'up' ? '📈' : prediction.direction === 'down' ? '📉' : '➡️';
  const dirLabel = prediction.direction === 'up' ? 'TĂNG' : prediction.direction === 'down' ? 'GIẢM' : 'SIDEWAY';

  return (
    <div style={{ padding: '24px' }} id="ml-prediction">
      <div style={{ textAlign: 'center', marginBottom: '16px' }}>
        <div style={{ fontSize: '28px', marginBottom: '8px' }}>{dirIcon}</div>
        <div style={{ fontSize: '14px', fontWeight: 600, color: 'var(--text-secondary)' }}>
          Dự báo biến động
        </div>
      </div>

      <div
        style={{
          textAlign: 'center',
          padding: '16px',
          background: prediction.direction === 'up' ? 'var(--up-bg)' : prediction.direction === 'down' ? 'var(--down-bg)' : 'var(--accent-muted)',
          borderRadius: 'var(--radius-md)',
          marginBottom: '16px',
        }}
      >
        <div style={{ fontSize: '18px', fontWeight: 800, color: dirColor, letterSpacing: '1px' }}>
          {dirLabel}
        </div>
        <div style={{ fontSize: '24px', fontWeight: 700, fontFamily: 'var(--font-mono)', color: dirColor, marginTop: '4px' }}>
          {formatUSD(prediction.predicted_price)}
        </div>
      </div>

      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '12px' }}>
        <span style={{ color: 'var(--text-tertiary)' }}>Confidence</span>
        <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 600, color: 'var(--text-secondary)' }}>
          {(prediction.confidence * 100).toFixed(1)}%
        </span>
      </div>

      <div
        style={{
          height: '4px',
          background: 'var(--bg-tertiary)',
          borderRadius: '2px',
          marginTop: '6px',
          overflow: 'hidden',
        }}
      >
        <div
          style={{
            height: '100%',
            width: `${prediction.confidence * 100}%`,
            background: dirColor,
            borderRadius: '2px',
            transition: 'width 0.3s ease',
          }}
        />
      </div>

      <div style={{ marginTop: '12px', fontSize: '10px', fontFamily: 'var(--font-mono)', color: 'var(--text-tertiary)', textAlign: 'center' }}>
        Model: {prediction.model}
      </div>
    </div>
  );
}
