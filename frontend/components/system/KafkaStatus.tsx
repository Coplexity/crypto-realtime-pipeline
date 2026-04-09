'use client';

import type { KafkaMetrics } from '@/types';
import { formatNumber } from '@/lib/formatters';

interface KafkaStatusProps {
  metrics: KafkaMetrics;
}

export default function KafkaStatus({ metrics }: KafkaStatusProps) {
  const lagStatus = metrics.consumer_lag < 100 ? 'healthy' : metrics.consumer_lag < 1000 ? 'degraded' : 'down';

  return (
    <div style={{ padding: '18px' }} id="kafka-status">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
        <div style={{ fontSize: '14px', fontWeight: 600, color: 'var(--text-primary)' }}>
          Apache Kafka
        </div>
        <span className={`status-badge ${lagStatus}`}>
          {lagStatus === 'healthy' ? 'OPTIMAL' : lagStatus === 'degraded' ? 'LAG' : 'HIGH LAG'}
        </span>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '10px', fontSize: '13px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: 'var(--text-tertiary)' }}>Topic</span>
          <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent)', fontSize: '12px' }}>
            {metrics.topic}
          </span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: 'var(--text-tertiary)' }}>Partitions</span>
          <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-secondary)' }}>
            {metrics.partitions}
          </span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: 'var(--text-tertiary)' }}>Throughput</span>
          <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--up)', fontWeight: 600 }}>
            {metrics.messages_per_second} msg/s
          </span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: 'var(--text-tertiary)' }}>Consumer Lag</span>
          <span style={{
            fontFamily: 'var(--font-mono)',
            fontWeight: 600,
            color: lagStatus === 'healthy' ? 'var(--up)' : lagStatus === 'degraded' ? 'var(--accent)' : 'var(--down)',
          }}>
            {formatNumber(metrics.consumer_lag)}
          </span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: 'var(--text-tertiary)' }}>Total Messages</span>
          <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-secondary)' }}>
            {formatNumber(metrics.total_messages)}
          </span>
        </div>
      </div>

      {/* Consumer lag bar */}
      <div style={{ marginTop: '14px' }}>
        <div style={{ fontSize: '11px', color: 'var(--text-tertiary)', marginBottom: '4px' }}>
          Consumer Lag Health
        </div>
        <div style={{ height: '6px', background: 'var(--bg-tertiary)', borderRadius: '3px', overflow: 'hidden' }}>
          <div
            style={{
              height: '100%',
              width: `${Math.min((metrics.consumer_lag / 1000) * 100, 100)}%`,
              background: lagStatus === 'healthy' ? 'var(--up)' : lagStatus === 'degraded' ? 'var(--accent)' : 'var(--down)',
              borderRadius: '3px',
              transition: 'width 0.3s ease',
            }}
          />
        </div>
      </div>
    </div>
  );
}
