'use client';

import { useEffect, useState } from 'react';
import { API_BASE_URL } from '@/lib/constants';

interface HealthData {
  status: string;
  kafka_configured: boolean;
  kafka_topic: string;
}

export default function APIHealth() {
  const [health, setHealth] = useState<HealthData | null>(null);
  const [apiLatency, setApiLatency] = useState<number>(0);
  const [isOnline, setIsOnline] = useState(false);
  const [lastCheck, setLastCheck] = useState<string>('');

  useEffect(() => {
    const checkHealth = async () => {
      const start = performance.now();
      try {
        const res = await fetch(`${API_BASE_URL}/health`, { cache: 'no-store' });
        const data = await res.json();
        const latency = performance.now() - start;

        setHealth(data);
        setApiLatency(Math.round(latency));
        setIsOnline(true);
        setLastCheck(new Date().toLocaleTimeString('en-US', { hour12: false }));
      } catch {
        setIsOnline(false);
        setApiLatency(0);
        setLastCheck(new Date().toLocaleTimeString('en-US', { hour12: false }));
      }
    };

    checkHealth();
    const interval = setInterval(checkHealth, 10000); // Check every 10s
    return () => clearInterval(interval);
  }, []);

  return (
    <div style={{ padding: '18px' }} id="api-health">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span className={`status-dot ${isOnline ? 'connected' : 'disconnected'}`} />
          <span style={{ fontSize: '14px', fontWeight: 600, color: isOnline ? 'var(--up)' : 'var(--down)' }}>
            {isOnline ? 'Online' : 'Offline'}
          </span>
        </div>
        <span className={`status-badge ${isOnline ? 'healthy' : 'down'}`}>
          {isOnline ? 'HEALTHY' : 'DOWN'}
        </span>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '10px', fontSize: '13px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: 'var(--text-tertiary)' }}>Endpoint</span>
          <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-secondary)', fontSize: '11px' }}>
            {API_BASE_URL}
          </span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: 'var(--text-tertiary)' }}>Latency</span>
          <span style={{
            fontFamily: 'var(--font-mono)',
            fontWeight: 600,
            color: apiLatency < 100 ? 'var(--up)' : apiLatency < 500 ? 'var(--accent)' : 'var(--down)',
          }}>
            {apiLatency}ms
          </span>
        </div>
        {health && (
          <>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: 'var(--text-tertiary)' }}>Kafka</span>
              <span style={{ fontFamily: 'var(--font-mono)', color: health.kafka_configured ? 'var(--up)' : 'var(--down)' }}>
                {health.kafka_configured ? '✓ Connected' : '✗ Disconnected'}
              </span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: 'var(--text-tertiary)' }}>Topic</span>
              <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent)', fontSize: '11px' }}>
                {health.kafka_topic}
              </span>
            </div>
          </>
        )}
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: 'var(--text-tertiary)' }}>Last Check</span>
          <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-tertiary)', fontSize: '11px' }}>
            {lastCheck}
          </span>
        </div>
      </div>
    </div>
  );
}
