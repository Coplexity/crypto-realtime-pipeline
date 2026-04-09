'use client';

import { useMemo } from 'react';
import AppShell, { useAppContext } from '@/components/AppShell';
import Card from '@/components/common/Card';
import MetricCard from '@/components/common/MetricCard';
import APIHealth from '@/components/system/APIHealth';
import KafkaStatus from '@/components/system/KafkaStatus';
import SparkJobs from '@/components/system/SparkJobs';
import { formatNumber, formatDuration } from '@/lib/formatters';
import type { KafkaMetrics, SparkJob } from '@/types';

function SystemContent() {
  const { stats, tradesPerSecond, messagesReceived, connectionStatus } = useAppContext();

  // Simulated Kafka metrics based on actual data flowing through
  const kafkaMetrics: KafkaMetrics = useMemo(() => ({
    topic: 'binance_trades',
    partitions: 3,
    messages_per_second: tradesPerSecond,
    consumer_lag: Math.max(0, Math.floor(Math.random() * 50)),
    total_messages: messagesReceived,
  }), [tradesPerSecond, messagesReceived]);

  // Simulated Spark jobs
  const sparkJobs: SparkJob[] = useMemo(() => [
    {
      id: 'spark-streaming-001',
      name: 'CryptoRealtimeProcessor',
      status: 'running' as const,
      start_time: new Date(Date.now() - 3600000).toLocaleTimeString(),
      duration: formatDuration(3600000),
      records_processed: stats.tradeCount24h,
    },
    {
      id: 'spark-batch-002',
      name: 'OHLCV Aggregation (1m)',
      status: 'running' as const,
      start_time: new Date(Date.now() - 1800000).toLocaleTimeString(),
      duration: formatDuration(1800000),
      records_processed: Math.floor(stats.tradeCount24h / 60),
    },
    {
      id: 'spark-ml-003',
      name: 'Price Prediction (MLlib)',
      status: 'completed' as const,
      start_time: new Date(Date.now() - 600000).toLocaleTimeString(),
      duration: '2m 34s',
      records_processed: 15000,
    },
  ], [stats.tradeCount24h]);

  const uptimeMs = Date.now() - (Date.now() - 3600000); // Simulated 1 hour

  return (
    <>
      {/* System Overview Metrics */}
      <div className="grid-overview">
        <MetricCard
          label="System Status"
          value={connectionStatus === 'connected' ? 'Online' : 'Degraded'}
          changeDirection={connectionStatus === 'connected' ? 'up' : 'down'}
          icon="🖥️"
          id="metric-system-status"
        />
        <MetricCard
          label="Uptime"
          value={formatDuration(uptimeMs)}
          icon="⏱️"
          id="metric-uptime"
        />
        <MetricCard
          label="Messages Processed"
          value={formatNumber(messagesReceived)}
          change={`${tradesPerSecond} msg/s`}
          changeDirection="up"
          icon="📨"
          id="metric-messages"
        />
        <MetricCard
          label="Active Connections"
          value={connectionStatus === 'connected' ? '1' : '0'}
          icon="🔗"
          id="metric-connections"
        />
      </div>

      {/* Architecture Pipeline Status */}
      <Card title="Pipeline Status" id="pipeline-status">
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, 1fr)', gap: '8px', alignItems: 'center' }}>
          {[
            { name: 'Binance WS', status: connectionStatus === 'connected' ? 'healthy' : 'down', icon: '🌐' },
            { name: 'arrow', status: 'arrow' },
            { name: 'Kafka Broker', status: 'healthy', icon: '📨' },
            { name: 'arrow', status: 'arrow' },
            { name: 'Spark Stream', status: 'healthy', icon: '⚡' },
            { name: 'arrow', status: 'arrow' },
          ].map((item, i) => {
            if (item.status === 'arrow') {
              return (
                <div key={`arrow-${i}`} style={{ textAlign: 'center', fontSize: '18px', color: 'var(--accent)' }}>
                  →
                </div>
              );
            }
            return (
              <div
                key={item.name}
                style={{
                  padding: '12px 8px',
                  background: 'var(--bg-secondary)',
                  borderRadius: 'var(--radius-md)',
                  border: '1px solid var(--border)',
                  textAlign: 'center',
                }}
              >
                <div style={{ fontSize: '20px', marginBottom: '6px' }}>{item.icon}</div>
                <div style={{ fontSize: '11px', fontWeight: 600, color: 'var(--text-primary)', marginBottom: '4px' }}>
                  {item.name}
                </div>
                <span className={`status-badge ${item.status}`}>
                  {item.status === 'healthy' ? 'OK' : 'DOWN'}
                </span>
              </div>
            );
          })}
          {/* Continue pipeline */}
          {[
            { name: 'MongoDB', status: 'healthy', icon: '🗃️' },
            { name: 'arrow', status: 'arrow' },
            { name: 'Redis Cache', status: 'healthy', icon: '💾' },
            { name: 'arrow', status: 'arrow' },
            { name: 'FastAPI', status: connectionStatus === 'connected' ? 'healthy' : 'down', icon: '🔌' },
            { name: 'arrow', status: 'arrow' },
          ].map((item, i) => {
            if (item.status === 'arrow') {
              return (
                <div key={`arrow2-${i}`} style={{ textAlign: 'center', fontSize: '18px', color: 'var(--accent)' }}>
                  →
                </div>
              );
            }
            return (
              <div
                key={item.name}
                style={{
                  padding: '12px 8px',
                  background: 'var(--bg-secondary)',
                  borderRadius: 'var(--radius-md)',
                  border: '1px solid var(--border)',
                  textAlign: 'center',
                }}
              >
                <div style={{ fontSize: '20px', marginBottom: '6px' }}>{item.icon}</div>
                <div style={{ fontSize: '11px', fontWeight: 600, color: 'var(--text-primary)', marginBottom: '4px' }}>
                  {item.name}
                </div>
                <span className={`status-badge ${item.status}`}>
                  {item.status === 'healthy' ? 'OK' : 'DOWN'}
                </span>
              </div>
            );
          })}
        </div>
      </Card>

      {/* Detailed Status */}
      <div className="status-grid" style={{ marginTop: '16px' }}>
        <Card title="FastAPI Backend" noPadding>
          <APIHealth />
        </Card>

        <Card title="Apache Kafka" noPadding>
          <KafkaStatus metrics={kafkaMetrics} />
        </Card>

        <Card title="Docker Services">
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', fontSize: '13px' }}>
            {[
              { name: 'zookeeper', port: 2181, status: 'running' },
              { name: 'kafka', port: 9092, status: 'running' },
              { name: 'namenode (HDFS)', port: 9870, status: 'running' },
              { name: 'datanode (HDFS)', port: 9864, status: 'running' },
              { name: 'spark-master', port: 8080, status: 'running' },
              { name: 'spark-worker', port: 8081, status: 'running' },
              { name: 'mongodb', port: 27017, status: 'running' },
              { name: 'grafana', port: 3000, status: 'running' },
            ].map((svc) => (
              <div key={svc.name} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <span className={`status-dot connected`} style={{ width: '6px', height: '6px' }} />
                  <span style={{ color: 'var(--text-primary)', fontWeight: 500 }}>{svc.name}</span>
                </div>
                <span style={{ fontFamily: 'var(--font-mono)', fontSize: '11px', color: 'var(--text-tertiary)' }}>
                  :{svc.port}
                </span>
              </div>
            ))}
          </div>
        </Card>
      </div>

      {/* Spark Jobs */}
      <Card title="Spark Jobs" noPadding style={{ marginTop: '16px' }}>
        <SparkJobs jobs={sparkJobs} />
      </Card>

      {/* Technology Stack */}
      <Card title="Technology Stack" style={{ marginTop: '16px' }} id="tech-stack">
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '12px' }}>
          {[
            { category: 'Processing', tech: 'Apache Spark 3.2', detail: 'PySpark + Structured Streaming' },
            { category: 'Message Queue', tech: 'Apache Kafka 7.3', detail: '3 partitions, Snappy compression' },
            { category: 'Storage', tech: 'HDFS + MongoDB', detail: 'Parquet (batch) + JSON (realtime)' },
            { category: 'Cache', tech: 'Redis', detail: 'Hot data, sub-ms reads' },
            { category: 'Backend', tech: 'FastAPI', detail: 'REST + WebSocket, async' },
            { category: 'Frontend', tech: 'Next.js 15', detail: 'React 19, Tailwind CSS v4' },
            { category: 'Orchestration', tech: 'Docker Compose', detail: 'Kubernetes (Phase 3)' },
            { category: 'Monitoring', tech: 'Grafana', detail: 'Prometheus metrics' },
          ].map((item) => (
            <div
              key={item.category}
              style={{
                padding: '14px',
                background: 'var(--bg-secondary)',
                borderRadius: 'var(--radius-md)',
                border: '1px solid var(--border)',
              }}
            >
              <div style={{ fontSize: '10px', color: 'var(--text-tertiary)', textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: '4px' }}>
                {item.category}
              </div>
              <div style={{ fontSize: '13px', fontWeight: 700, color: 'var(--accent)', marginBottom: '2px' }}>
                {item.tech}
              </div>
              <div style={{ fontSize: '11px', color: 'var(--text-tertiary)' }}>
                {item.detail}
              </div>
            </div>
          ))}
        </div>
      </Card>
    </>
  );
}

export default function SystemPage() {
  return (
    <AppShell title="System Monitor" description="Giám sát hạ tầng & hiệu năng hệ thống">
      <SystemContent />
    </AppShell>
  );
}
