'use client';

import type { ConnectionStatus } from '@/types';

interface HeaderProps {
  title: string;
  description?: string;
  connectionStatus: ConnectionStatus;
  reconnectAttempt: number;
  messagesReceived: number;
  tradesPerSecond: number;
}

const STATUS_LABELS: Record<ConnectionStatus, string> = {
  connected: 'Live',
  connecting: 'Connecting...',
  reconnecting: 'Reconnecting...',
  disconnected: 'Offline',
};

export default function Header({
  title,
  description,
  connectionStatus,
  reconnectAttempt,
  messagesReceived,
  tradesPerSecond,
}: HeaderProps) {
  return (
    <header className="header" id="app-header">
      <div className="header-title">
        <div>
          <h1>{title}</h1>
          {description && <div className="header-title-desc">{description}</div>}
        </div>
      </div>

      <div className="header-right">
        {/* Messages counter */}
        <div
          style={{
            fontSize: '11px',
            fontFamily: 'var(--font-mono)',
            color: 'var(--text-tertiary)',
            display: 'flex',
            gap: '16px',
          }}
        >
          <span>
            <span style={{ color: 'var(--text-secondary)' }}>TPS:</span>{' '}
            <span style={{ color: 'var(--accent)' }}>{tradesPerSecond}</span>
          </span>
          <span>
            <span style={{ color: 'var(--text-secondary)' }}>Total:</span>{' '}
            {messagesReceived.toLocaleString()}
          </span>
        </div>

        {/* Connection status */}
        <div className={`connection-status ${connectionStatus}`} id="connection-status">
          <span className={`status-dot ${connectionStatus}`} />
          <span>
            {STATUS_LABELS[connectionStatus]}
            {connectionStatus === 'reconnecting' && ` (#${reconnectAttempt})`}
          </span>
        </div>
      </div>
    </header>
  );
}
