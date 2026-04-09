'use client';

import type { TradeDisplay } from '@/types';
import { formatPrice, formatQuantity } from '@/lib/formatters';

interface TradeHistoryProps {
  trades: TradeDisplay[];
  maxDisplay?: number;
}

export default function TradeHistory({ trades, maxDisplay = 50 }: TradeHistoryProps) {
  const displayTrades = trades.slice(0, maxDisplay);

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <div className="trade-table-header">
        <span>Price (USDT)</span>
        <span style={{ textAlign: 'right' }}>Qty (BTC)</span>
        <span style={{ textAlign: 'right' }}>Time</span>
      </div>

      <div style={{ flex: 1, overflowY: 'auto' }}>
        {displayTrades.length === 0 ? (
          <div className="empty-state">
            <div className="empty-state-icon">📡</div>
            <div>Đang chờ giao dịch...</div>
          </div>
        ) : (
          displayTrades.map((trade, i) => (
            <div
              key={`${trade.id}-${i}`}
              className={`trade-row ${trade.side}`}
            >
              <span className="trade-price">{formatPrice(trade.price)}</span>
              <span className="trade-qty">{formatQuantity(trade.quantity)}</span>
              <span className="trade-time">{trade.time}</span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
