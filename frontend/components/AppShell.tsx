'use client';

import type { ReactNode } from 'react';
import Sidebar from '@/components/layout/Sidebar';
import Header from '@/components/layout/Header';
import { useWebSocket } from '@/hooks/useWebSocket';
import { useTradeData } from '@/hooks/useTradeData';
import { createContext, useContext } from 'react';
import type { RawTrade, TradeDisplay, CandleData, DashboardStats, ConnectionStatus } from '@/types';

// ===== Shared Context =====
// This allows all pages to access the same WebSocket data

interface AppContextType {
  // WebSocket
  connectionStatus: ConnectionStatus;
  reconnectAttempt: number;
  messagesReceived: number;
  lastTrade: RawTrade | null;
  connect: () => void;
  disconnect: () => void;
  // Trade data
  trades: TradeDisplay[];
  candles: CandleData[];
  stats: DashboardStats;
  tradesPerSecond: number;
}

const AppContext = createContext<AppContextType | null>(null);

export function useAppContext() {
  const ctx = useContext(AppContext);
  if (!ctx) throw new Error('useAppContext must be used within AppProvider');
  return ctx;
}

// ===== App Shell =====

interface AppShellProps {
  children: ReactNode;
  title: string;
  description?: string;
}

export default function AppShell({ children, title, description }: AppShellProps) {
  const ws = useWebSocket();
  const tradeData = useTradeData(ws.lastTrade);

  const contextValue: AppContextType = {
    connectionStatus: ws.status,
    reconnectAttempt: ws.reconnectAttempt,
    messagesReceived: ws.messagesReceived,
    lastTrade: ws.lastTrade,
    connect: ws.connect,
    disconnect: ws.disconnect,
    trades: tradeData.trades,
    candles: tradeData.candles,
    stats: tradeData.stats,
    tradesPerSecond: tradeData.tradesPerSecond,
  };

  return (
    <AppContext.Provider value={contextValue}>
      <div className="app-layout">
        <Sidebar />
        <div className="app-main">
          <Header
            title={title}
            description={description}
            connectionStatus={ws.status}
            reconnectAttempt={ws.reconnectAttempt}
            messagesReceived={ws.messagesReceived}
            tradesPerSecond={tradeData.tradesPerSecond}
          />
          <main className="app-content">
            {children}
          </main>
        </div>
      </div>
    </AppContext.Provider>
  );
}
