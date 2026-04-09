'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import type { ConnectionStatus, WebSocketMessage, RawTrade } from '@/types';
import {
  WS_URL,
  WS_RECONNECT_BASE_DELAY,
  WS_RECONNECT_MAX_DELAY,
  WS_MAX_RECONNECT_ATTEMPTS,
} from '@/lib/constants';

interface UseWebSocketReturn {
  status: ConnectionStatus;
  lastTrade: RawTrade | null;
  error: string | null;
  reconnectAttempt: number;
  messagesReceived: number;
  connect: () => void;
  disconnect: () => void;
}

export function useWebSocket(): UseWebSocketReturn {
  const [status, setStatus] = useState<ConnectionStatus>('disconnected');
  const [lastTrade, setLastTrade] = useState<RawTrade | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [reconnectAttempt, setReconnectAttempt] = useState(0);
  const [messagesReceived, setMessagesReceived] = useState(0);

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const messageCountRef = useRef(0);
  const isManualDisconnect = useRef(false);

  const clearReconnectTimer = useCallback(() => {
    if (reconnectTimerRef.current) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
  }, []);

  const scheduleReconnect = useCallback((attempt: number) => {
    if (attempt >= WS_MAX_RECONNECT_ATTEMPTS) {
      setError(`Đã thử kết nối lại ${WS_MAX_RECONNECT_ATTEMPTS} lần. Vui lòng kiểm tra backend.`);
      setStatus('disconnected');
      return;
    }

    const delay = Math.min(
      WS_RECONNECT_BASE_DELAY * Math.pow(2, attempt),
      WS_RECONNECT_MAX_DELAY
    );

    setStatus('reconnecting');
    setReconnectAttempt(attempt + 1);

    reconnectTimerRef.current = setTimeout(() => {
      connectWs(attempt + 1);
    }, delay);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const connectWs = useCallback((attempt: number = 0) => {
    // Cleanup existing
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }

    setStatus('connecting');
    setError(null);

    try {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        setStatus('connected');
        setReconnectAttempt(0);
        setError(null);
        isManualDisconnect.current = false;
      };

      ws.onmessage = (event) => {
        try {
          const message: WebSocketMessage = JSON.parse(event.data);

          if (message.type === 'trade' && message.data) {
            setLastTrade(message.data);
          } else if (message.data) {
            // Fallback: some messages might come without type wrapper
            setLastTrade(message.data);
          } else {
            // Direct trade data from simple backend
            const trade = JSON.parse(event.data) as RawTrade;
            if (trade.price) {
              setLastTrade(trade);
            }
          }

          messageCountRef.current += 1;
          // Batch state updates - only update count every 10 messages
          if (messageCountRef.current % 10 === 0) {
            setMessagesReceived(messageCountRef.current);
          }
        } catch {
          // Try parsing as direct trade
          try {
            const directTrade = JSON.parse(event.data);
            if (directTrade.price) {
              setLastTrade(directTrade);
              messageCountRef.current += 1;
            }
          } catch {
            // Ignore unparseable messages
          }
        }
      };

      ws.onerror = () => {
        setError('Lỗi kết nối WebSocket');
      };

      ws.onclose = (event) => {
        wsRef.current = null;
        if (!isManualDisconnect.current && event.code !== 1000) {
          scheduleReconnect(attempt);
        } else {
          setStatus('disconnected');
        }
      };
    } catch {
      setError('Không thể tạo kết nối WebSocket');
      scheduleReconnect(attempt);
    }
  }, [scheduleReconnect]);

  const connect = useCallback(() => {
    isManualDisconnect.current = false;
    clearReconnectTimer();
    connectWs(0);
  }, [connectWs, clearReconnectTimer]);

  const disconnect = useCallback(() => {
    isManualDisconnect.current = true;
    clearReconnectTimer();
    if (wsRef.current) {
      wsRef.current.close(1000, 'Manual disconnect');
      wsRef.current = null;
    }
    setStatus('disconnected');
  }, [clearReconnectTimer]);

  // Auto-connect on mount
  useEffect(() => {
    connect();
    return () => {
      isManualDisconnect.current = true;
      clearReconnectTimer();
      if (wsRef.current) {
        wsRef.current.close(1000, 'Component unmount');
      }
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  return {
    status,
    lastTrade,
    error,
    reconnectAttempt,
    messagesReceived,
    connect,
    disconnect,
  };
}
