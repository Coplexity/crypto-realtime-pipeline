"use client"
import { useEffect, useRef, useState, useCallback } from "react"

interface Trade {
  symbol: string
  price: number
  quantity: number
  time: number
  isBuyerMaker: boolean
  tradeId?: string | number
}

interface TradesState {
  trades: Trade[]
  connected: boolean
}

const MAX_TRADES = 50

export function useTradesWS(symbol: string): TradesState {
  const [state, setState] = useState<TradesState>({ trades: [], connected: false })
  const wsRef = useRef<WebSocket | null>(null)
  const isIntentionalClose = useRef(false)

  const connect = useCallback(() => {
    isIntentionalClose.current = false
    const WS_URL = process.env.NEXT_PUBLIC_WS_URL || "ws://localhost:8000"
    const ws = new WebSocket(`${WS_URL}/ws/trades?symbol=${symbol}&limit=30`)
    wsRef.current = ws

    ws.onopen = () => setState(s => ({ ...s, connected: true }))

    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data)

        if (msg.type === "initial" && Array.isArray(msg.trades)) {
          // Load initial snapshot
          setState(s => ({
            ...s,
            trades: msg.trades.slice(0, MAX_TRADES),
            connected: true,
          }))
        } else if (msg.type === "realtime" && msg.trade) {
          // FIX: Bắt đúng type "realtime" và lấy data từ object con "trade"
          const t = msg.trade
          const trade: Trade = {
            symbol: t.symbol || symbol,
            price: Number(t.price || 0),
            quantity: Number(t.quantity || 0),
            time: t.trade_time || t.time || Date.now(),
            isBuyerMaker: t.isBuyerMaker ?? false,
            tradeId: t.tradeId,
          }
          setState(s => ({
            ...s,
            trades: [trade, ...s.trades].slice(0, MAX_TRADES),
            connected: true,
          }))
        } else if (msg.type === "update" || msg.price !== undefined) {
          // Giữ lại logic cũ phòng hờ lúc gọi API snapshot
          const trade: Trade = {
            symbol: msg.symbol || symbol,
            price: Number(msg.price || 0),
            quantity: Number(msg.quantity || 0),
            time: msg.trade_time || msg.time || Date.now(),
            isBuyerMaker: msg.isBuyerMaker ?? false,
            tradeId: msg.tradeId,
          }
          setState(s => ({
            ...s,
            trades: [trade, ...s.trades].slice(0, MAX_TRADES),
            connected: true,
          }))
        }
      } catch {}
    }

    ws.onclose = () => {
      setState(s => ({ ...s, connected: false }))
      if (!isIntentionalClose.current) {
        setTimeout(connect, 3000)
      }
    }

    ws.onerror = () => setState(s => ({ ...s, connected: false }))
  }, [symbol])

  useEffect(() => {
    connect()
    return () => {
      isIntentionalClose.current = true
      wsRef.current?.close()
    }
  }, [connect])

  return state
}