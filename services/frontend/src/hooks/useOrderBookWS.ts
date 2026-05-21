"use client"
import { useEffect, useRef, useState, useCallback } from "react"

interface OrderBookEntry {
  price: number
  quantity: number
  total: number
}

interface OrderBookState {
  bids: OrderBookEntry[]
  asks: OrderBookEntry[]
  spread: number | null
  timestamp: number | null
  connected: boolean
}

export function useOrderBookWS(symbol: string): OrderBookState {
  const [state, setState] = useState<OrderBookState>({
    bids: [], asks: [], spread: null, timestamp: null, connected: false
  })
  const wsRef = useRef<WebSocket | null>(null)
  const isIntentionalClose = useRef(false)

  const connect = useCallback(() => {
    isIntentionalClose.current = false
    const WS_URL = process.env.NEXT_PUBLIC_WS_URL || "ws://localhost:8000"
    const ws = new WebSocket(`${WS_URL}/ws/orderbook?symbol=${symbol}`)
    wsRef.current = ws

    ws.onopen = () => setState(s => ({ ...s, connected: true }))

    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data)
        if (msg.type === "initial" || msg.type === "update") {
          const bids: OrderBookEntry[] = msg.bids || []
          const asks: OrderBookEntry[] = msg.asks || []
          const topBid = bids[0]?.price ?? null
          const topAsk = asks[0]?.price ?? null
          const spread = topBid && topAsk ? topAsk - topBid : null

          setState(s => ({
            ...s,
            bids,
            asks,
            spread,
            timestamp: msg.timestamp ?? Date.now(),
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

    ws.onerror = () => {
      setState(s => ({ ...s, connected: false }))
    }
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
