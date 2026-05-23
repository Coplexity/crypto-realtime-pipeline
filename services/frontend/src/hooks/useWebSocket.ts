"use client"
import { useEffect, useRef, useState, useCallback } from "react"

// Đổi type của callback để nhận nguyên cục message từ Backend
export function useWebSocket(symbol: string, interval: string, onMessage: (msg: any) => void) {
  const ws = useRef<WebSocket | null>(null)
  const [connected, setConnected] = useState(false)
  const onMessageRef = useRef(onMessage)
  onMessageRef.current = onMessage

  // [FIX LỖI 1]: Cờ đánh dấu xem việc đóng WebSocket có phải do mình chủ động hay không
  const isIntentionalClose = useRef(false)

  const connect = useCallback(() => {
    isIntentionalClose.current = false // Reset cờ mỗi lần kết nối
    const WS_URL = process.env.NEXT_PUBLIC_WS_URL || "ws://localhost:8000"
    
    const socket = new WebSocket(`${WS_URL}/ws/kline?symbol=${symbol}&interval=${interval}`)
    ws.current = socket

    socket.onopen  = () => setConnected(true)
    
    socket.onclose = () => {
      setConnected(false)
      // CHỈ reconnect nếu sự cố đứt mạng ngoài ý muốn
      if (!isIntentionalClose.current) {
        console.log(`[WS] Mất kết nối ${symbol}. Thử lại sau 3s...`)
        setTimeout(connect, 3000)
      }
    }
    
    socket.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data)
        // [FIX LỖI 2]: Trả về toàn bộ message (cả initial chứa mảng candles, và update/realtime)
        if (msg.type) {
          onMessageRef.current(msg)
        }
      } catch (err) {
        console.error("Lỗi parse WS data:", err)
      }
    }
  }, [symbol, interval]) // Giữ nguyên dependencies

  useEffect(() => {
    connect()
    
    // Cleanup function: Chạy khi component unmount hoặc symbol/interval thay đổi
    return () => {
      isIntentionalClose.current = true // Đánh dấu là tôi chủ động đóng kết nối
      ws.current?.close()
    }
  }, [connect])

  return { connected }
}