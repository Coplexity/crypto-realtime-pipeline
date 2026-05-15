"use client"
import { useState, useEffect, useCallback, useRef } from "react"
import dynamic from "next/dynamic"
import OrderBook    from "@/components/OrderBook"
import TradeList    from "@/components/TradeList"
import MLPredictions from "@/components/MLPredictions"
import { fetchOHLC, fetchOrderbook, fetchTrades, fetchPredictions, fetchSymbols } from "@/lib/api"
import { useWebSocket } from "@/hooks/useWebSocket"

const CandleChart = dynamic(() => import("@/components/CandleChart"), { ssr: false })

const INTERVALS = ["1m", "5m", "1h", "4h", "1d"]
const DEFAULT_SYMBOLS = ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","ADAUSDT","XRPUSDT","DOGEUSDT","AVAXUSDT"]

export default function Dashboard() {
  const [symbols,      setSymbols]      = useState<string[]>(DEFAULT_SYMBOLS)
  const [symbol,       setSymbol]       = useState("BTCUSDT")
  // Đổi tên từ interval -> timeframe để không bị trùng với hàm setInterval của JS
  const [timeframe,    setTimeframe]    = useState("5m") 
  const [candles,      setCandles]      = useState<any[]>([])
  const [liveCandle,   setLiveCandle]   = useState<any>(null)
  const [orderbook,    setOrderbook]    = useState<any>(null)
  const [trades,       setTrades]       = useState<any[]>([])
  const [predictions,  setPredictions]  = useState<any[]>([])
  const [prices,       setPrices]       = useState<Record<string,number>>({})

  // WebSocket live candle - sử dụng timeframe mới
  const { connected } = useWebSocket(symbol, timeframe, useCallback((msg: any) => {
    if (msg.candle) {
      setLiveCandle(msg.candle)
      setPrices(prev => ({ ...prev, [msg.candle.symbol]: msg.candle.close }))
    }
  }, []))

  // Load symbols
  useEffect(() => {
    fetchSymbols().then(d => setSymbols(d?.symbols || DEFAULT_SYMBOLS)).catch(() => {})
  }, [])

  // Load OHLC - sử dụng timeframe mới
  useEffect(() => {
    setCandles([])
    fetchOHLC(symbol, timeframe, 200)
      .then(d => setCandles(d?.candles || []))
      .catch(() => {})
  }, [symbol, timeframe])

  // Orderbook polling - Bây giờ setInterval sẽ hoạt động đúng
  useEffect(() => {
    const load = () => fetchOrderbook(symbol).then(setOrderbook).catch(() => {})
    load()
    const id = setInterval(load, 2000)
    return () => clearInterval(id)
  }, [symbol])

  // Trades polling
  useEffect(() => {
    const load = () => fetchTrades(symbol, 30).then(d => setTrades(d?.trades || [])).catch(() => {})
    load()
    const id = setInterval(load, 3000)
    return () => clearInterval(id)
  }, [symbol])

  // Predictions polling
  useEffect(() => {
    const load = () => fetchPredictions().then(d => setPredictions(d?.predictions || [])).catch(() => {})
    load()
    const id = setInterval(load, 30000)
    return () => clearInterval(id)
  }, [])

  const currentPrice = prices[symbol] || candles.at(-1)?.close || 0
  const prevPrice    = candles.at(-2)?.close || currentPrice
  const isUp         = currentPrice >= prevPrice
  const pct          = prevPrice ? ((currentPrice - prevPrice) / prevPrice * 100) : 0

  return (
    <div style={s.root}>
      {/* Header */}
      <header style={s.header}>
        <div style={s.brand}>
          <span style={{ fontSize: 22 }}>⬡</span>
          <span style={s.brandName}>CryptoAnalytics</span>
          <span style={s.brandTag}>Lambda Architecture</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12 }}>
          <span style={{ width: 8, height: 8, borderRadius: "50%", background: connected ? "#22c55e" : "#ef4444", display: "inline-block" }} />
          <span style={{ color: connected ? "#22c55e" : "#ef4444" }}>{connected ? "Live" : "Connecting..."}</span>
        </div>
      </header>

      {/* Symbol ticker bar */}
      <div style={s.tickerBar}>
        {symbols.map(sym => {
          const p = prices[sym]
          return (
            <button key={sym} onClick={() => setSymbol(sym)}
              style={{ ...s.tickerBtn, ...(symbol === sym ? s.tickerActive : {}) }}>
              <span style={{ fontSize: 11, color: "#94a3b8" }}>{sym.replace("USDT","")}</span>
              <span style={{ fontSize: 13, fontWeight: 600 }}>
                {p ? `$${p.toLocaleString("en",{minimumFractionDigits:2,maximumFractionDigits:2})}` : "—"}
              </span>
            </button>
          )
        })}
      </div>

      {/* Main layout */}
      <div style={s.main}>
        {/* Left: Chart */}
        <section style={s.chartSection}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
            <div style={{ display: "flex", alignItems: "baseline", gap: 12 }}>
              <span style={{ fontSize: 20, fontWeight: 700, color: "#f1f5f9" }}>{symbol}</span>
              <span style={{ fontSize: 28, fontWeight: 700, color: "#f8fafc" }}>
                ${currentPrice.toLocaleString("en", { minimumFractionDigits: 2 })}
              </span>
              <span style={{ fontSize: 13, fontWeight: 600, padding: "2px 8px", borderRadius: 4,
                background: isUp ? "rgba(34,197,94,0.12)" : "rgba(239,68,68,0.12)",
                color: isUp ? "#22c55e" : "#ef4444" }}>
                {isUp ? "▲" : "▼"} {Math.abs(pct).toFixed(2)}%
              </span>
            </div>
            {/* Interval tabs - Cập nhật dùng timeframe */}
            <div style={{ display: "flex", gap: 4 }}>
              {INTERVALS.map(iv => (
                <button key={iv} onClick={() => setTimeframe(iv)}
                  style={{ ...s.ivBtn, ...(timeframe === iv ? s.ivActive : {}) }}>
                  {iv}
                </button>
              ))}
            </div>
          </div>

          <CandleChart candles={candles} liveCandle={liveCandle} />
        </section>

        {/* Right: Orderbook + Trades */}
        <aside style={s.aside}>
          <OrderBook data={orderbook} />
          <TradeList trades={trades} />
        </aside>
      </div>

      <section style={{ padding: "0 16px 16px" }}>
        <MLPredictions predictions={predictions} />
      </section>
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  root:        { minHeight: "100vh", display: "flex", flexDirection: "column", background: "#080c14", color: "#e2e8f0" },
  header:      { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 20px", background: "#0d1117", borderBottom: "1px solid #1e3a5f" },
  brand:       { display: "flex", alignItems: "center", gap: 10 },
  brandName:   { fontSize: 18, fontWeight: 700, color: "#93c5fd" },
  brandTag:    { fontSize: 10, background: "#1e3a5f", color: "#60a5fa", padding: "2px 8px", borderRadius: 4 },
  tickerBar:   { display: "flex", overflowX: "auto", background: "#080c14", borderBottom: "1px solid #1e3a5f", scrollbarWidth: "none" },
  tickerBtn:   { display: "flex", flexDirection: "column", alignItems: "flex-start", padding: "8px 16px", border: "none", background: "transparent", cursor: "pointer", borderRight: "1px solid #0f1923", minWidth: 110, gap: 2, color: "#e2e8f0" },
  tickerActive:{ background: "#0d1520", borderTop: "2px solid #3b82f6" },
  main:        { display: "grid", gridTemplateColumns: "1fr 300px", flex: 1, gap: 1, background: "#0f1923" },
  chartSection:{ background: "#0a0f1a", padding: 16, display: "flex", flexDirection: "column", gap: 12 },
  aside:       { background: "#080c14", display: "flex", flexDirection: "column", gap: 1, overflowY: "auto" },
  ivBtn:       { padding: "5px 12px", border: "1px solid #1e3a5f", background: "transparent", color: "#64748b", cursor: "pointer", borderRadius: 4, fontSize: 12, fontFamily: "inherit" },
  ivActive:    { background: "#1e3a5f", color: "#60a5fa", borderColor: "#3b82f6" },
}