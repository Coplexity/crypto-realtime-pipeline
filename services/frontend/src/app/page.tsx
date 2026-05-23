"use client"
import { useState, useEffect, useCallback } from "react"
import dynamic from "next/dynamic"
import OrderBook       from "@/components/OrderBook"
import TradeList       from "@/components/TradeList"
import MLPredictions   from "@/components/MLPredictions"
import TickerBar       from "@/components/TickerBar"
import MarketStats     from "@/components/MarketStats"
import TopGainers      from "@/components/TopGainers"
import PipelineStatus  from "@/components/PipelineStatus"
import { useWebSocket }    from "@/hooks/useWebSocket"
import { useOrderBookWS }  from "@/hooks/useOrderBookWS"
import { useTradesWS }     from "@/hooks/useTradesWS"
import { fetchOHLC, fetchPredictions, fetchSymbols } from "@/lib/api"

const CandleChart = dynamic(() => import("@/components/CandleChart"), { ssr: false })

const INTERVALS      = ["1m", "5m", "1h", "4h", "1d"]
const DEFAULT_SYMBOLS = ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","ADAUSDT","XRPUSDT","DOGEUSDT","AVAXUSDT"]

export default function Dashboard() {
  const [symbols,      setSymbols]      = useState<string[]>(DEFAULT_SYMBOLS)
  const [symbol,       setSymbol]       = useState("BTCUSDT")
  const [timeframe,    setTimeframe]    = useState("5m")
  const [candles,      setCandles]      = useState<any[]>([])
  const [liveCandle,   setLiveCandle]   = useState<any>(null)
  const [predictions,  setPredictions]  = useState<any[]>([])
  const [currentPrice, setCurrentPrice] = useState(0)
  const [prevPrice,    setPrevPrice]    = useState(0)
  const [backendError, setBackendError] = useState<string | null>(null)

  // ── WebSocket connections ────────────────────────────────────────────────
  // 1. Kline (candlestick) WS
  const { connected: wsKlineConnected } = useWebSocket(
    symbol, timeframe,
    useCallback((msg: any) => {
      if (msg.candle) {
        setLiveCandle(msg.candle)
        setCurrentPrice(prev => { setPrevPrice(prev || msg.candle.close); return msg.candle.close })
      }
    }, [])
  )

  // 2. OrderBook WS — replaces HTTP polling
  const { bids, asks, spread, connected: wsObConnected } = useOrderBookWS(symbol)

  // 3. Trades WS — replaces HTTP polling, streams from Kafka
  const { trades, connected: wsTradesConnected } = useTradesWS(symbol)

  // ── REST: symbols ────────────────────────────────────────────────────────
  useEffect(() => {
    fetchSymbols()
      .then(d => setSymbols(d?.symbols || DEFAULT_SYMBOLS))
      .catch(() => {})
  }, [])

  // ── REST: OHLC candles ───────────────────────────────────────────────────
  useEffect(() => {
    setCandles([])
    setBackendError(null)
    fetchOHLC(symbol, timeframe, 200)
      .then(d => {
        const cs = d?.candles || []
        setCandles(cs)
        if (cs.length > 0) {
          setCurrentPrice(cs.at(-1)?.close || 0)
          setPrevPrice(cs.at(-2)?.close || 0)
        }
      })
      .catch(err => {
        setBackendError("Không thể kết nối Backend — kiểm tra docker compose")
        console.error("OHLC fetch error:", err)
      })
  }, [symbol, timeframe])

  // ── REST: ML predictions ─────────────────────────────────────────────────
  useEffect(() => {
    const load = () =>
      fetchPredictions().then(d => setPredictions(d?.predictions || [])).catch(() => {})
    load()
    const id = setInterval(load, 30000)
    return () => clearInterval(id)
  }, [])

  const isUp = currentPrice >= prevPrice
  const pct  = prevPrice > 0 ? ((currentPrice - prevPrice) / prevPrice * 100) : 0

  return (
    <div style={s.root}>

      {/* ── Error Banner ──────────────────────────────────────────────────── */}
      {backendError && (
        <div id="error-banner" style={s.errorBanner}>
          ⚠️ {backendError}
          <button onClick={() => setBackendError(null)} style={s.errorClose}>✕</button>
        </div>
      )}

      {/* ── Header ───────────────────────────────────────────────────────── */}
      <header style={s.header}>
        <div style={s.brand}>
          <span style={{ fontSize: 22, lineHeight: 1 }}>⬡</span>
          <div>
            <div style={s.brandName}>CryptoAnalytics</div>
            <div style={s.brandSub}>Lambda Architecture · Kafka · Spark · MongoDB</div>
          </div>
          <span style={s.brandBadge}>LIVE</span>
        </div>

        <div style={s.headerRight}>
          <div style={s.priceBlock}>
            <span style={{ fontSize: 13, color: "var(--text-secondary)" }}>{symbol}</span>
            <span style={{ fontSize: 22, fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.02em" }}>
              ${currentPrice > 0
                ? currentPrice.toLocaleString("en", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
                : "—"}
            </span>
            {currentPrice > 0 && (
              <span style={{
                fontSize: 12, fontWeight: 600, padding: "2px 8px", borderRadius: 4,
                background: isUp ? "var(--green-dim)" : "var(--red-dim)",
                color: isUp ? "var(--green-up)" : "var(--red-down)",
              }}>
                {isUp ? "▲" : "▼"} {Math.abs(pct).toFixed(2)}%
              </span>
            )}
          </div>

          {/* WS health summary */}
          <div style={s.wsGroup}>
            {[
              { label: "K",  ok: wsKlineConnected,  title: "Kline WS"    },
              { label: "OB", ok: wsObConnected,      title: "OrderBook WS"},
              { label: "T",  ok: wsTradesConnected,  title: "Trades WS"  },
            ].map(({ label, ok, title }) => (
              <span key={label} title={title} style={{ ...s.wsChip, background: ok ? "var(--green-dim)" : "var(--red-dim)", color: ok ? "var(--green-up)" : "var(--red-down)" }}>
                <span className={`live-dot ${ok ? "green" : "red"}`} style={{ width: 5, height: 5 }} />
                {label}
              </span>
            ))}
          </div>
        </div>
      </header>

      {/* ── Ticker Bar ───────────────────────────────────────────────────── */}
      <TickerBar symbols={symbols} activeSymbol={symbol} onSelect={setSymbol} />

      {/* ── Market Stats ─────────────────────────────────────────────────── */}
      <MarketStats symbolCount={symbols.length} wsConnected={wsKlineConnected} />

      {/* ── Main 3-column layout ─────────────────────────────────────────── */}
      <div style={s.main}>

        {/* Left: Candlestick Chart */}
        <section style={s.chartSection}>
          <div style={s.chartHeader}>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <span style={s.chartSymbol}>{symbol}</span>
              <div style={s.ivGroup}>
                {INTERVALS.map(iv => (
                  <button
                    key={iv}
                    id={`interval-${iv}`}
                    onClick={() => setTimeframe(iv)}
                    style={{ ...s.ivBtn, ...(timeframe === iv ? s.ivActive : {}) }}
                  >
                    {iv}
                  </button>
                ))}
              </div>
            </div>
            <span style={{ fontSize: 10, color: "var(--text-dim)" }}>
              {candles.length > 0 ? `${candles.length} candles` : "Loading..."}
            </span>
          </div>

          <CandleChart candles={candles} liveCandle={liveCandle} />
        </section>

        {/* Center: Top Gainers / Losers */}
        <aside style={s.gainersAside}>
          <TopGainers onSelectSymbol={setSymbol} activeSymbol={symbol} />
        </aside>

        {/* Right: OrderBook + Trades — now WebSocket powered */}
        <aside style={s.aside}>
          <OrderBook
            bids={bids}
            asks={asks}
            spread={spread}
            connected={wsObConnected}
          />
          <TradeList
            trades={trades}
            connected={wsTradesConnected}
          />
        </aside>
      </div>

      {/* ── ML Predictions ───────────────────────────────────────────────── */}
      <section style={s.predictionsSection}>
        <MLPredictions predictions={predictions} />
      </section>

      {/* ── Pipeline Status (collapsible) ────────────────────────────────── */}
      <PipelineStatus
        wsKlineConnected={wsKlineConnected}
        wsOrderBookConnected={wsObConnected}
        wsTradesConnected={wsTradesConnected}
      />

      {/* ── Footer ───────────────────────────────────────────────────────── */}
      <footer style={s.footer}>
        <span>CryptoAnalytics Dashboard · Lambda Architecture</span>
        <span style={{ color: "var(--text-dim)" }}>
          Kafka · Spark Streaming · Spark MLlib · MongoDB · Redis · FastAPI · Next.js
        </span>
      </footer>
    </div>
  )
}

/* ─── Styles ──────────────────────────────────────────────────────────────── */
const s: Record<string, React.CSSProperties> = {
  root: {
    minHeight: "100vh",
    display: "flex",
    flexDirection: "column",
    background: "var(--bg-base)",
    color: "var(--text-primary)",
  },

  /* Error banner */
  errorBanner: {
    background: "rgba(239,68,68,0.15)",
    borderBottom: "1px solid var(--red-down)",
    color: "var(--red-down)",
    padding: "8px 20px",
    fontSize: 12,
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    fontWeight: 600,
  },
  errorClose: {
    background: "transparent", border: "none", color: "var(--red-down)",
    cursor: "pointer", fontSize: 14, fontFamily: "inherit",
  },

  /* Header */
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "11px 20px",
    background: "var(--bg-primary)",
    borderBottom: "1px solid var(--border-accent)",
    gap: 16,
  },
  brand:     { display: "flex", alignItems: "center", gap: 12 },
  brandName: { fontSize: 16, fontWeight: 700, color: "#93c5fd", letterSpacing: "-0.01em" },
  brandSub:  { fontSize: 9, color: "var(--text-dim)", letterSpacing: "0.04em", marginTop: 1 },
  brandBadge: {
    fontSize: 9, fontWeight: 700, letterSpacing: "0.1em",
    background: "var(--green-dim)", color: "var(--green-up)",
    padding: "2px 7px", borderRadius: 4, border: "1px solid rgba(34,197,94,0.4)",
  },
  headerRight: { display: "flex", alignItems: "center", gap: 16 },
  priceBlock:  { display: "flex", alignItems: "baseline", gap: 10 },
  wsGroup:     { display: "flex", gap: 4 },
  wsChip: {
    display: "flex", alignItems: "center", gap: 4,
    padding: "3px 7px", borderRadius: 4, fontSize: 10, fontWeight: 700,
  },

  /* Main 3-col grid */
  main: {
    display: "grid",
    gridTemplateColumns: "1fr 220px 280px",
    flex: 1,
    gap: 1,
    background: "var(--border-subtle)",
    minHeight: 0,
  },

  chartSection: {
    background: "var(--bg-secondary)",
    padding: 14,
    display: "flex",
    flexDirection: "column",
    gap: 10,
  },
  chartHeader: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
  },
  chartSymbol: { fontSize: 14, fontWeight: 700, color: "var(--text-primary)" },
  ivGroup:     { display: "flex", gap: 3 },
  ivBtn: {
    padding: "4px 10px",
    border: "1px solid var(--border-accent)",
    background: "transparent",
    color: "var(--text-muted)",
    cursor: "pointer",
    borderRadius: 4,
    fontSize: 11,
    fontFamily: "inherit",
    transition: "all 0.15s",
  },
  ivActive: {
    background: "var(--accent-blue-dim)",
    color: "#60a5fa",
    borderColor: "var(--accent-blue)",
  },

  gainersAside: {
    background: "var(--bg-card)",
    overflowY: "auto",
    display: "flex",
    flexDirection: "column",
  },
  aside: {
    background: "var(--bg-primary)",
    display: "flex",
    flexDirection: "column",
    gap: 1,
    overflowY: "auto",
  },

  predictionsSection: {
    padding: "8px 14px 10px",
    background: "var(--bg-base)",
  },

  footer: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    padding: "7px 20px",
    fontSize: 10,
    color: "var(--text-muted)",
    borderTop: "1px solid var(--border-subtle)",
    background: "var(--bg-primary)",
    letterSpacing: "0.04em",
  },
}