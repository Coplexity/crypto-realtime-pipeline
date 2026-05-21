"use client"
import { useRef, useEffect } from "react"

interface Trade {
  symbol?: string
  price: number
  quantity: number
  time: number
  isBuyerMaker: boolean
  tradeId?: string | number
}

interface Props {
  trades: Trade[]
  connected: boolean
}

export default function TradeList({ trades, connected }: Props) {
  const listRef = useRef<HTMLDivElement>(null)
  const prevLen = useRef(0)

  // Auto-scroll to top when new trade arrives
  useEffect(() => {
    if (trades.length > prevLen.current && listRef.current) {
      listRef.current.scrollTop = 0
    }
    prevLen.current = trades.length
  }, [trades.length])

  // Buy vs sell volume summary
  const buyQty  = trades.filter(t => !t.isBuyerMaker).reduce((s, t) => s + Number(t.quantity), 0)
  const sellQty = trades.filter(t => t.isBuyerMaker).reduce((s, t) => s + Number(t.quantity), 0)
  const totalQty = buyQty + sellQty
  const buyPct   = totalQty > 0 ? (buyQty / totalQty) * 100 : 50

  return (
    <div className="card" style={s.box}>
      {/* Header */}
      <div className="card-title" style={{ justifyContent: "space-between" }}>
        <span>⚡ Trades</span>
        <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
          <span className={`live-dot ${connected ? "green" : "red"}`} />
          <span style={{ fontSize: 9, color: connected ? "var(--green-up)" : "var(--red-down)", fontWeight: 400 }}>
            {connected ? "Kafka" : "Offline"}
          </span>
        </div>
      </div>

      {/* Buy/Sell volume bar */}
      {trades.length > 0 && (
        <div style={s.volRow}>
          <span style={{ color: "var(--green-up)", fontSize: 10 }}>
            B {buyPct.toFixed(0)}%
          </span>
          <div style={s.volTrack}>
            <div style={{ ...s.volBuy, width: `${buyPct}%` }} />
          </div>
          <span style={{ color: "var(--red-down)", fontSize: 10 }}>
            {(100 - buyPct).toFixed(0)}% S
          </span>
        </div>
      )}

      {/* Column headers */}
      <div style={s.colHeader}>
        <span>Price</span>
        <span style={{ textAlign: "center" }}>Qty</span>
        <span style={{ textAlign: "right" }}>Time</span>
      </div>

      {/* Trade rows */}
      <div ref={listRef} style={{ flex: 1, overflowY: "auto", maxHeight: 280 }}>
        {/* Loading skeleton */}
        {trades.length === 0 && (
          Array.from({ length: 10 }).map((_, i) => (
            <div key={i} style={{ ...s.row, gap: 8, padding: "4px 10px" }}>
              <span className="skeleton" style={{ width: 65, height: 11, display: "inline-block" }} />
              <span className="skeleton" style={{ width: 55, height: 11, display: "inline-block", margin: "0 auto" }} />
              <span className="skeleton" style={{ width: 50, height: 11, display: "inline-block", marginLeft: "auto" }} />
            </div>
          ))
        )}

        {trades.slice(0, 50).map((t, i) => {
          const isBuy  = !t.isBuyerMaker
          const price  = Number(t.price).toFixed(2)
          const qty    = Number(t.quantity).toFixed(4)
          const time   = new Date(t.time || 0).toLocaleTimeString("en", { hour12: false })
          const isNew  = i === 0

          return (
            <div
              key={`${t.tradeId ?? i}`}
              className={isNew ? "slide-in-r" : ""}
              style={{
                ...s.row,
                background: isNew
                  ? (isBuy ? "rgba(34,197,94,0.06)" : "rgba(239,68,68,0.06)")
                  : "transparent",
                transition: "background 0.4s ease",
              }}
            >
              <span style={{ color: isBuy ? "var(--green-up)" : "var(--red-down)", fontSize: 11, fontWeight: 600 }}>
                {price}
              </span>
              <span style={{ textAlign: "center", color: "var(--text-secondary)", fontSize: 11 }}>
                {qty}
              </span>
              <span style={{ textAlign: "right", color: "var(--text-dim)", fontSize: 10 }}>
                {time}
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  box: { background: "var(--bg-card)", display: "flex", flexDirection: "column" },
  volRow: {
    display: "flex", alignItems: "center", gap: 6,
    padding: "5px 10px", borderBottom: "1px solid var(--border-subtle)",
  },
  volTrack: {
    flex: 1, height: 5, borderRadius: 3,
    background: "var(--red-bar)", overflow: "hidden",
  },
  volBuy: {
    height: "100%", borderRadius: 3,
    background: "var(--green-bar)", transition: "width 0.5s ease",
  },
  colHeader: {
    display: "grid", gridTemplateColumns: "1fr 1fr 1fr",
    padding: "4px 10px", fontSize: 9,
    color: "var(--text-dim)", letterSpacing: "0.06em", textTransform: "uppercase",
  },
  row: {
    display: "grid", gridTemplateColumns: "1fr 1fr 1fr",
    padding: "3px 10px",
  },
}
