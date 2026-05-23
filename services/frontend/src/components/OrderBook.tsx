"use client"

interface OrderBookEntry {
  price: number
  quantity: number
  total?: number
}

interface Props {
  bids: OrderBookEntry[]
  asks: OrderBookEntry[]
  spread: number | null
  connected: boolean
}

export default function OrderBook({ bids: rawBids, asks: rawAsk, spread, connected }: Props) {
  const bids = (rawBids || []).slice(0, 15)
  const asks = (rawAsk || []).slice(0, 15)

  const maxVol = Math.max(
    ...bids.map(b => b.quantity || 0),
    ...asks.map(a => a.quantity || 0),
    0.0001
  )

  // Bid/Ask imbalance
  const totalBidQty = bids.reduce((s, b) => s + (b.quantity || 0), 0)
  const totalAskQty = asks.reduce((s, a) => s + (a.quantity || 0), 0)
  const totalQty = totalBidQty + totalAskQty
  const bidPct = totalQty > 0 ? (totalBidQty / totalQty) * 100 : 50

  const isLoading = bids.length === 0 && asks.length === 0

  return (
    <div className="card" style={s.box}>
      {/* Header */}
      <div className="card-title" style={{ justifyContent: "space-between" }}>
        <span>Order Book</span>
        <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
          <span className={`live-dot ${connected ? "blue" : "red"}`} />
          <span style={{ fontSize: 9, color: connected ? "var(--accent-blue)" : "var(--red-down)", fontWeight: 400 }}>
            {connected ? "WS" : "Offline"}
          </span>
        </div>
      </div>

      {/* Bid/Ask imbalance bar */}
      {!isLoading && (
        <div style={s.imbalanceRow}>
          <span style={{ color: "var(--green-up)", fontSize: 10 }}>
            {bidPct.toFixed(0)}% B
          </span>
          <div style={s.imbalanceTrack}>
            <div style={{ ...s.imbalanceBid, width: `${bidPct}%` }} />
          </div>
          <span style={{ color: "var(--red-down)", fontSize: 10 }}>
            {(100 - bidPct).toFixed(0)}% A
          </span>
        </div>
      )}

      {/* Column headers */}
      <div style={s.colHeader}>
        <span>Price</span>
        <span style={{ textAlign: "right" }}>Size</span>
        <span style={{ textAlign: "right" }}>Total</span>
      </div>

      {/* Loading skeleton */}
      {isLoading && Array.from({ length: 8 }).map((_, i) => (
        <div key={i} style={{ ...s.row, gap: 8, padding: "4px 10px" }}>
          <span className="skeleton" style={{ width: 70, height: 11, display: "inline-block" }} />
          <span className="skeleton" style={{ width: 55, height: 11, display: "inline-block", marginLeft: "auto" }} />
          <span className="skeleton" style={{ width: 50, height: 11, display: "inline-block" }} />
        </div>
      ))}

      {!isLoading && (
        <>
          {/* Asks (reversed → highest ask at top) */}
          {[...asks].reverse().map((row, i) => (
            <div key={`ask-${i}`} style={{ ...s.row, position: "relative" }}>
              <div style={{
                ...s.depthBar,
                background: "var(--red-bar)",
                width: `${(row.quantity / maxVol) * 100}%`,
                right: 0
              }} />
              <span style={{ color: "var(--red-down)", fontSize: 11, zIndex: 1, position: "relative" }}>
                {Number(row.price).toFixed(2)}
              </span>
              <span style={{ fontSize: 11, textAlign: "right", zIndex: 1, position: "relative", color: "var(--text-secondary)" }}>
                {Number(row.quantity).toFixed(4)}
              </span>
              <span style={{ fontSize: 10, textAlign: "right", zIndex: 1, position: "relative", color: "var(--text-dim)" }}>
                {row.total ? Number(row.total).toFixed(0) : "—"}
              </span>
            </div>
          ))}

          {/* Spread */}
          <div style={s.spreadRow}>
            Spread: {spread != null ? `$${Number(spread).toFixed(2)}` : "—"}
          </div>

          {/* Bids */}
          {bids.map((row, i) => (
            <div key={`bid-${i}`} style={{ ...s.row, position: "relative" }}>
              <div style={{
                ...s.depthBar,
                background: "var(--green-bar)",
                width: `${(row.quantity / maxVol) * 100}%`,
                right: 0
              }} />
              <span style={{ color: "var(--green-up)", fontSize: 11, zIndex: 1, position: "relative" }}>
                {Number(row.price).toFixed(2)}
              </span>
              <span style={{ fontSize: 11, textAlign: "right", zIndex: 1, position: "relative", color: "var(--text-secondary)" }}>
                {Number(row.quantity).toFixed(4)}
              </span>
              <span style={{ fontSize: 10, textAlign: "right", zIndex: 1, position: "relative", color: "var(--text-dim)" }}>
                {row.total ? Number(row.total).toFixed(0) : "—"}
              </span>
            </div>
          ))}
        </>
      )}
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  box: { background: "var(--bg-card)", display: "flex", flexDirection: "column" },
  imbalanceRow: {
    display: "flex", alignItems: "center", gap: 6,
    padding: "5px 10px", borderBottom: "1px solid var(--border-subtle)",
  },
  imbalanceTrack: {
    flex: 1, height: 5, borderRadius: 3,
    background: "var(--red-bar)", overflow: "hidden", position: "relative",
  },
  imbalanceBid: {
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
    padding: "2px 10px", overflow: "hidden",
  },
  depthBar: { position: "absolute", top: 0, height: "100%", opacity: 0.7 },
  spreadRow: {
    textAlign: "center", padding: "5px 8px", fontSize: 10,
    color: "var(--text-muted)",
    borderTop: "1px solid var(--border-accent)",
    borderBottom: "1px solid var(--border-accent)",
    background: "var(--bg-tertiary)",
  },
}
