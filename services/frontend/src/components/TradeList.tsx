"use client"

export default function TradeList({ trades }: { trades: any[] }) {
  return (
    <div style={styles.box}>
      <div style={styles.title}>Recent Trades</div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", fontSize: 10, color: "#475569", padding: "4px 8px" }}>
        <span>Price</span><span style={{ textAlign: "center" }}>Amount</span><span style={{ textAlign: "right" }}>Time</span>
      </div>
      <div style={{ maxHeight: 300, overflowY: "auto" }}>
        {(trades || []).slice(0, 30).map((t: any, i: number) => {
          const isBuy  = !t.isBuyerMaker
          const price  = Number(t.price).toFixed(2)
          const qty    = Number(t.quantity).toFixed(4)
          const time   = new Date(t.time || t.trade_time || 0).toLocaleTimeString()
          return (
            <div key={i} style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", padding: "2px 8px", fontSize: 11 }}>
              <span style={{ color: isBuy ? "#22c55e" : "#ef4444" }}>{price}</span>
              <span style={{ textAlign: "center", color: "#94a3b8" }}>{qty}</span>
              <span style={{ textAlign: "right", color: "#475569" }}>{time}</span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  box:   { background: "#0a0e1a", border: "1px solid #1e3a5f", borderRadius: 8, overflow: "hidden" },
  title: { padding: "10px 12px", fontSize: 13, fontWeight: 600, color: "#93c5fd", borderBottom: "1px solid #1e3a5f" },
}
