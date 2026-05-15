"use client"

export default function OrderBook({ data }: { data: any }) {
  if (!data) return (
    <div style={styles.box}>
      <div style={styles.title}>Order Book</div>
      <div style={{ color: "#475569", textAlign: "center", padding: 20 }}>Loading...</div>
    </div>
  )

  const bids = (data.bids || []).slice(0, 15)
  const asks = (data.asks || []).slice(0, 15)
  const maxVol = Math.max(
    ...bids.map((b: any) => b.quantity || b[1] || 0),
    ...asks.map((a: any) => a.quantity || a[1] || 0),
  )

  const getPrice = (r: any) => r.price ?? r[0] ?? 0
  const getQty   = (r: any) => r.quantity ?? r[1] ?? 0

  return (
    <div style={styles.box}>
      <div style={styles.title}>Order Book</div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", fontSize: 10, color: "#475569", padding: "4px 8px" }}>
        <span>Price</span><span style={{ textAlign: "right" }}>Size</span>
      </div>

      {/* Asks (reversed) */}
      {[...asks].reverse().map((row: any, i: number) => (
        <div key={i} style={{ ...styles.row, position: "relative" }}>
          <div style={{ ...styles.bar, background: "rgba(239,68,68,0.15)", width: `${(getQty(row)/maxVol)*100}%`, right: 0 }} />
          <span style={{ color: "#ef4444", fontSize: 11 }}>{Number(getPrice(row)).toFixed(2)}</span>
          <span style={{ fontSize: 11, textAlign: "right" }}>{Number(getQty(row)).toFixed(4)}</span>
        </div>
      ))}

      {/* Spread */}
      <div style={{ textAlign: "center", padding: "4px", fontSize: 10, color: "#64748b", borderTop: "1px solid #1e3a5f", borderBottom: "1px solid #1e3a5f" }}>
        Spread: {data.spread ? Number(data.spread).toFixed(2) : "—"}
      </div>

      {/* Bids */}
      {bids.map((row: any, i: number) => (
        <div key={i} style={{ ...styles.row, position: "relative" }}>
          <div style={{ ...styles.bar, background: "rgba(34,197,94,0.15)", width: `${(getQty(row)/maxVol)*100}%`, right: 0 }} />
          <span style={{ color: "#22c55e", fontSize: 11 }}>{Number(getPrice(row)).toFixed(2)}</span>
          <span style={{ fontSize: 11, textAlign: "right" }}>{Number(getQty(row)).toFixed(4)}</span>
        </div>
      ))}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  box:   { background: "#0a0e1a", border: "1px solid #1e3a5f", borderRadius: 8, overflow: "hidden" },
  title: { padding: "10px 12px", fontSize: 13, fontWeight: 600, color: "#93c5fd", borderBottom: "1px solid #1e3a5f" },
  row:   { display: "grid", gridTemplateColumns: "1fr 1fr", padding: "2px 8px", overflow: "hidden" },
  bar:   { position: "absolute", top: 0, height: "100%", opacity: 0.8 },
}
