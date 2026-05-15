"use client"

export default function MLPredictions({ predictions }: { predictions: any[] }) {
  return (
    <div style={styles.box}>
      <div style={styles.title}>🤖 ML Predictions <span style={{ fontSize: 10, color: "#475569" }}>Spark MLlib</span></div>
      <div style={{ overflowX: "auto" }}>
        <table style={styles.table}>
          <thead>
            <tr style={{ background: "#0d1520" }}>
              {["Symbol", "Current", "Predicted", "Change", "Signal", "Confidence"].map(h => (
                <th key={h} style={styles.th}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {(predictions || []).map((p: any, i: number) => {
              const isUp = p.direction === "UP"
              return (
                <tr key={i} style={{ borderBottom: "1px solid #0f1923" }}>
                  <td style={styles.td}>{p.symbol?.replace("USDT","")}<span style={{ color: "#475569", fontSize: 10 }}>/USDT</span></td>
                  <td style={styles.td}>${Number(p.current_price||0).toLocaleString("en", { minimumFractionDigits: 2 })}</td>
                  <td style={styles.td}>${Number(p.predicted_price||0).toLocaleString("en", { minimumFractionDigits: 2 })}</td>
                  <td style={{ ...styles.td, color: isUp ? "#22c55e" : "#ef4444" }}>
                    {isUp ? "▲" : "▼"} {Math.abs(Number(p.predicted_change||0)).toFixed(2)}%
                  </td>
                  <td style={styles.td}>
                    <span style={{
                      padding: "2px 8px", borderRadius: 4, fontSize: 10, fontWeight: 600,
                      background: isUp ? "rgba(34,197,94,0.15)" : "rgba(239,68,68,0.15)",
                      color: isUp ? "#22c55e" : "#ef4444",
                    }}>{p.direction}</span>
                  </td>
                  <td style={{ ...styles.td, color: "#64748b" }}>
                    {(Number(p.confidence_score||0)*100).toFixed(0)}%
                  </td>
                </tr>
              )
            })}
            {!predictions?.length && (
              <tr><td colSpan={6} style={{ ...styles.td, textAlign: "center", color: "#475569" }}>
                Chạy ML training trước để có predictions
              </td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  box:   { background: "#0a0e1a", border: "1px solid #1e3a5f", borderRadius: 8, overflow: "hidden" },
  title: { padding: "10px 12px", fontSize: 13, fontWeight: 600, color: "#93c5fd", borderBottom: "1px solid #1e3a5f", display: "flex", gap: 8, alignItems: "center" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: 12 },
  th:    { padding: "6px 12px", textAlign: "left", fontSize: 10, color: "#475569", fontWeight: 500 },
  td:    { padding: "8px 12px", color: "#e2e8f0" },
}
