"use client"

interface Prediction {
  symbol: string
  current_price?: number
  predicted_price?: number
  predicted_change?: number
  direction?: string
  confidence_score?: number
  prediction_time?: string
}

interface Props { predictions: Prediction[] }

export default function MLPredictions({ predictions }: Props) {
  const loading = !predictions

  return (
    <div className="card" style={s.box}>
      {/* Header */}
      <div className="card-title">
        <span>🤖</span>
        <span>ML Price Predictions</span>
        <span style={{ fontSize: 9, color: "var(--text-dim)", marginLeft: 4 }}>Spark MLlib · Linear Regression</span>
        <div style={{ marginLeft: "auto", display: "flex", gap: 6, alignItems: "center" }}>
          {predictions?.length > 0 && (
            <span style={{ fontSize: 10, color: "var(--text-muted)" }}>
              {predictions.length} models active
            </span>
          )}
        </div>
      </div>

      <div style={{ overflowX: "auto" }}>
        <table style={s.table}>
          <thead>
            <tr style={{ background: "var(--bg-tertiary)" }}>
              {["Symbol", "Current", "Predicted", "Change", "Signal", "Confidence"].map(h => (
                <th key={h} style={s.th}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {/* Loading skeleton */}
            {loading && Array.from({ length: 4 }).map((_, i) => (
              <tr key={i}>
                {Array.from({ length: 6 }).map((_, j) => (
                  <td key={j} style={s.td}>
                    <span className="skeleton" style={{ display: "inline-block", width: j === 5 ? 80 : 60, height: 12 }} />
                  </td>
                ))}
              </tr>
            ))}

            {/* Data rows */}
            {(predictions || []).map((p, i) => {
              const isUp       = p.direction === "UP"
              const confidence = Number(p.confidence_score || 0)
              const confPct    = Math.min(confidence * 100, 100)
              const confColor  = confPct >= 70 ? "var(--green-up)" : confPct >= 45 ? "var(--yellow)" : "var(--red-down)"

              return (
                <tr key={i} className="fade-in" style={{ ...s.row, animationDelay: `${i * 40}ms` }}>
                  {/* Symbol */}
                  <td style={s.td}>
                    <span style={{ fontWeight: 600, color: "var(--text-primary)" }}>
                      {p.symbol?.replace("USDT", "")}
                    </span>
                    <span style={{ color: "var(--text-dim)", fontSize: 10 }}>/USDT</span>
                  </td>

                  {/* Current */}
                  <td style={s.td}>
                    ${Number(p.current_price || 0).toLocaleString("en", { minimumFractionDigits: 2 })}
                  </td>

                  {/* Predicted */}
                  <td style={{ ...s.td, color: isUp ? "var(--green-up)" : "var(--red-down)", fontWeight: 600 }}>
                    ${Number(p.predicted_price || 0).toLocaleString("en", { minimumFractionDigits: 2 })}
                  </td>

                  {/* Change % */}
                  <td style={{ ...s.td, color: isUp ? "var(--green-up)" : "var(--red-down)" }}>
                    {isUp ? "▲" : "▼"} {Math.abs(Number(p.predicted_change || 0)).toFixed(2)}%
                  </td>

                  {/* Signal badge */}
                  <td style={s.td}>
                    <span style={{
                      padding: "2px 8px", borderRadius: 4, fontSize: 10, fontWeight: 700,
                      background: isUp ? "var(--green-dim)" : "var(--red-dim)",
                      color: isUp ? "var(--green-up)" : "var(--red-down)",
                      letterSpacing: "0.05em",
                    }}>
                      {p.direction || "—"}
                    </span>
                  </td>

                  {/* Confidence bar */}
                  <td style={{ ...s.td, minWidth: 90 }}>
                    <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
                      <span style={{ fontSize: 11, color: confColor, fontWeight: 600 }}>
                        {confPct.toFixed(0)}%
                      </span>
                      <div className="conf-bar-track">
                        <div
                          className="conf-bar-fill"
                          style={{ width: `${confPct}%`, background: confColor }}
                        />
                      </div>
                    </div>
                  </td>
                </tr>
              )
            })}

            {/* Empty state */}
            {!loading && !predictions?.length && (
              <tr>
                <td colSpan={6} style={{ ...s.td, textAlign: "center", padding: "24px 12px", color: "var(--text-muted)" }}>
                  <div style={{ fontSize: 20, marginBottom: 8 }}>⚙️</div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>Chưa có dữ liệu dự đoán</div>
                  <div style={{ fontSize: 10 }}>Chạy Spark MLlib batch job để tạo predictions</div>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  box:   { background: "var(--bg-card)" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: 12 },
  th:    { padding: "8px 12px", textAlign: "left", fontSize: 10, color: "var(--text-muted)", fontWeight: 600, letterSpacing: "0.06em", textTransform: "uppercase", whiteSpace: "nowrap" },
  td:    { padding: "10px 12px", color: "var(--text-secondary)", borderBottom: "1px solid var(--border-subtle)" },
  row:   { transition: "background 0.15s" },
}
