"use client"
import { useEffect, useState } from "react"
import { fetchTopGainers } from "@/lib/api"

interface CoinRanking {
  symbol: string
  price: number
  percent_change: number
  volume_24h?: number
}

interface Props {
  onSelectSymbol?: (symbol: string) => void
  activeSymbol?: string
}

export default function TopGainers({ onSelectSymbol, activeSymbol }: Props) {
  const [gainers, setGainers] = useState<CoinRanking[]>([])
  const [losers, setLosers] = useState<CoinRanking[]>([])
  const [loading, setLoading] = useState(true)
  const [tab, setTab] = useState<"gainers" | "losers">("gainers")
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null)
  const [hasData, setHasData] = useState(false)

  const load = async () => {
    try {
      const data = await fetchTopGainers("gainers", 100)
      if (data?.rankings) {
        setHasData(data.rankings.length > 0)
        const sorted = [...data.rankings].sort(
          (a: CoinRanking, b: CoinRanking) => b.percent_change - a.percent_change
        )
        setGainers(sorted.filter((r: CoinRanking) => r.percent_change > 0).slice(0, 8))
        setLosers([...sorted].reverse().filter((r: CoinRanking) => r.percent_change < 0).slice(0, 8))
        setLastUpdate(new Date())
      }
    } catch { 
      setHasData(false)
    }
    setLoading(false)
  }

  useEffect(() => {
    load()
    const id = setInterval(load, 30000)
    return () => clearInterval(id)
  }, [])

  const list = tab === "gainers" ? gainers : losers

  return (
    <div className="card" style={s.box}>
      {/* Header */}
      <div style={s.header}>
        <div style={s.tabs}>
          <button
            id="tab-gainers"
            onClick={() => setTab("gainers")}
            style={{ ...s.tab, ...(tab === "gainers" ? s.tabActiveGreen : {}) }}
          >
            Gainers
          </button>
          <button
            id="tab-losers"
            onClick={() => setTab("losers")}
            style={{ ...s.tab, ...(tab === "losers" ? s.tabActiveRed : {}) }}
          >
            Losers
          </button>
        </div>
        {lastUpdate && (
          <span style={s.updated}>
            Updated {lastUpdate.toLocaleTimeString()}
          </span>
        )}
      </div>

      {/* Body */}
      <div style={{ padding: "4px 0" }}>
        {/* Column headers */}
        <div style={s.colHeader}>
          <span>Symbol</span>
          <span style={{ textAlign: "right" }}>Price</span>
          <span style={{ textAlign: "right" }}>Change</span>
        </div>

        {loading && Array.from({ length: 5 }).map((_, i) => (
          <div key={i} style={{ ...s.row, gap: 8, padding: "7px 12px" }}>
            <span className="skeleton" style={{ width: 48, height: 12, display: "inline-block" }} />
            <span className="skeleton" style={{ width: 70, height: 12, display: "inline-block", marginLeft: "auto" }} />
            <span className="skeleton" style={{ width: 50, height: 12, display: "inline-block" }} />
          </div>
        ))}

        {!hasData && !loading && (
          <div style={{ padding: "20px 12px", textAlign: "center", color: "var(--text-muted)", fontSize: 11 }}>
            <div style={{ fontSize: 20, marginBottom: 6 }}></div>
            Spark Streaming job chưa chạy<br />
            <span style={{ fontSize: 10 }}>Ranking data sẽ hiện khi job active</span>
          </div>
        )}

        {hasData && !loading && list.length === 0 && (
          <div style={{ padding: "20px 12px", textAlign: "center", color: "var(--text-muted)", fontSize: 11 }}>
            Chưa có coin nào {tab === "gainers" ? "tăng" : "giảm"} giá
          </div>
        )}

        {!loading && list.map((item, i) => {
          const isUp = item.percent_change >= 0
          const isActive = item.symbol === activeSymbol
          const barWidth = Math.min(Math.abs(item.percent_change) * 8, 100)

          return (
            <div
              key={item.symbol}
              id={`ranking-${item.symbol}`}
              onClick={() => onSelectSymbol?.(item.symbol)}
              className="fade-in"
              style={{
                ...s.row,
                ...(isActive ? s.rowActive : {}),
                cursor: onSelectSymbol ? "pointer" : "default",
                animationDelay: `${i * 40}ms`,
                position: "relative",
              }}
            >
              {/* Background depth bar */}
              <div style={{
                position: "absolute", left: 0, top: 0, height: "100%",
                width: `${barWidth}%`,
                background: isUp ? "var(--green-bar)" : "var(--red-bar)",
                opacity: 0.25,
                pointerEvents: "none",
              }} />

              <div style={{ display: "flex", alignItems: "center", gap: 6, zIndex: 1 }}>
                <span style={{ ...s.rank, color: i < 3 ? "var(--yellow)" : "var(--text-dim)" }}>
                  {i + 1}
                </span>
                <span style={s.symName}>
                  {item.symbol.replace("USDT", "")}
                  <span style={{ color: "var(--text-dim)", fontSize: 9 }}>/USDT</span>
                </span>
              </div>

              <span style={{ ...s.priceText, zIndex: 1 }}>
                ${Number(item.price).toLocaleString("en", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
              </span>

              <span style={{ ...s.changeBadge, background: isUp ? "var(--green-dim)" : "var(--red-dim)", color: isUp ? "var(--green-up)" : "var(--red-down)", zIndex: 1 }}>
                {isUp ? "▲" : "▼"} {Math.abs(item.percent_change).toFixed(2)}%
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  box: { display: "flex", flexDirection: "column", background: "var(--bg-card)" },
  header: {
    padding: "8px 12px",
    borderBottom: "1px solid var(--border-accent)",
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 8,
  },
  tabs: { display: "flex", gap: 4 },
  tab: {
    padding: "4px 10px",
    border: "1px solid var(--border-accent)",
    borderRadius: 4,
    background: "transparent",
    color: "var(--text-secondary)",
    fontSize: 11,
    fontWeight: 600,
    cursor: "pointer",
    fontFamily: "inherit",
    transition: "all 0.15s",
  },
  tabActiveGreen: {
    background: "var(--green-dim)",
    borderColor: "var(--green-up)",
    color: "var(--green-up)",
  },
  tabActiveRed: {
    background: "var(--red-dim)",
    borderColor: "var(--red-down)",
    color: "var(--red-down)",
  },
  updated: { fontSize: 10, color: "var(--text-dim)" },
  colHeader: {
    display: "grid",
    gridTemplateColumns: "1fr 1fr 80px",
    padding: "4px 12px",
    fontSize: 9,
    color: "var(--text-dim)",
    letterSpacing: "0.06em",
    textTransform: "uppercase",
  },
  row: {
    display: "grid",
    gridTemplateColumns: "1fr 1fr 80px",
    alignItems: "center",
    padding: "6px 12px",
    borderRadius: 4,
    margin: "1px 4px",
    transition: "background 0.15s",
    overflow: "hidden",
  },
  rowActive: { outline: "1px solid var(--accent-blue)" },
  rank: { fontSize: 10, fontWeight: 700, width: 14 },
  symName: { fontSize: 12, fontWeight: 600, color: "var(--text-primary)" },
  priceText: { fontSize: 11, color: "var(--text-secondary)", textAlign: "right" },
  changeBadge: {
    fontSize: 10,
    fontWeight: 700,
    padding: "2px 6px",
    borderRadius: 4,
    textAlign: "right",
    justifySelf: "end",
  },
}
