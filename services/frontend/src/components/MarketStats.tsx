"use client"
import { useEffect, useState } from "react"
import { fetchTopGainers } from "@/lib/api"

interface Stats {
  gainersCount: number
  losersCount: number
  totalCoins: number
  sentimentScore: number // 0-100, 50 = neutral
}

interface Props {
  symbolCount: number
  wsConnected: boolean
}

export default function MarketStats({ symbolCount, wsConnected }: Props) {
  const [stats, setStats] = useState<Stats | null>(null)
  const [uptime, setUptime] = useState(0) // seconds
  const [loading, setLoading] = useState(true)

  // Uptime counter
  useEffect(() => {
    const id = setInterval(() => setUptime(s => s + 1), 1000)
    return () => clearInterval(id)
  }, [])

  // Fetch market sentiment from ranking
  useEffect(() => {
    const load = async () => {
      try {
        const data = await fetchTopGainers("gainers", 1000)
        if (data?.rankings) {
          const gainers = data.rankings.filter((r: any) => r.percent_change > 0).length
          const losers  = data.rankings.filter((r: any) => r.percent_change < 0).length
          const total   = data.rankings.length
          const score   = total > 0 ? Math.round((gainers / total) * 100) : 50
          setStats({ gainersCount: gainers, losersCount: losers, totalCoins: total, sentimentScore: score })
        }
      } catch {}
      setLoading(false)
    }
    load()
    const id = setInterval(load, 60000)
    return () => clearInterval(id)
  }, [])

  const formatUptime = (s: number) => {
    const h = Math.floor(s / 3600)
    const m = Math.floor((s % 3600) / 60)
    const sec = s % 60
    return h > 0
      ? `${h}h ${m.toString().padStart(2,"0")}m`
      : `${m}m ${sec.toString().padStart(2,"0")}s`
  }

  const sentiment = stats?.sentimentScore ?? 50
  const sentimentLabel = sentiment >= 70 ? "Greed" : sentiment >= 55 ? "Bullish" : sentiment >= 45 ? "Neutral" : sentiment >= 30 ? "Bearish" : "Fear"
  const sentimentColor = sentiment >= 70 ? "var(--green-up)" : sentiment >= 55 ? "#86efac" : sentiment >= 45 ? "var(--yellow)" : sentiment >= 30 ? "#fca5a5" : "var(--red-down)"

  const cards = [
    {
      id: "stat-sentiment",
      icon: "🌡️",
      label: "Market Sentiment",
      value: loading ? null : sentimentLabel,
      sub: loading ? null : `${stats?.gainersCount ?? 0}↑ ${stats?.losersCount ?? 0}↓`,
      color: sentimentColor,
      extra: !loading && (
        <div style={{ marginTop: 6 }}>
          <div style={{ height: 4, background: "var(--bg-hover)", borderRadius: 2, overflow: "hidden" }}>
            <div style={{
              height: "100%",
              width: `${sentiment}%`,
              background: `linear-gradient(90deg, var(--red-down), var(--yellow), var(--green-up))`,
              borderRadius: 2,
              transition: "width 0.8s ease"
            }} />
          </div>
        </div>
      )
    },
    {
      id: "stat-symbols",
      icon: "📡",
      label: "Tracked Symbols",
      value: symbolCount.toString(),
      sub: "Binance USDT pairs",
      color: "var(--accent-blue)",
      extra: null
    },
    {
      id: "stat-ws",
      icon: "⚡",
      label: "WebSocket",
      value: wsConnected ? "Connected" : "Connecting",
      sub: wsConnected ? "Live data stream" : "Reconnecting...",
      color: wsConnected ? "var(--green-up)" : "var(--yellow)",
      extra: (
        <span className={`live-dot ${wsConnected ? "green" : "red"}`} style={{ marginTop: 6 }} />
      )
    },
    {
      id: "stat-uptime",
      icon: "🕒",
      label: "Session Uptime",
      value: formatUptime(uptime),
      sub: "Lambda pipeline active",
      color: "var(--purple)",
      extra: null
    },
  ]

  return (
    <div style={s.grid}>
      {cards.map(card => (
        <div key={card.id} id={card.id} className="card fade-in" style={s.card}>
          <div style={s.iconRow}>
            <span style={s.icon}>{card.icon}</span>
            <span style={s.label}>{card.label}</span>
          </div>
          <div style={{ ...s.value, color: card.color }}>
            {card.value == null
              ? <span className="skeleton" style={{ display: "inline-block", width: 80, height: 18 }} />
              : card.value
            }
          </div>
          <div style={s.sub}>
            {card.sub == null
              ? <span className="skeleton" style={{ display: "inline-block", width: 100, height: 10 }} />
              : card.sub
            }
          </div>
          {card.extra}
        </div>
      ))}
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  grid: {
    display: "grid",
    gridTemplateColumns: "repeat(4, 1fr)",
    gap: 8,
    padding: "12px 16px",
    background: "var(--bg-base)",
    borderBottom: "1px solid var(--border-accent)",
  },
  card: {
    padding: "12px 14px",
    display: "flex",
    flexDirection: "column",
    gap: 4,
    background: "var(--bg-card)",
    transition: "border-color 0.2s",
  },
  iconRow: {
    display: "flex",
    alignItems: "center",
    gap: 6,
    marginBottom: 2,
  },
  icon: { fontSize: 13 },
  label: { fontSize: 10, color: "var(--text-muted)", letterSpacing: "0.06em", textTransform: "uppercase" },
  value: { fontSize: 18, fontWeight: 700, letterSpacing: "-0.01em" },
  sub:   { fontSize: 10, color: "var(--text-muted)" },
}
