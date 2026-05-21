"use client"
import { useEffect, useState } from "react"

interface ServiceStatus {
  name: string
  icon: string
  label: string
  status: "ok" | "error" | "checking"
  detail: string
  layer: "ingest" | "stream" | "batch" | "serve" | "viz"
}

const LAYER_COLOR: Record<string, string> = {
  ingest: "#f59e0b",
  stream: "#3b82f6",
  batch: "#a855f7",
  serve: "#22c55e",
  viz: "#93c5fd",
}

interface Props {
  wsKlineConnected: boolean
  wsOrderBookConnected: boolean
  wsTradesConnected: boolean
}

export default function PipelineStatus({ wsKlineConnected, wsOrderBookConnected, wsTradesConnected }: Props) {
  const [backendOk, setBackendOk] = useState<boolean | null>(null)
  const [dataOk, setDataOk] = useState<boolean | null>(null)
  const [isCollapsed, setIsCollapsed] = useState(false)

  useEffect(() => {
    const check = async () => {
      const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"
      try {
        const r = await fetch(`${API}/health`, { signal: AbortSignal.timeout(3000) })
        setBackendOk(r.ok)
      } catch { setBackendOk(false) }

      try {
        const r = await fetch(`${API}/symbols`, { signal: AbortSignal.timeout(3000) })
        setDataOk(r.ok)
      } catch { setDataOk(false) }
    }
    check()
    const id = setInterval(check, 15000)
    return () => clearInterval(id)
  }, [])

  const allWsOk = wsKlineConnected && wsOrderBookConnected && wsTradesConnected

  const services: ServiceStatus[] = [
    // Ingest layer
    {
      name: "binance-ws",
      icon: "",
      label: "Binance WS",
      status: dataOk === null ? "checking" : dataOk ? "ok" : "error",
      detail: "Data Ingestion",
      layer: "ingest",
    },
    {
      name: "kafka",
      icon: "",
      label: "Kafka",
      status: allWsOk ? "ok" : dataOk ? "ok" : "checking",
      detail: "Message Queue",
      layer: "stream",
    },
    // Speed layer
    {
      name: "spark-streaming",
      icon: "",
      label: "Spark Streaming",
      status: wsKlineConnected ? "ok" : "checking",
      detail: "Speed Layer",
      layer: "stream",
    },
    {
      name: "redis",
      icon: "",
      label: "Redis",
      status: backendOk === null ? "checking" : backendOk ? "ok" : "error",
      detail: "Hot Cache",
      layer: "serve",
    },
    // Batch layer
    {
      name: "spark-batch",
      icon: "",
      label: "Spark Batch",
      status: backendOk ? "ok" : "checking",
      detail: "Batch Layer",
      layer: "batch",
    },
    {
      name: "mongodb",
      icon: "",
      label: "MongoDB",
      status: backendOk === null ? "checking" : backendOk ? "ok" : "error",
      detail: "Cold Store",
      layer: "serve",
    },
    // Serving
    {
      name: "backend",
      icon: "",
      label: "FastAPI",
      status: backendOk === null ? "checking" : backendOk ? "ok" : "error",
      detail: "Serving Layer",
      layer: "serve",
    },
    // Viz
    {
      name: "websockets",
      icon: "",
      label: "WebSockets",
      status: allWsOk ? "ok" : "error",
      detail: `K:${wsKlineConnected ? 1 : 0} OB:${wsOrderBookConnected ? 1 : 0} T:${wsTradesConnected ? 1 : 0}`,
      layer: "viz",
    },
  ]

  const okCount = services.filter(s => s.status === "ok").length

  return (
    <div style={s.wrapper}>
      {/* Header — collapsible */}
      <button id="pipeline-toggle" onClick={() => setIsCollapsed(c => !c)} style={s.toggle}>
        <span style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontWeight: 600, fontSize: 11, color: "var(--text-secondary)", letterSpacing: "0.06em", textTransform: "uppercase" }}>
            Lambda Pipeline Status
          </span>
          <span style={{
            fontSize: 10, fontWeight: 700, padding: "1px 7px", borderRadius: 10,
            background: okCount === services.length ? "var(--green-dim)" : "rgba(245,158,11,0.15)",
            color: okCount === services.length ? "var(--green-up)" : "var(--yellow)",
          }}>
            {okCount}/{services.length} OK
          </span>
        </span>
        <span style={{ fontSize: 10, color: "var(--text-dim)" }}>{isCollapsed ? "▼ show" : "▲ hide"}</span>
      </button>

      {!isCollapsed && (
        <div style={s.grid}>
          {services.map(svc => (
            <div key={svc.name} id={`pipeline-${svc.name}`} style={s.card}>
              <div style={s.cardTop}>
                <span style={{ fontSize: 14 }}>{svc.icon}</span>
                <div style={{
                  width: 8, height: 8, borderRadius: "50%",
                  background:
                    svc.status === "ok" ? "var(--green-up)" :
                      svc.status === "error" ? "var(--red-down)" :
                        "var(--yellow)",
                  boxShadow:
                    svc.status === "ok" ? "0 0 6px var(--green-up)" :
                      svc.status === "error" ? "0 0 6px var(--red-down)" :
                        "none",
                  animation: svc.status === "ok" ? "pulse-dot 2s ease infinite" : "none",
                  marginLeft: "auto",
                }} />
              </div>
              <div style={{ fontSize: 11, fontWeight: 600, color: "var(--text-primary)", marginTop: 4 }}>
                {svc.label}
              </div>
              <div style={{ fontSize: 9, color: LAYER_COLOR[svc.layer], letterSpacing: "0.04em" }}>
                {svc.detail}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  wrapper: {
    background: "var(--bg-primary)",
    borderTop: "1px solid var(--border-subtle)",
  },
  toggle: {
    width: "100%",
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    padding: "7px 16px",
    background: "transparent",
    border: "none",
    cursor: "pointer",
    color: "var(--text-primary)",
    fontFamily: "inherit",
    borderBottom: "1px solid var(--border-subtle)",
  },
  grid: {
    display: "grid",
    gridTemplateColumns: "repeat(8, 1fr)",
    gap: 1,
    background: "var(--border-subtle)",
    padding: 0,
  },
  card: {
    background: "var(--bg-secondary)",
    padding: "10px 12px",
    display: "flex",
    flexDirection: "column",
  },
  cardTop: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
  },
}
