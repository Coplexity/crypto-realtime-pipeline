"use client"
import { useEffect, useRef, useState, useCallback } from "react"

interface TickerItem {
  symbol: string
  price: number | null
  prevPrice: number | null
}

interface Props {
  symbols: string[]
  activeSymbol: string
  onSelect: (symbol: string) => void
}

export default function TickerBar({ symbols, activeSymbol, onSelect }: Props) {
  const [tickers, setTickers] = useState<Record<string, TickerItem>>(() =>
    Object.fromEntries(symbols.map(s => [s, { symbol: s, price: null, prevPrice: null }]))
  )
  const [flash, setFlash] = useState<Record<string, "green" | "red" | null>>({})
  const wsRefs = useRef<Record<string, WebSocket>>({})

  const triggerFlash = useCallback((symbol: string, dir: "green" | "red") => {
    setFlash(f => ({ ...f, [symbol]: dir }))
    setTimeout(() => setFlash(f => ({ ...f, [symbol]: null })), 620)
  }, [])

  useEffect(() => {
    const WS_URL = process.env.NEXT_PUBLIC_WS_URL || "ws://localhost:8000"

    symbols.forEach(symbol => {
      if (wsRefs.current[symbol]) return

      const connect = () => {
        const ws = new WebSocket(`${WS_URL}/ws/kline?symbol=${symbol}&interval=1m`)
        wsRefs.current[symbol] = ws

        ws.onmessage = (e) => {
          try {
            const msg = JSON.parse(e.data)
            const candle = msg.candle || (msg.type === "latest" ? msg.candle : null)
            if (candle?.close) {
              setTickers(prev => {
                const cur = prev[symbol]
                const newPrice = Number(candle.close)
                const dir = cur.price != null
                  ? newPrice > cur.price ? "green" : newPrice < cur.price ? "red" : null
                  : null
                if (dir) triggerFlash(symbol, dir)
                return {
                  ...prev,
                  [symbol]: { symbol, price: newPrice, prevPrice: cur.price }
                }
              })
            }
          } catch {}
        }

        ws.onclose = () => {
          delete wsRefs.current[symbol]
          setTimeout(connect, 3000)
        }
      }

      connect()
    })

    return () => {
      Object.values(wsRefs.current).forEach(ws => ws.close())
      wsRefs.current = {}
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbols.join(",")])

  return (
    <div style={s.bar}>
      {symbols.map(sym => {
        const t = tickers[sym]
        const isActive = sym === activeSymbol
        const flashDir = flash[sym]
        const isUp = t.price != null && t.prevPrice != null ? t.price >= t.prevPrice : true
        const pct = t.price && t.prevPrice && t.prevPrice !== t.price
          ? ((t.price - t.prevPrice) / t.prevPrice) * 100
          : 0

        return (
          <button
            key={sym}
            id={`ticker-${sym}`}
            onClick={() => onSelect(sym)}
            className={flashDir ? `flash-${flashDir}` : ""}
            style={{
              ...s.btn,
              ...(isActive ? s.btnActive : {}),
            }}
          >
            <span style={s.symName}>{sym.replace("USDT", "")}</span>
            <span style={{ ...s.price, color: isUp ? "var(--green-up)" : "var(--red-down)" }}>
              {t.price != null
                ? `$${t.price.toLocaleString("en", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                : <span className="skeleton" style={{ display: "inline-block", width: 64, height: 12 }} />
              }
            </span>
            {t.price != null && t.prevPrice != null && t.prevPrice !== t.price && (
              <span style={{ ...s.pct, color: isUp ? "var(--green-up)" : "var(--red-down)" }}>
                {isUp ? "▲" : "▼"} {Math.abs(pct).toFixed(2)}%
              </span>
            )}
          </button>
        )
      })}
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  bar: {
    display: "flex",
    overflowX: "auto",
    background: "var(--bg-primary)",
    borderBottom: "1px solid var(--border-accent)",
    scrollbarWidth: "none",
  },
  btn: {
    display: "flex",
    flexDirection: "column",
    alignItems: "flex-start",
    padding: "8px 16px",
    border: "none",
    background: "transparent",
    cursor: "pointer",
    borderRight: "1px solid var(--border-subtle)",
    minWidth: 110,
    gap: 2,
    color: "var(--text-primary)",
    transition: "background 0.15s ease",
    flexShrink: 0,
  },
  btnActive: {
    background: "var(--bg-hover)",
    borderTop: "2px solid var(--accent-blue)",
  },
  symName: {
    fontSize: 10,
    color: "var(--text-secondary)",
    letterSpacing: "0.05em",
  },
  price: {
    fontSize: 13,
    fontWeight: 600,
  },
  pct: {
    fontSize: 9,
    fontWeight: 500,
  },
}
