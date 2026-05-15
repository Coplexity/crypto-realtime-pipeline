"use client"
import { useEffect, useRef } from "react"

export default function CandleChart({ candles, liveCandle }: { candles: any[], liveCandle: any }) {
  const chartRef  = useRef<HTMLDivElement>(null)
  const chartInst = useRef<any>(null)
  const seriesRef = useRef<any>(null)
  const volRef    = useRef<any>(null)

  useEffect(() => {
    if (!chartRef.current || typeof window === "undefined") return
    const { createChart } = require("lightweight-charts")

    chartInst.current = createChart(chartRef.current, {
      width:  chartRef.current.clientWidth,
      height: 380,
      layout: { background: { color: "#0a0e1a" }, textColor: "#64748b" },
      grid:   { vertLines: { color: "#0f1923" }, horzLines: { color: "#0f1923" } },
      crosshair: { mode: 1 },
      rightPriceScale: { borderColor: "#1e3a5f" },
      timeScale: { borderColor: "#1e3a5f", timeVisible: true },
    })

    seriesRef.current = chartInst.current.addCandlestickSeries({
      upColor: "#22c55e", downColor: "#ef4444",
      borderUpColor: "#22c55e", borderDownColor: "#ef4444",
      wickUpColor: "#22c55e", wickDownColor: "#ef4444",
    })

    volRef.current = chartInst.current.addHistogramSeries({
      color: "#1e3a5f", priceScaleId: "vol",
    })
    chartInst.current.priceScale("vol").applyOptions({
      scaleMargins: { top: 0.85, bottom: 0 },
    })

    const ro = new ResizeObserver(() => {
      if (chartRef.current)
        chartInst.current?.resize(chartRef.current.clientWidth, 380)
    })
    ro.observe(chartRef.current)
    return () => { ro.disconnect(); chartInst.current?.remove() }
  }, [])

  // Load historical candles
  useEffect(() => {
    if (!candles?.length || !seriesRef.current) return
    const data = candles.map((c: any) => ({
      time: Math.floor((c.openTime || c.time) / 1000),
      open: c.open, high: c.high, low: c.low, close: c.close,
    })).filter((c: any) => c.time > 0)

    const vols = candles.map((c: any) => ({
      time:  Math.floor((c.openTime || c.time) / 1000),
      value: c.volume,
      color: c.close >= c.open ? "rgba(34,197,94,0.3)" : "rgba(239,68,68,0.3)",
    })).filter((v: any) => v.time > 0)

    try {
      seriesRef.current.setData(data)
      volRef.current?.setData(vols)
      chartInst.current?.timeScale().fitContent()
    } catch {}
  }, [candles])

// Update live candle
  useEffect(() => {
    if (!liveCandle || !seriesRef.current) return
    try {
      const timeSecs = Math.floor(liveCandle.openTime / 1000)
      
      // 1. Cập nhật nến giá
      seriesRef.current.update({
        time:  timeSecs,
        open:  liveCandle.open,
        high:  liveCandle.high,
        low:   liveCandle.low,
        close: liveCandle.close,
      })
      
      // 2. [BỔ SUNG] Cập nhật luôn cột Volume tương ứng
      if (volRef.current) {
        volRef.current.update({
          time:  timeSecs,
          value: liveCandle.volume,
          color: liveCandle.close >= liveCandle.open ? "rgba(34,197,94,0.3)" : "rgba(239,68,68,0.3)",
        })
      }
    } catch {}  
  }, [liveCandle])

  return (
    <div style={{ background: "#0a0e1a", borderRadius: 8, border: "1px solid #1e3a5f", overflow: "hidden" }}>
      <div ref={chartRef} style={{ width: "100%", height: 380 }} />
    </div>
  )
}
