'use client';

import { useEffect, useRef, useCallback } from 'react';
import type { CandleData, Timeframe } from '@/types';
import { TIMEFRAMES } from '@/lib/constants';

interface CandlestickChartProps {
  candles: CandleData[];
  timeframe: Timeframe;
  onTimeframeChange: (tf: Timeframe) => void;
}

export default function CandlestickChart({
  candles,
  timeframe,
  onTimeframeChange,
}: CandlestickChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<ReturnType<typeof import('lightweight-charts').createChart> | null>(null);
  const candleSeriesRef = useRef<ReturnType<ReturnType<typeof import('lightweight-charts').createChart>['addCandlestickSeries']> | null>(null);
  const volumeSeriesRef = useRef<ReturnType<ReturnType<typeof import('lightweight-charts').createChart>['addHistogramSeries']> | null>(null);

  const initChart = useCallback(async () => {
    if (!chartContainerRef.current) return;

    // Dynamic import to avoid SSR issues
    const { createChart, ColorType, CrosshairMode } = await import('lightweight-charts');

    // Cleanup previous chart
    if (chartRef.current) {
      chartRef.current.remove();
    }

    const container = chartContainerRef.current;

    const chart = createChart(container, {
      width: container.clientWidth,
      height: container.clientHeight,
      layout: {
        background: { type: ColorType.Solid, color: '#181a20' },
        textColor: '#848e9c',
        fontFamily: "'Geist Mono', monospace",
        fontSize: 11,
      },
      grid: {
        vertLines: { color: 'rgba(43, 49, 57, 0.5)' },
        horzLines: { color: 'rgba(43, 49, 57, 0.5)' },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          color: 'rgba(240, 185, 11, 0.4)',
          labelBackgroundColor: '#f0b90b',
        },
        horzLine: {
          color: 'rgba(240, 185, 11, 0.4)',
          labelBackgroundColor: '#f0b90b',
        },
      },
      rightPriceScale: {
        borderColor: '#2b3139',
        scaleMargins: { top: 0.1, bottom: 0.25 },
      },
      timeScale: {
        borderColor: '#2b3139',
        timeVisible: true,
        secondsVisible: false,
      },
    });

    chartRef.current = chart;

    // Candlestick series
    const candleSeries = chart.addCandlestickSeries({
      upColor: '#0ecb81',
      downColor: '#f6465d',
      borderDownColor: '#f6465d',
      borderUpColor: '#0ecb81',
      wickDownColor: '#f6465d',
      wickUpColor: '#0ecb81',
    });
    candleSeriesRef.current = candleSeries;

    // Volume series on the same pane, using histogram
    const volumeSeries = chart.addHistogramSeries({
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });

    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });

    volumeSeriesRef.current = volumeSeries;

    // Handle resize
    const resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width, height } = entry.contentRect;
        chart.applyOptions({ width, height });
      }
    });

    resizeObserver.observe(container);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
      chartRef.current = null;
    };
  }, []);

  // Initialize chart
  useEffect(() => {
    const cleanup = initChart();
    return () => {
      cleanup?.then((fn) => fn?.());
    };
  }, [initChart]);

  // Update data when candles change
  useEffect(() => {
    if (!candleSeriesRef.current || !volumeSeriesRef.current || candles.length === 0) {
      return;
    }

    const candleData = candles.map((c) => ({
      time: c.time as import('lightweight-charts').UTCTimestamp,
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
    }));

    const volumeData = candles.map((c) => ({
      time: c.time as import('lightweight-charts').UTCTimestamp,
      value: c.volume,
      color: c.close >= c.open ? 'rgba(14, 203, 129, 0.3)' : 'rgba(246, 70, 93, 0.3)',
    }));

    candleSeriesRef.current.setData(candleData);
    volumeSeriesRef.current.setData(volumeData);

    // Auto-scroll to latest
    if (chartRef.current) {
      chartRef.current.timeScale().scrollToRealTime();
    }
  }, [candles]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Timeframe toolbar */}
      <div className="chart-toolbar">
        {TIMEFRAMES.map((tf) => (
          <button
            key={tf.value}
            className={`chart-toolbar-btn ${timeframe === tf.value ? 'active' : ''}`}
            onClick={() => onTimeframeChange(tf.value)}
            id={`tf-${tf.value}`}
          >
            {tf.label}
          </button>
        ))}
        <div style={{ flex: 1 }} />
        <span
          style={{
            fontSize: '11px',
            fontFamily: 'var(--font-mono)',
            color: 'var(--text-tertiary)',
          }}
        >
          {candles.length} candles
        </span>
      </div>

      {/* Chart */}
      <div
        ref={chartContainerRef}
        className="chart-container"
        style={{ flex: 1, minHeight: 0 }}
        id="candlestick-chart"
      />

      {candles.length === 0 && (
        <div
          style={{
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            textAlign: 'center',
            color: 'var(--text-tertiary)',
            fontSize: '14px',
            zIndex: 10,
          }}
        >
          <div style={{ fontSize: '32px', marginBottom: '8px' }}>📊</div>
          <div>Đang chờ dữ liệu để vẽ biểu đồ nến...</div>
          <div style={{ fontSize: '12px', marginTop: '4px' }}>
            Cần ít nhất 1 phút dữ liệu
          </div>
        </div>
      )}
    </div>
  );
}
