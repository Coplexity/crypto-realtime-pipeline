'use client';

import { useEffect, useRef, useCallback } from 'react';



interface MiniSparklineProps {
  data: number[];
  width?: number;
  height?: number;
  color?: string;
  fillOpacity?: number;
}

/**
 * Lightweight sparkline chart using canvas for overview metrics.
 */
export default function MiniSparkline({
  data,
  width = 120,
  height = 40,
  color = '#f0b90b',
  fillOpacity = 0.1,
}: MiniSparklineProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas || data.length < 2) return;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const dpr = window.devicePixelRatio || 1;
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    ctx.scale(dpr, dpr);

    ctx.clearRect(0, 0, width, height);

    const min = Math.min(...data);
    const max = Math.max(...data);
    const range = max - min || 1;
    const padding = 2;

    const points: [number, number][] = data.map((val, i) => [
      (i / (data.length - 1)) * (width - padding * 2) + padding,
      height - padding - ((val - min) / range) * (height - padding * 2),
    ]);

    // Fill
    ctx.beginPath();
    ctx.moveTo(points[0][0], height);
    points.forEach(([x, y]) => ctx.lineTo(x, y));
    ctx.lineTo(points[points.length - 1][0], height);
    ctx.closePath();
    ctx.fillStyle = color.replace(')', `, ${fillOpacity})`).replace('rgb', 'rgba');
    ctx.fill();

    // Line
    ctx.beginPath();
    ctx.moveTo(points[0][0], points[0][1]);
    for (let i = 1; i < points.length; i++) {
      ctx.lineTo(points[i][0], points[i][1]);
    }
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.stroke();

    // Last point dot
    const lastPoint = points[points.length - 1];
    ctx.beginPath();
    ctx.arc(lastPoint[0], lastPoint[1], 2.5, 0, Math.PI * 2);
    ctx.fillStyle = color;
    ctx.fill();
  }, [data, width, height, color, fillOpacity]);

  useEffect(() => {
    draw();
  }, [draw]);

  return (
    <canvas
      ref={canvasRef}
      style={{ width: `${width}px`, height: `${height}px` }}
    />
  );
}
