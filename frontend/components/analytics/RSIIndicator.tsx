'use client';

interface RSIIndicatorProps {
  value: number;
}

function getRSILabel(value: number): { text: string; class: string } {
  if (value >= 70) return { text: 'Overbought', class: 'overbought' };
  if (value <= 30) return { text: 'Oversold', class: 'oversold' };
  return { text: 'Neutral', class: 'neutral' };
}

function getRSIBarColor(value: number): string {
  if (value >= 70) return 'var(--down)';
  if (value <= 30) return 'var(--up)';
  return 'var(--accent)';
}

export default function RSIIndicator({ value }: RSIIndicatorProps) {
  const { text, class: cls } = getRSILabel(value);

  return (
    <div className="rsi-gauge" id="rsi-indicator">
      <div className={`rsi-value ${cls}`}>{value.toFixed(1)}</div>
      <div className={`rsi-label`} style={{ color: getRSIBarColor(value) }}>
        {text}
      </div>

      <div className="rsi-bar">
        <div
          className="rsi-bar-fill"
          style={{
            width: `${value}%`,
            background: `linear-gradient(90deg, var(--up), var(--accent), var(--down))`,
          }}
        />
        {/* Zone markers */}
        <div
          style={{
            position: 'absolute',
            left: '30%',
            top: 0,
            bottom: 0,
            width: '1px',
            background: 'var(--text-tertiary)',
            opacity: 0.5,
          }}
        />
        <div
          style={{
            position: 'absolute',
            left: '70%',
            top: 0,
            bottom: 0,
            width: '1px',
            background: 'var(--text-tertiary)',
            opacity: 0.5,
          }}
        />
      </div>

      <div className="rsi-bar-zones">
        <span>0</span>
        <span style={{ color: 'var(--up)' }}>30</span>
        <span>50</span>
        <span style={{ color: 'var(--down)' }}>70</span>
        <span>100</span>
      </div>

      <div
        style={{
          marginTop: '16px',
          fontSize: '11px',
          color: 'var(--text-tertiary)',
          textAlign: 'center',
          lineHeight: '1.5',
        }}
      >
        RSI (Relative Strength Index) đo lường tốc độ và biên độ thay đổi giá.
        <br />
        Khi RSI {'>'} 70: giá có thể quá mua (overbought).
        <br />
        Khi RSI {'<'} 30: giá có thể quá bán (oversold).
      </div>
    </div>
  );
}
