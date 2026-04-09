interface MetricCardProps {
  label: string;
  value: string;
  change?: string;
  changeDirection?: 'up' | 'down' | 'neutral';
  icon?: string;
  id?: string;
}

export default function MetricCard({
  label,
  value,
  change,
  changeDirection = 'neutral',
  icon,
  id,
}: MetricCardProps) {
  return (
    <div className={`metric-card ${changeDirection}`} id={id}>
      {icon && <div className="metric-icon">{icon}</div>}
      <div className="metric-label">{label}</div>
      <div className={`metric-value ${changeDirection}`}>{value}</div>
      {change && (
        <div className={`metric-change ${changeDirection}`}>
          <span>{changeDirection === 'up' ? '▲' : changeDirection === 'down' ? '▼' : '─'}</span>
          <span>{change}</span>
        </div>
      )}
    </div>
  );
}
