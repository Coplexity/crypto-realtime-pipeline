import type { ReactNode } from 'react';

interface CardProps {
  title?: string;
  extra?: ReactNode;
  children: ReactNode;
  noPadding?: boolean;
  id?: string;
  className?: string;
  style?: React.CSSProperties;
}

export default function Card({ title, extra, children, noPadding, id, className, style }: CardProps) {
  return (
    <div className={`card ${className || ''}`} id={id} style={style}>
      {title && (
        <div className="card-header">
          <div className="card-title">{title}</div>
          {extra && <div>{extra}</div>}
        </div>
      )}
      <div className={`card-body ${noPadding ? 'no-padding' : ''}`}>{children}</div>
    </div>
  );
}
