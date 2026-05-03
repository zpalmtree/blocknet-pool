import type { CSSProperties, ReactNode } from 'react';

interface StatCardProps {
  className?: string;
  id?: string;
  label: ReactNode;
  meta?: ReactNode;
  mono?: boolean;
  onClick?: () => void;
  style?: CSSProperties;
  title?: string;
  value: ReactNode;
  valueTitle?: string;
}

export function StatCard({
  className,
  id,
  label,
  meta,
  mono,
  onClick,
  style,
  title,
  value,
  valueTitle,
}: StatCardProps) {
  return (
    <div className={className ? `stat-card ${className}` : 'stat-card'} title={title} onClick={onClick} style={style}>
      <div className="label">{label}</div>
      <div className={mono ? 'value mono' : 'value'} id={id} title={valueTitle}>
        {value}
      </div>
      {meta != null && <div className="stat-meta">{meta}</div>}
    </div>
  );
}
