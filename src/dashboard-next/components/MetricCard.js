'use client';
import { useEffect, useState, createElement } from 'react';

export default function MetricCard({ label, value, delta, deltaType = 'neutral', icon, color }) {
  const [show, setShow] = useState(false);
  useEffect(() => { setShow(true); }, []);
  const cls = deltaType === 'positive' ? 'positive' : deltaType === 'negative' ? 'negative' : 'neutral';

  const renderIcon = () => {
    if (!icon) return null;
    if (typeof icon === 'string') {
      return <span style={{ fontSize: 14 }}>{icon}</span>;
    }
    // Lucide icons are forwardRef objects — use createElement to render them
    return createElement(icon, { size: 14, style: { opacity: 0.7 } });
  };

  return (
    <div className="metric-card" style={{ opacity: show ? 1 : 0, transform: show ? 'translateY(0)' : 'translateY(10px)', transition: 'all 0.4s ease' }}>
      <div className="metric-label">
        {renderIcon()}
        {label}
      </div>
      <div className="metric-value" style={color ? { color } : {}}>{value}</div>
      {delta && <div className={`metric-delta ${cls}`}>{delta}</div>}
    </div>
  );
}
