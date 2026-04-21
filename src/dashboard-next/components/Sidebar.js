'use client';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Activity, Map, TrendingUp, Shield, Home, Zap, Database, BarChart3, Cpu } from 'lucide-react';

const NAV = [
  { href: '/', label: 'Overview', icon: Home },
  { href: '/energy-mix', label: 'Live Energy Mix', icon: Activity },
  { href: '/regional', label: 'Regional Map', icon: Map },
  { href: '/forecast', label: 'Forecast', icon: TrendingUp },
  { href: '/pipeline', label: 'Pipeline Health', icon: Shield },
];

export default function Sidebar() {
  const pathname = usePathname();
  return (
    <aside className="sidebar">
      <div className="sidebar-brand">
        <div className="sidebar-logo"><Zap size={18} color="#fff" /></div>
        <div>
          <div className="sidebar-title">Energy Platform</div>
          <div className="sidebar-subtitle">French Grid Intelligence</div>
        </div>
      </div>
      <div className="sidebar-divider" />
      <div className="sidebar-section">Navigation</div>
      <nav>
        {NAV.map(({ href, label, icon: Icon }) => (
          <Link key={href} href={href} className={`nav-item ${pathname === href ? 'active' : ''}`}>
            <Icon className="nav-icon" size={18} />
            {label}
          </Link>
        ))}
      </nav>
      <div className="sidebar-divider" />
      <div className="sidebar-section">Stack</div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4, padding: '0 12px' }}>
        <span style={{ fontSize: 12, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 8 }}><Database size={13} /> DuckDB · Gold Layer</span>
        <span style={{ fontSize: 12, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 8 }}><Cpu size={13} /> LightGBM · MLflow</span>
        <span style={{ fontSize: 12, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 8 }}><BarChart3 size={13} /> FastAPI · Next.js</span>
      </div>
      <div className="sidebar-footer">
        <div className="sidebar-tag">
          <span className="sidebar-tag-dot" />
          <span>RTE France · Live Data</span>
        </div>
      </div>
    </aside>
  );
}
