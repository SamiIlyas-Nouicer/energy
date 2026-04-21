'use client';
import Link from 'next/link';
import { Activity, Map, TrendingUp, Shield, Zap, Server, GitBranch, Layers, Database, Cpu, BarChart3 } from 'lucide-react';

const CARDS = [
  { href: '/energy-mix', Icon: Activity, title: 'Live Energy Mix', desc: 'Real-time generation mix and CO₂ intensity for the last 48 hours.', color: 'rgba(239,68,68,0.15)', iconColor: '#ef4444' },
  { href: '/regional', Icon: Map, title: 'Regional Map', desc: 'Per-capita consumption by French region with interactive visualization.', color: 'rgba(52,211,153,0.15)', iconColor: '#34d399' },
  { href: '/forecast', Icon: TrendingUp, title: 'Consumption Forecast', desc: '24-hour ahead LightGBM predictions with confidence bands.', color: 'rgba(251,191,36,0.15)', iconColor: '#fbbf24' },
  { href: '/pipeline', Icon: Shield, title: 'Pipeline Health', desc: 'Data quality — null rates, anomalies, freshness monitoring.', color: 'rgba(167,139,250,0.15)', iconColor: '#a78bfa' },
];

export default function HomePage() {
  return (
    <div className="animate-in">
      <div className="page-header" style={{ marginBottom: 48 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 12 }}>
          <div style={{ width: 52, height: 52, background: 'var(--gradient-primary)', borderRadius: 'var(--radius-md)', display: 'flex', alignItems: 'center', justifyContent: 'center', boxShadow: '0 0 30px rgba(56,189,248,0.3)' }}>
            <Zap size={26} color="#fff" />
          </div>
          <div>
            <h1 style={{ fontSize: 32, fontWeight: 800, letterSpacing: -1, background: 'var(--gradient-primary)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>
              French Energy Intelligence
            </h1>
            <p style={{ color: 'var(--text-secondary)', fontSize: 15, marginTop: 4 }}>
              Real-time monitoring · Regional analysis · ML-powered forecasting · Pipeline observability
            </p>
          </div>
        </div>
      </div>

      <div className="grid-4 stagger" style={{ marginBottom: 48 }}>
        {CARDS.map(({ href, Icon, title, desc, color, iconColor }) => (
          <Link key={href} href={href} className="hero-card">
            <div className="hero-card-icon" style={{ background: color }}>
              <Icon size={22} color={iconColor} />
            </div>
            <h3>{title}</h3>
            <p>{desc}</p>
          </Link>
        ))}
      </div>

      <div className="glass-card" style={{ marginBottom: 24 }}>
        <h3 className="section-title"><Layers size={18} style={{ color: 'var(--accent-cyan)' }} /> Platform Architecture</h3>
        <div className="grid-3" style={{ marginTop: 16 }}>
          <div style={{ padding: 16, borderRadius: 'var(--radius-sm)', background: 'rgba(56,189,248,0.05)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--accent-cyan)', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}><Server size={14} /> Data Ingestion</div>
            <div style={{ fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.6 }}>RTE France Open API → Kafka → MinIO → Spark → Delta Lake</div>
          </div>
          <div style={{ padding: 16, borderRadius: 'var(--radius-sm)', background: 'rgba(52,211,153,0.05)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--accent-emerald)', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}><GitBranch size={14} /> Transformation</div>
            <div style={{ fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.6 }}>dbt models → Bronze → Silver → Gold → DuckDB analytics</div>
          </div>
          <div style={{ padding: 16, borderRadius: 'var(--radius-sm)', background: 'rgba(167,139,250,0.05)', border: '1px solid var(--border-subtle)' }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--accent-violet)', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}><Cpu size={14} /> ML & Serving</div>
            <div style={{ fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.6 }}>LightGBM → MLflow Registry → FastAPI → Next.js Dashboard</div>
          </div>
        </div>
      </div>

      <div style={{ display: 'flex', gap: 16 }}>
        <div className="glass-card" style={{ flex: 1 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--accent-cyan)', marginBottom: 4, display: 'flex', alignItems: 'center', gap: 6 }}><Database size={14} /> Data Source</div>
          <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>RTE France Open API — éCO2mix national data</div>
        </div>
        <div className="glass-card" style={{ flex: 1 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--accent-emerald)', marginBottom: 4, display: 'flex', alignItems: 'center', gap: 6 }}><BarChart3 size={14} /> Model Performance</div>
          <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>LightGBM — MAE 0.33 GWh · R² 0.95</div>
        </div>
        <div className="glass-card" style={{ flex: 1 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--accent-violet)', marginBottom: 4, display: 'flex', alignItems: 'center', gap: 6 }}><Layers size={14} /> Infrastructure</div>
          <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>Docker Compose · 8 services · Auto-refresh</div>
        </div>
      </div>
    </div>
  );
}
