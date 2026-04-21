'use client';
import { useEffect, useState, useCallback } from 'react';
import { Shield, BarChart3, Zap, CloudCog, MapPin, RefreshCw, AlertTriangle, CheckCircle, XCircle, Thermometer, Activity, Hash, Table2 } from 'lucide-react';
import MetricCard from '@/components/MetricCard';
import { getPipelineHealth } from '@/lib/api';

function freshness(isoStr) {
  if (!isoStr) return { label: 'Unknown', ok: false };
  const diff = Date.now() - new Date(isoStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return { label: `${mins} min ago`, ok: true };
  if (mins < 1440) return { label: `${Math.floor(mins / 60)}h ${mins % 60}m ago`, ok: true };
  return { label: `${Math.floor(mins / 1440)} days ago`, ok: false };
}

export default function PipelinePage() {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchData = useCallback(async () => {
    try {
      const res = await getPipelineHealth();
      setStats(res);
      setError(null);
    } catch (e) { setError(e.message); }
    setLoading(false);
  }, []);

  useEffect(() => { fetchData(); const id = setInterval(fetchData, 60000); return () => clearInterval(id); }, [fetchData]);

  if (loading) return <div className="loader"><div className="spinner" /></div>;
  if (error) return <div className="error-box">{error}</div>;

  const tables = stats.tables;
  const overall = freshness(stats.overall_latest);

  const checks = Object.entries(tables).map(([key, s]) => {
    const f = freshness(s.latest);
    const nullPct = s.null_consumption_pct ?? s.null_pct ?? s.null_co2_pct ?? 0;
    const nullOk = nullPct < 5;
    const anomalies = s.anomalies_consumption ?? s.anomalies ?? s.anomalies_co2 ?? 0;
    const anomOk = anomalies === 0;
    const allOk = f.ok && nullOk && anomOk;
    return { key, ...s, freshness: f, nullPct, nullOk, anomalies, anomOk, allOk };
  });

  const StatusIcon = ({ ok }) => ok ? <CheckCircle size={14} style={{ color: 'var(--accent-emerald)' }} /> : <XCircle size={14} style={{ color: 'var(--accent-red)' }} />;

  return (
    <div className="animate-in">
      <div className="page-header">
        <h1><Shield size={24} style={{ color: 'var(--accent-violet)' }} /> Pipeline Health Monitor</h1>
        <p>Data quality checks, freshness, null rates, and anomaly detection</p>
      </div>

      <div className="glass-card" style={{ marginBottom: 24, display: 'flex', alignItems: 'center', gap: 12, padding: '16px 24px' }}>
        <span className={`badge ${overall.ok ? 'badge-success' : 'badge-danger'}`} style={{ fontSize: 13, padding: '6px 14px' }}>
          {overall.ok ? <><CheckCircle size={13} /> Pipeline Healthy</> : <><AlertTriangle size={13} /> Stale Data</>}
        </span>
        <span style={{ color: 'var(--text-secondary)', fontSize: 13 }}>Latest data: <strong>{overall.label}</strong></span>
      </div>

      <div className="section-title"><Table2 size={16} style={{ color: 'var(--accent-cyan)' }} /> Table-by-Table Status</div>
      <div className="glass-card" style={{ marginBottom: 24, padding: 0, overflow: 'hidden' }}>
        <table className="data-table">
          <thead>
            <tr><th>Status</th><th>Table</th><th>Rows</th><th>Latest Data</th><th>Null Rate</th><th>Anomalies</th><th>Fresh</th><th>Nulls</th><th>Anom.</th></tr>
          </thead>
          <tbody>
            {checks.map(c => (
              <tr key={c.key}>
                <td><span className={`badge ${c.allOk ? 'badge-success' : 'badge-danger'}`}><StatusIcon ok={c.allOk} /></span></td>
                <td style={{ color: 'var(--text-primary)', fontFamily: 'var(--font-body)', fontWeight: 500 }}>{c.table}</td>
                <td>{c.rows?.toLocaleString()}</td>
                <td>{c.freshness.label}</td>
                <td>{c.nullPct.toFixed(2)}%</td>
                <td>{c.anomalies}</td>
                <td><StatusIcon ok={c.freshness.ok} /></td>
                <td><StatusIcon ok={c.nullOk} /></td>
                <td><StatusIcon ok={c.anomOk} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="section-title"><Hash size={16} style={{ color: 'var(--accent-cyan)' }} /> Gold Layer Summary</div>
      <div className="grid-4 stagger" style={{ marginBottom: 24 }}>
        <MetricCard label="hourly_energy_mix" value={tables.energy_mix?.rows?.toLocaleString()} icon={Zap} color="var(--accent-cyan)" />
        <MetricCard label="co2_intensity" value={tables.co2?.rows?.toLocaleString()} icon={CloudCog} color="var(--accent-emerald)" />
        <MetricCard label="daily_consumption" value={tables.consumption?.rows?.toLocaleString()} icon={BarChart3} color="var(--accent-amber)" />
        <MetricCard label="Regions Tracked" value={tables.regional?.regions} icon={MapPin} color="var(--accent-violet)" />
      </div>

      <div className="section-title"><AlertTriangle size={16} style={{ color: 'var(--accent-amber)' }} /> Data Quality Notes</div>
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
        <div className="glass-card" style={{ flex: 1, minWidth: 280 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}><Activity size={14} /> Consumption Anomalies</div>
          {tables.energy_mix?.anomalies_consumption === 0 ? (
            <span className="badge badge-success"><CheckCircle size={12} /> No anomalies found</span>
          ) : (
            <span className="badge badge-warning"><AlertTriangle size={12} /> {tables.energy_mix?.anomalies_consumption} anomalous rows</span>
          )}
        </div>
        <div className="glass-card" style={{ flex: 1, minWidth: 280 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}><Thermometer size={14} /> Temperature Coverage</div>
          {tables.consumption?.null_temperature_pct > 95 ? (
            <span className="badge badge-warning"><AlertTriangle size={12} /> {tables.consumption?.null_temperature_pct?.toFixed(0)}% null — weather join not run</span>
          ) : (
            <span className="badge badge-success"><CheckCircle size={12} /> {(100 - (tables.consumption?.null_temperature_pct || 0)).toFixed(1)}% complete</span>
          )}
        </div>
      </div>

      <div style={{ marginTop: 24, textAlign: 'center' }}>
        <button onClick={() => { setLoading(true); fetchData(); }} style={{
          padding: '10px 24px', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-glass)',
          background: 'var(--bg-glass)', color: 'var(--text-primary)', cursor: 'pointer', fontSize: 13,
          fontWeight: 500, fontFamily: 'var(--font-body)', transition: 'all 0.2s',
          display: 'inline-flex', alignItems: 'center', gap: 6,
        }}><RefreshCw size={14} /> Refresh Now</button>
        <p style={{ color: 'var(--text-muted)', fontSize: 11, marginTop: 8 }}>Auto-refreshes every 60 seconds</p>
      </div>
    </div>
  );
}
