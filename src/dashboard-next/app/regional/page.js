'use client';
import { useEffect, useState, useCallback } from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts';
import { MapPin, Trophy, Leaf, BarChart3, Table2 } from 'lucide-react';
import MetricCard from '@/components/MetricCard';
import { getRegional, getRegionalWeeks } from '@/lib/api';

const METRIC_OPTIONS = [
  { value: 'consumption_kwh_per_capita', label: 'Per-capita (kWh)', color: '#3B82F6' },
  { value: 'avg_renewable_share_pct', label: 'Renewable %', color: '#34D399' },
  { value: 'regional_consumption_gwh', label: 'Total (GWh)', color: '#F97316' },
];

function CustomTooltip({ active, payload }) {
  if (!active || !payload?.[0]) return null;
  const d = payload[0].payload;
  return (
    <div className="custom-tooltip">
      <div className="label">{d.region}</div>
      <div className="item">Per-capita: <b>{d.consumption_kwh_per_capita?.toFixed(1)} kWh</b></div>
      <div className="item">Renewable: <b>{d.avg_renewable_share_pct?.toFixed(1)}%</b></div>
      <div className="item">Total: <b>{d.regional_consumption_gwh?.toFixed(2)} GWh</b></div>
      <div className="item">Population: <b>{d.population?.toLocaleString()}</b></div>
    </div>
  );
}

export default function RegionalPage() {
  const [weeks, setWeeks] = useState([]);
  const [selectedWeek, setSelectedWeek] = useState(null);
  const [data, setData] = useState([]);
  const [metric, setMetric] = useState('consumption_kwh_per_capita');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    getRegionalWeeks().then(r => { setWeeks(r.weeks); if (r.weeks.length) setSelectedWeek(r.weeks[0]); }).catch(e => setError(e.message));
  }, []);

  const fetchData = useCallback(async () => {
    if (!selectedWeek) return;
    setLoading(true);
    try {
      const res = await getRegional(selectedWeek);
      setData(res.data);
      setError(null);
    } catch (e) { setError(e.message); }
    setLoading(false);
  }, [selectedWeek]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const metricCfg = METRIC_OPTIONS.find(m => m.value === metric);
  const sorted = [...data].sort((a, b) => (b[metric] || 0) - (a[metric] || 0));
  const top = sorted[0];
  const greenest = [...data].sort((a, b) => (b.avg_renewable_share_pct || 0) - (a.avg_renewable_share_pct || 0))[0];
  const avg = data.length ? (data.reduce((s, d) => s + (d.consumption_kwh_per_capita || 0), 0) / data.length).toFixed(1) : '—';

  return (
    <div className="animate-in">
      <div className="page-header">
        <h1><MapPin size={24} style={{ color: 'var(--accent-emerald)' }} /> Regional Energy Map</h1>
        <p>Weekly per-capita electricity consumption by French metropolitan region</p>
      </div>

      <div style={{ display: 'flex', gap: 16, marginBottom: 24, flexWrap: 'wrap' }}>
        <div>
          <label style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: 0.5, marginBottom: 4, display: 'block' }}>Week</label>
          <select className="custom-select" value={selectedWeek || ''} onChange={e => setSelectedWeek(e.target.value)}>
            {weeks.map(w => <option key={w} value={w}>{w}</option>)}
          </select>
        </div>
        <div>
          <label style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: 0.5, marginBottom: 4, display: 'block' }}>Metric</label>
          <select className="custom-select" value={metric} onChange={e => setMetric(e.target.value)}>
            {METRIC_OPTIONS.map(m => <option key={m.value} value={m.value}>{m.label}</option>)}
          </select>
        </div>
      </div>

      {loading ? <div className="loader"><div className="spinner" /></div> : error ? <div className="error-box">{error}</div> : (
        <>
          <div className="chart-card">
            <h3><BarChart3 size={16} style={{ color: 'var(--accent-cyan)' }} /> {metricCfg.label} by Region</h3>
            <ResponsiveContainer width="100%" height={Math.max(400, data.length * 36)}>
              <BarChart data={sorted} layout="vertical" margin={{ top: 5, right: 30, left: 120, bottom: 5 }}>
                <XAxis type="number" tick={{ fill: '#5a6380', fontSize: 11 }} axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="region" tick={{ fill: '#8892b0', fontSize: 12 }} axisLine={false} tickLine={false} width={110} />
                <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(56,189,248,0.05)' }} />
                <Bar dataKey={metric} radius={[0, 6, 6, 0]} maxBarSize={24}>
                  {sorted.map((_, i) => <Cell key={i} fill={metricCfg.color} fillOpacity={1 - (i * 0.04)} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="grid-3 stagger" style={{ marginBottom: 24 }}>
            <MetricCard label="Highest Consumption" value={top?.region || '—'} icon={Trophy} color="var(--accent-amber)" />
            <MetricCard label="Most Renewable" value={greenest?.region || '—'} icon={Leaf} color="var(--accent-emerald)" />
            <MetricCard label="National Avg Per-Capita" value={`${avg} kWh`} icon={BarChart3} color="var(--accent-cyan)" />
          </div>

          <div className="glass-card">
            <h3 className="section-title"><Table2 size={16} style={{ color: 'var(--accent-cyan)' }} /> Region Data Table</h3>
            <div style={{ overflowX: 'auto' }}>
              <table className="data-table">
                <thead><tr><th>Region</th><th>Population</th><th>Total (GWh)</th><th>Per-capita (kWh)</th><th>Renewable %</th></tr></thead>
                <tbody>
                  {sorted.map(r => (
                    <tr key={r.region}>
                      <td style={{ color: 'var(--text-primary)', fontFamily: 'var(--font-body)' }}>{r.region}</td>
                      <td>{r.population?.toLocaleString()}</td>
                      <td>{r.regional_consumption_gwh?.toFixed(2)}</td>
                      <td>{r.consumption_kwh_per_capita?.toFixed(1)}</td>
                      <td>{r.avg_renewable_share_pct?.toFixed(1)}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
