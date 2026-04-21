'use client';
import { useEffect, useState, useCallback } from 'react';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { Zap, Wind, Sun, Droplets, Leaf, Flame, CircleDot, CloudCog, Calendar, Plug, Activity } from 'lucide-react';
import MetricCard from '@/components/MetricCard';
import { getEnergyMix, getCO2Latest } from '@/lib/api';

const SOURCES = [
  { key: 'nuclear_mwh', label: 'Nuclear', color: '#3B82F6' },
  { key: 'wind_mwh', label: 'Wind', color: '#34D399' },
  { key: 'solar_mwh', label: 'Solar', color: '#FBBF24' },
  { key: 'hydro_mwh', label: 'Hydro', color: '#14B8A6' },
  { key: 'bio_mwh', label: 'Bio', color: '#A78BFA' },
  { key: 'gas_mwh', label: 'Gas', color: '#F97316' },
  { key: 'coal_mwh', label: 'Coal', color: '#6B7280' },
  { key: 'oil_mwh', label: 'Oil', color: '#EF4444' },
];

const RENEWABLE_ICONS = {
  wind_mwh: Wind,
  solar_mwh: Sun,
  hydro_mwh: Droplets,
  bio_mwh: Leaf,
};

const RENEWABLE_KEYS = ['wind_mwh', 'solar_mwh', 'hydro_mwh', 'bio_mwh'];

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload) return null;
  return (
    <div className="custom-tooltip">
      <div className="label">{label}</div>
      {payload.map((p, i) => (
        <div key={i} className="item"><span className="dot" style={{ background: p.color }} />{p.name}: {(p.value / 1000).toFixed(2)} GWh</div>
      ))}
    </div>
  );
}

export default function EnergyMixPage() {
  const [data, setData] = useState([]);
  const [co2, setCo2] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchData = useCallback(async () => {
    try {
      const [mixRes, co2Res] = await Promise.all([getEnergyMix(), getCO2Latest()]);
      const rows = mixRes.data.map(r => ({ ...r, time: r.date.slice(5, 16).replace('T', ' ') }));
      setData(rows);
      setCo2(co2Res.co2_intensity);
      setError(null);
    } catch (e) { setError(e.message); }
    setLoading(false);
  }, []);

  useEffect(() => { fetchData(); const id = setInterval(fetchData, 60000); return () => clearInterval(id); }, [fetchData]);

  if (loading) return <div className="loader"><div className="spinner" /></div>;
  if (error) return <div className="error-box"><Activity size={16} /> {error}</div>;
  if (!data.length) return <div className="error-box">No data available</div>;

  const latest = data[data.length - 1];
  const prev = data.length > 1 ? data[data.length - 2] : latest;
  const renewPct = (latest.renewable_share_pct ?? 0).toFixed(1);
  const renewDelta = ((latest.renewable_share_pct ?? 0) - (prev.renewable_share_pct ?? 0)).toFixed(1);
  const consumptionGW = (latest.consumption_mwh / 1000).toFixed(1);
  const consDelta = ((latest.consumption_mwh - prev.consumption_mwh) / 1000).toFixed(1);
  const lastTime = latest.date.slice(0, 16).replace('T', ' ');

  return (
    <div className="animate-in">
      <div className="page-header">
        <h1><CircleDot size={24} style={{ color: '#ef4444' }} /> Live Energy Mix</h1>
        <p>Real-time French electricity generation — last 48 hours · auto-refreshes every 60s</p>
      </div>

      <div className="grid-4 stagger" style={{ marginBottom: 28 }}>
        <MetricCard label="Renewable Share" value={`${renewPct}%`} delta={`${renewDelta > 0 ? '+' : ''}${renewDelta}%`} deltaType={renewDelta >= 0 ? 'positive' : 'negative'} icon={Zap} color="var(--accent-emerald)" />
        <MetricCard label="CO₂ Intensity" value={co2 ? `${co2} gCO₂/kWh` : 'N/A'} icon={CloudCog} color="var(--accent-cyan)" />
        <MetricCard label="Consumption" value={`${consumptionGW} GW`} delta={`${consDelta > 0 ? '+' : ''}${consDelta} GW`} deltaType={parseFloat(consDelta) <= 0 ? 'positive' : 'negative'} icon={Plug} color="var(--accent-amber)" />
        <MetricCard label="Last Reading" value={lastTime} icon={Calendar} />
      </div>

      <div className="chart-card">
        <h3><Zap size={16} style={{ color: 'var(--accent-cyan)' }} /> Generation by Source (last 48h)</h3>
        <ResponsiveContainer width="100%" height={420}>
          <AreaChart data={data} margin={{ top: 5, right: 5, left: 0, bottom: 5 }}>
            <defs>
              {SOURCES.map(s => (
                <linearGradient key={s.key} id={`grad-${s.key}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={s.color} stopOpacity={0.3} />
                  <stop offset="95%" stopColor={s.color} stopOpacity={0.05} />
                </linearGradient>
              ))}
            </defs>
            <XAxis dataKey="time" tick={{ fill: '#5a6380', fontSize: 11 }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
            <YAxis tick={{ fill: '#5a6380', fontSize: 11 }} tickLine={false} axisLine={false} tickFormatter={v => `${(v / 1000).toFixed(0)}`} label={{ value: 'GWh', angle: -90, position: 'insideLeft', fill: '#5a6380', fontSize: 11 }} />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 12, color: '#8892b0' }} />
            {SOURCES.map(s => (
              <Area key={s.key} type="monotone" dataKey={s.key} name={s.label} stackId="1" stroke={s.color} fill={`url(#grad-${s.key})`} strokeWidth={1} />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>

      <div className="section-title"><Leaf size={16} style={{ color: 'var(--accent-emerald)' }} /> Current Renewable Breakdown</div>
      <div className="grid-4 stagger">
        {RENEWABLE_KEYS.map(k => {
          const src = SOURCES.find(s => s.key === k);
          const RenIcon = RENEWABLE_ICONS[k];
          const val = latest[k] || 0;
          const pct = latest.total_production_mwh > 0 ? ((val / latest.total_production_mwh) * 100).toFixed(1) : '0.0';
          return <MetricCard key={k} label={src.label} value={`${(val / 1000).toFixed(2)} GWh`} delta={`${pct}% of total`} deltaType="neutral" icon={RenIcon} color={src.color} />;
        })}
      </div>
    </div>
  );
}
