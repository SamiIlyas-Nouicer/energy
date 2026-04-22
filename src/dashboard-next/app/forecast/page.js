'use client';
import { useEffect, useState, useCallback } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { TrendingUp, BarChart3, Sparkles, Zap, CheckCircle, XCircle, Wand2, Table2, Terminal } from 'lucide-react';
import MetricCard from '@/components/MetricCard';
import { getForecastActual, getAPIHealth, postPredict } from '@/lib/api';

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload) return null;
  return (
    <div className="custom-tooltip">
      <div className="label">{label}</div>
      {payload.map((p, i) => (
        <div key={i} className="item"><span className="dot" style={{ background: p.color }} />{p.name}: {p.value?.toFixed(3)} GWh</div>
      ))}
    </div>
  );
}

export default function ForecastPage() {
  const [actualData, setActualData] = useState([]);
  const [forecastData, setForecastData] = useState([]);
  const [apiOnline, setApiOnline] = useState(false);
  const [modelLoaded, setModelLoaded] = useState(false);
  const [loading, setLoading] = useState(true);
  const [forecasting, setForecasting] = useState(false);
  const [error, setError] = useState(null);

  const fetchData = useCallback(async () => {
    try {
      const actual = await getForecastActual();
      setActualData(actual.data);
      setError(null);
    } catch (e) { setError(e.message); }
    setLoading(false);
    // Health check runs independently — never blocks data display
    getAPIHealth().then(h => { setApiOnline(h.online); setModelLoaded(h.modelLoaded); }).catch(() => { setApiOnline(false); setModelLoaded(false); });
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const runForecast = async () => {
    if (!modelLoaded || !actualData.length) return;
    setForecasting(true);
    try {
      const lastActual = actualData[actualData.length - 1];
      const mean = actualData.reduce((s, d) => s + d.total_consumption_gwh, 0) / actualData.length;
      const slots = [];
      const baseDate = new Date(lastActual.date + 'T00:00:00');

      for (let i = 0; i < 48; i++) {
        const dt = new Date(baseDate.getTime() + (i + 1) * 30 * 60 * 1000);
        const seasonMap = { 12: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1, 6: 2, 7: 2, 8: 2, 9: 3, 10: 3, 11: 3 };
        const features = {
          hour_of_day: dt.getHours(),
          minute: dt.getMinutes() >= 30 ? 30 : 0,
          day_of_week: (dt.getDay() + 6) % 7,
          is_weekend: dt.getDay() === 0 || dt.getDay() === 6 ? 1 : 0,
          month: dt.getMonth() + 1,
          day_of_year: Math.floor((dt - new Date(dt.getFullYear(), 0, 0)) / 86400000),
          season: seasonMap[dt.getMonth() + 1],
          consumption_lag_1h: slots.length > 0 ? slots[slots.length - 1].pred : lastActual.total_consumption_gwh,
          consumption_lag_24h: mean,
          consumption_lag_168h: mean,
          renewable_share_pct: lastActual.avg_renewable_share_pct || 30,
        };
        try {
          const result = await postPredict(features);
          slots.push({
            date: dt.toISOString().slice(0, 16).replace('T', ' '),
            pred: result.predicted_gwh,
            low: result.confidence_low,
            high: result.confidence_high,
          });
        } catch { break; }
      }
      setForecastData(slots);
    } catch (e) { setError(e.message); }
    setForecasting(false);
  };

  if (loading) return <div className="loader"><div className="spinner" /></div>;
  if (error) return <div className="error-box">{error}</div>;

  const chartData = [
    ...actualData.map(d => ({ date: d.date, actual: d.total_consumption_gwh })),
    ...forecastData.map(d => ({ date: d.date, forecast: d.pred, low: d.low, high: d.high })),
  ];

  return (
    <div className="animate-in">
      <div className="page-header">
        <h1><TrendingUp size={24} style={{ color: 'var(--accent-amber)' }} /> Consumption Forecast</h1>
        <p>24-hour ahead prediction with LightGBM — actual vs forecast comparison</p>
      </div>

      <div style={{ display: 'flex', gap: 12, alignItems: 'center', marginBottom: 24 }}>
        <span className={`badge ${apiOnline ? 'badge-success' : 'badge-danger'}`}>
          {apiOnline ? <><CheckCircle size={12} /> API Online</> : <><XCircle size={12} /> API Offline</>}
        </span>
        {apiOnline && (
          <span className={`badge ${modelLoaded ? 'badge-success' : 'badge-warning'}`}>
            {modelLoaded ? <><CheckCircle size={12} /> Model Ready</> : <><Sparkles size={12} /> Model Loading</>}
          </span>
        )}
        <button onClick={runForecast} disabled={!modelLoaded || forecasting} style={{
          padding: '8px 20px', borderRadius: 'var(--radius-sm)', border: '1px solid var(--accent-cyan)',
          background: 'rgba(56,189,248,0.1)', color: 'var(--accent-cyan)', cursor: modelLoaded ? 'pointer' : 'not-allowed',
          fontSize: 13, fontWeight: 600, fontFamily: 'var(--font-body)', transition: 'all 0.2s',
          opacity: modelLoaded ? 1 : 0.4, display: 'flex', alignItems: 'center', gap: 6,
        }}>
          <Wand2 size={14} /> {forecasting ? 'Generating...' : 'Run 24h Forecast'}
        </button>
      </div>

      <div className="grid-4 stagger" style={{ marginBottom: 24 }}>
        <MetricCard label="Actual Days" value={actualData.length} icon={BarChart3} color="var(--accent-blue)" />
        <MetricCard label="Forecast Slots" value={forecastData.length} icon={Sparkles} color="var(--accent-orange)" />
        {actualData.length > 0 && <MetricCard label="Latest Consumption" value={`${actualData[actualData.length - 1].total_consumption_gwh.toFixed(2)} GWh`} icon={Zap} color="var(--accent-cyan)" />}
        {forecastData.length > 0 && <MetricCard label="Next Slot Forecast" value={`${forecastData[0].pred.toFixed(2)} GWh`} icon={TrendingUp} color="var(--accent-orange)" />}
      </div>

      <div className="chart-card">
        <h3><TrendingUp size={16} style={{ color: 'var(--accent-cyan)' }} /> Actual vs Forecast</h3>
        <ResponsiveContainer width="100%" height={440}>
          <LineChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
            <XAxis dataKey="date" tick={{ fill: '#5a6380', fontSize: 11 }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
            <YAxis tick={{ fill: '#5a6380', fontSize: 11 }} tickLine={false} axisLine={false} label={{ value: 'GWh', angle: -90, position: 'insideLeft', fill: '#5a6380', fontSize: 11 }} />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <Line type="monotone" dataKey="actual" name="Actual" stroke="#3B82F6" strokeWidth={2} dot={false} />
            {forecastData.length > 0 && <Line type="monotone" dataKey="forecast" name="Forecast" stroke="#F97316" strokeWidth={2.5} strokeDasharray="8 4" dot={false} />}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {forecastData.length > 0 && (
        <div className="glass-card">
          <h3 className="section-title"><Table2 size={16} style={{ color: 'var(--accent-cyan)' }} /> Forecast Data</h3>
          <div style={{ overflowX: 'auto', maxHeight: 300 }}>
            <table className="data-table">
              <thead><tr><th>Timestamp</th><th>Forecast (GWh)</th><th>Low (GWh)</th><th>High (GWh)</th></tr></thead>
              <tbody>
                {forecastData.map((r, i) => (
                  <tr key={i}><td>{r.date}</td><td>{r.pred?.toFixed(3)}</td><td>{r.low?.toFixed(3)}</td><td>{r.high?.toFixed(3)}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {!apiOnline && (
        <div className="glass-card" style={{ marginTop: 24, textAlign: 'center' }}>
          <p style={{ color: 'var(--text-secondary)', fontSize: 13, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8 }}>
            <Terminal size={14} /> Start the API to generate forecasts: <code style={{ color: 'var(--accent-cyan)', fontFamily: 'var(--font-mono)', fontSize: 12 }}>uvicorn src.api.main:app --port 8000</code>
          </p>
        </div>
      )}

      {apiOnline && !modelLoaded && (
        <div className="glass-card" style={{ marginTop: 24, textAlign: 'center' }}>
          <p style={{ color: 'var(--text-secondary)', fontSize: 13, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8 }}>
            <Terminal size={14} /> Train the model first: <code style={{ color: 'var(--accent-cyan)', fontFamily: 'var(--font-mono)', fontSize: 12 }}>python -m src.ml.train_local</code>
          </p>
        </div>
      )}
    </div>
  );
}
