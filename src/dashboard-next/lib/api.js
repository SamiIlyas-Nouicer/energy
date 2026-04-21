const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export async function fetchAPI(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: { 'Content-Type': 'application/json', ...options.headers },
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export const getEnergyMix = () => fetchAPI('/api/energy-mix');
export const getCO2Latest = () => fetchAPI('/api/co2-latest');
export const getRegionalWeeks = () => fetchAPI('/api/regional/weeks');
export const getRegional = (week) => fetchAPI(`/api/regional${week ? `?week=${week}` : ''}`);
export const getForecastActual = (start, end) => {
  const params = new URLSearchParams();
  if (start) params.set('start', start);
  if (end) params.set('end', end);
  const qs = params.toString();
  return fetchAPI(`/api/forecast/actual${qs ? `?${qs}` : ''}`);
};
export const getPipelineHealth = () => fetchAPI('/api/pipeline-health');
export const getAPIHealth = () => fetchAPI('/health').then(() => true).catch(() => false);
export const postPredict = (features) => fetchAPI('/predict', { method: 'POST', body: JSON.stringify(features) });
