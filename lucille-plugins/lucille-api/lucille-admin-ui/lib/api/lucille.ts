const BASE_URL = process.env.LUCILLE_API_URL || 'http://localhost:8080';
const AUTH_HEADER: Record<string,string> = process.env.LUCILLE_API_AUTH
  ? { authorization: process.env.LUCILLE_API_AUTH }
  : {};

async function fetchLucille(endpoint: string, init: RequestInit = {}) {
  // Merge default auth header with any provided headers
  const headers: Record<string,string> = {
    ...AUTH_HEADER,
    ...((init.headers as Record<string,string>) || {}),
  };
  const res = await fetch(`${BASE_URL}${endpoint}`, { ...init, headers });
  const text = await res.text();
  let data;
  try { data = text ? JSON.parse(text) : undefined; } catch { data = text; }
  return { ok: res.ok, status: res.status, data };
}

export async function getConfig(configId: string) {
  return fetchLucille(`/v1/config/${configId}`);
}

export async function getRun(runId: string) {
  return fetchLucille(`/v1/run/${runId}`);
}

export async function getRuns() {
  return fetchLucille(`/v1/run`);
}

export async function createRun(configId: string) {
  return fetchLucille(`/v1/run`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ configId }),
  });
}
