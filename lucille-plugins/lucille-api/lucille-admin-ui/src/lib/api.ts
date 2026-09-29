const API_BASE = import.meta.env.VITE_API_BASE ?? ""

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`)
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

async function healthCheck(path: string): Promise<boolean> {
  try {
    const res = await fetch(`${API_BASE}${path}`)
    return res.ok
  } catch {
    return false
  }
}

/**
 * POST a Lucille config as a JSON object to /v1/config.
 *
 * On success the API returns JSON `{ configId }`. On failure it returns a
 * plain-text body (e.g. "Invalid configuration provided: ..."), so errors are
 * read as text to preserve the useful message.
 *
 * @returns the created config's UUID
 * @throws Error with the server's message on a non-2xx response
 */
async function postConfig(config: unknown): Promise<string> {
  const res = await fetch(`${API_BASE}/v1/config`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  })

  if (!res.ok) {
    const message = (await res.text()) || `${res.status} ${res.statusText}`
    throw new Error(message)
  }

  const data: { configId?: string } = await res.json()
  if (!data.configId) {
    throw new Error("Server did not return a configId.")
  }
  return data.configId
}

/**
 * Start a run for an existing config via POST /v1/run.
 *
 * On success the API returns the run details JSON (including `runId`). On
 * failure it returns a plain-text/error body, read as text to preserve the
 * message.
 *
 * @returns the new run's ID
 * @throws Error with the server's message on a non-2xx response
 */
async function startRun(configId: string): Promise<string> {
  const res = await fetch(`${API_BASE}/v1/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ configId }),
  })

  if (!res.ok) {
    const message = (await res.text()) || `${res.status} ${res.statusText}`
    throw new Error(message)
  }

  const data: { runId?: string } = await res.json()
  if (!data.runId) {
    throw new Error("Server did not return a runId.")
  }
  return data.runId
}

export const api = { get, healthCheck, postConfig, startRun }
