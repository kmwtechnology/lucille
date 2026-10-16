import { errorMessageFrom } from "@/lib/utils"

const API_BASE = import.meta.env.VITE_API_BASE ?? ""

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`)
  if (!res.ok) throw new Error(await errorMessageFrom(res))
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
 * POST a Lucille config to /v1/config as a raw HOCON string.
 *
 * The body is sent as-is with Content-Type application/hocon, so HOCON features
 * (comments, ${?ENV} substitutions, unquoted keys) are preserved and parsed by
 * the server. JSON is also valid HOCON, so plain JSON still works.
 *
 * On success the API returns JSON `{ configId }`. On failure the server's
 * error `message` is surfaced (see errorMessageFrom), never the raw body.
 *
 * @param body the raw HOCON (or JSON) configuration text
 * @returns the created config's UUID
 * @throws Error with the server's message on a non-2xx response
 */
async function postConfig(body: string): Promise<string> {
  const res = await fetch(`${API_BASE}/v1/config`, {
    method: "POST",
    headers: { "Content-Type": "application/hocon" },
    body,
  })

  if (!res.ok) {
    throw new Error(await errorMessageFrom(res))
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
 * failure the server's error `message` is surfaced, never the raw body.
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
    throw new Error(await errorMessageFrom(res))
  }

  const data: { runId?: string } = await res.json()
  if (!data.runId) {
    throw new Error("Server did not return a runId.")
  }
  return data.runId
}

/**
 * Delete a config via DELETE /v1/config/{configId}.
 *
 * Returns 200 on success, 404 if the id is unknown (surfaced as an error here).
 *
 * @throws Error with the server's message on a non-2xx response
 */
async function deleteConfig(configId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/v1/config/${encodeURIComponent(configId)}`, {
    method: "DELETE",
  })

  if (!res.ok) {
    throw new Error(await errorMessageFrom(res))
  }
}

export const api = { get, healthCheck, postConfig, startRun, deleteConfig }
