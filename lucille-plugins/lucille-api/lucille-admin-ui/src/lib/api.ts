import { errorMessageFrom } from "@/lib/utils"
import { API_BASE } from "@/lib/constants"
import { authHeader, notifyUnauthorized } from "@/lib/auth-token"

/**
 * Fetches from the API with the bearer token attached. Notifies the auth layer on a 401 and throws
 * the server's error message on any non-2xx response.
 */
async function request(path: string, init: RequestInit = {}): Promise<Response> {
  const res = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: { ...authHeader(), ...(init.headers ?? {}) },
  })

  if (res.status === 401) {
    notifyUnauthorized()
  }
  if (!res.ok) {
    throw new Error(await errorMessageFrom(res))
  }
  return res
}

/**
 * Authenticate against POST /v1/login and return a bearer token.
 *
 * @param password the shared admin password
 * @returns the issued bearer token
 * @throws Error with the server's message on invalid credentials
 */
async function login(password: string): Promise<string> {
  // bypasses request() so a failed login shows its error instead of triggering a redirect
  const res = await fetch(`${API_BASE}/v1/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ password }),
  })

  if (!res.ok) {
    throw new Error(await errorMessageFrom(res))
  }

  const data: { token?: string } = await res.json()
  if (!data.token) {
    throw new Error("Server did not return a token.")
  }
  return data.token
}

/**
 * POST a Lucille config to /v1/config as a raw HOCON string.
 *
 * The body is sent as-is with Content-Type application/hocon, so HOCON features
 * (comments, ${?ENV} substitutions, unquoted keys) are preserved and parsed by
 * the server. JSON is also valid HOCON, so plain JSON still works.
 *
 * @param body the raw HOCON (or JSON) configuration text
 * @returns the created config's UUID
 * @throws Error with the server's message on a non-2xx response
 */
async function postConfig(body: string): Promise<string> {
  const res = await request("/v1/config", {
    method: "POST",
    headers: { "Content-Type": "application/hocon" },
    body,
  })

  const data: { configId?: string } = await res.json()
  if (!data.configId) {
    throw new Error("Server did not return a configId.")
  }
  return data.configId
}

/**
 * Start a run for an existing config via POST /v1/run.
 *
 * @returns the new run's ID
 * @throws Error with the server's message on a non-2xx response
 */
async function startRun(configId: string): Promise<string> {
  const res = await request("/v1/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ configId }),
  })

  const data: { runId?: string } = await res.json()
  if (!data.runId) {
    throw new Error("Server did not return a runId.")
  }
  return data.runId
}

/**
 * Delete a config via DELETE /v1/config/{configId}.
 *
 * @throws Error with the server's message on a non-2xx response
 */
async function deleteConfig(configId: string): Promise<void> {
  await request(`/v1/config/${encodeURIComponent(configId)}`, {
    method: "DELETE",
  })
}

export const api = { login, postConfig, startRun, deleteConfig }
