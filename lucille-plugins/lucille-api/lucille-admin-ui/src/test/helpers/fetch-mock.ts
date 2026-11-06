import { vi } from "vitest"

export interface MockResponse {
  status?: number
  statusText?: string
  json?: unknown
  /** Raw body; takes precedence over `json`. */
  body?: string
}

/** Static response or a function of the request. Route keys match as a URL substring. */
export type RouteHandler = MockResponse | ((url: string, init?: RequestInit) => MockResponse)

function toResponse(mock: MockResponse): Response {
  const body = mock.body ?? (mock.json !== undefined ? JSON.stringify(mock.json) : "")
  return new Response(body, {
    status: mock.status ?? 200,
    statusText: mock.statusText ?? "",
    headers: { "Content-Type": "application/json" },
  })
}

/** Stub global `fetch`, routing each request to the first route whose key is a URL substring. Unmatched URLs throw. */
export function mockFetch(routes: Record<string, RouteHandler>) {
  const entries = Object.entries(routes)

  const fn = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input.toString()
    const match = entries.find(([key]) => url.includes(key))
    if (!match) {
      throw new Error(`mockFetch: no route matched ${url}. Known routes: ${entries.map(([k]) => k).join(", ")}`)
    }
    const handler = match[1]
    const mock = typeof handler === "function" ? handler(url, init) : handler
    return toResponse(mock)
  })

  vi.stubGlobal("fetch", fn)
  return fn
}

/** Stub global `fetch` to reject every request, simulating a down API. */
export function mockFetchReject(message = "network down") {
  const fn = vi.fn(async () => {
    throw new Error(message)
  })
  vi.stubGlobal("fetch", fn)
  return fn
}
