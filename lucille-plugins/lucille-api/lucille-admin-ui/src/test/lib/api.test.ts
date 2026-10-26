import { describe, it, expect } from "vitest"
import { api } from "@/lib/api"
import { mockFetch } from "@/test/helpers/fetch-mock"

describe("api.postConfig", () => {
  it("POSTs the raw body as application/hocon and returns the configId", async () => {
    const fetchMock = mockFetch({ "/v1/config": { json: { configId: "cfg-1" } } })

    const id = await api.postConfig("connectors: []")

    expect(id).toBe("cfg-1")
    const [url, init] = fetchMock.mock.calls[0]!
    expect(url).toBe("/v1/config")
    expect(init?.method).toBe("POST")
    expect((init?.headers as Record<string, string>)["Content-Type"]).toBe("application/hocon")
    expect(init?.body).toBe("connectors: []")
  })

  it("throws the server error message on a non-2xx response", async () => {
    mockFetch({ "/v1/config": { status: 400, json: { message: "bad config" } } })

    await expect(api.postConfig("x")).rejects.toThrow("bad config")
  })

  it("throws when the server returns no configId", async () => {
    mockFetch({ "/v1/config": { json: {} } })

    await expect(api.postConfig("x")).rejects.toThrow("Server did not return a configId.")
  })
})

describe("api.startRun", () => {
  it("POSTs the configId as JSON and returns the runId", async () => {
    const fetchMock = mockFetch({ "/v1/run": { json: { runId: "run-1" } } })

    const id = await api.startRun("cfg-1")

    expect(id).toBe("run-1")
    const [url, init] = fetchMock.mock.calls[0]!
    expect(url).toBe("/v1/run")
    expect(init?.method).toBe("POST")
    expect((init?.headers as Record<string, string>)["Content-Type"]).toBe("application/json")
    expect(JSON.parse(init?.body as string)).toEqual({ configId: "cfg-1" })
  })

  it("throws the server error message on failure", async () => {
    mockFetch({ "/v1/run": { status: 409, json: { message: "already running" } } })

    await expect(api.startRun("cfg-1")).rejects.toThrow("already running")
  })

  it("throws when the server returns no runId", async () => {
    mockFetch({ "/v1/run": { json: {} } })

    await expect(api.startRun("cfg-1")).rejects.toThrow("Server did not return a runId.")
  })
})

describe("api.deleteConfig", () => {
  it("DELETEs the url-encoded config id", async () => {
    const fetchMock = mockFetch({ "/v1/config/": { status: 200 } })

    await expect(api.deleteConfig("a/b c")).resolves.toBeUndefined()

    const [url, init] = fetchMock.mock.calls[0]!
    expect(url).toBe("/v1/config/a%2Fb%20c")
    expect(init?.method).toBe("DELETE")
  })

  it("throws the server error message on a 404", async () => {
    mockFetch({ "/v1/config/": { status: 404, json: { message: "not found" } } })

    await expect(api.deleteConfig("missing")).rejects.toThrow("not found")
  })
})
