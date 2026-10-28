import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { renderHook, waitFor, act } from "@testing-library/react"
import { useFetch } from "@/hooks/use-fetch"
import { mockFetch } from "@/test/helpers/fetch-mock"

describe("useFetch", () => {
  it("starts in the loading state", () => {
    mockFetch({ "/v1/thing": { json: { ok: true } } })
    const { result } = renderHook(() => useFetch("/v1/thing"))
    expect(result.current.status).toBe("loading")
  })

  it("resolves to success with the parsed data", async () => {
    mockFetch({ "/v1/thing": { json: { value: 42 } } })

    const { result } = renderHook(() => useFetch<{ value: number }>("/v1/thing"))

    await waitFor(() => expect(result.current.status).toBe("success"))
    if (result.current.status === "success") {
      expect(result.current.data).toEqual({ value: 42 })
    }
  })

  it("resolves to error with the server message on a non-2xx response", async () => {
    mockFetch({ "/v1/thing": { status: 500, json: { message: "boom" } } })

    const { result } = renderHook(() => useFetch("/v1/thing"))

    await waitFor(() => expect(result.current.status).toBe("error"))
    if (result.current.status === "error") {
      expect(result.current.error).toBe("boom")
    }
  })

  it("refetches when reloadKey changes", async () => {
    const fetchMock = mockFetch({ "/v1/thing": { json: { v: 1 } } })

    const { result, rerender } = renderHook(({ key }) => useFetch("/v1/thing", { reloadKey: key }), {
      initialProps: { key: 0 },
    })

    await waitFor(() => expect(result.current.status).toBe("success"))
    expect(fetchMock).toHaveBeenCalledTimes(1)

    rerender({ key: 1 })

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
  })
})

describe("useFetch polling", () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())

  it("polls on the interval and stops when stopPollingWhen is satisfied", async () => {
    let calls = 0
    const fetchMock = mockFetch({
      "/v1/run": () => {
        calls += 1
        return { json: { done: calls >= 2 } }
      },
    })

    const { result } = renderHook(() =>
      useFetch<{ done: boolean }>("/v1/run", {
        pollIntervalMs: 1000,
        stopPollingWhen: (d) => d.done,
      }),
    )

    await vi.waitFor(() => expect(result.current.status).toBe("success"))
    expect(fetchMock).toHaveBeenCalledTimes(1)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000)
    })
    expect(fetchMock).toHaveBeenCalledTimes(2)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000)
    })
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })
})
