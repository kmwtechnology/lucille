// Tests the useDeleteConfig hook: success returns true and resets, failure returns false and sets the error.
import { describe, it, expect } from "vitest"
import { act, renderHook } from "@testing-library/react"
import { useDeleteConfig } from "@/hooks/use-delete-config"
import { mockFetch } from "@/test/helpers/fetch-mock"

describe("useDeleteConfig", () => {
  // No deletion yet -> idle.
  it("starts idle", () => {
    const { result } = renderHook(() => useDeleteConfig())
    expect(result.current.state.status).toBe("idle")
  })

  // A successful delete returns true and leaves no lingering error state.
  it("returns true and returns to idle on success", async () => {
    mockFetch({ "/v1/config/": { status: 200 } })
    const { result } = renderHook(() => useDeleteConfig())

    let ok = false
    await act(async () => {
      ok = await result.current.remove("cfg-1")
    })

    expect(ok).toBe(true)
    expect(result.current.state.status).toBe("idle")
  })

  // A failed delete (e.g. 404) returns false and surfaces the error.
  it("returns false and sets error on failure", async () => {
    mockFetch({ "/v1/config/": { status: 404, json: { message: "gone" } } })
    const { result } = renderHook(() => useDeleteConfig())

    let ok = true
    await act(async () => {
      ok = await result.current.remove("cfg-1")
    })

    expect(ok).toBe(false)
    expect(result.current.state).toEqual({ status: "error", error: "gone" })
  })
})
