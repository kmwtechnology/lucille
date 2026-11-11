// Tests the useCreateConfig hook: idle -> success/error states, the returned configId, and reset.
import { describe, it, expect } from "vitest"
import { act, renderHook } from "@testing-library/react"
import { useCreateConfig } from "@/hooks/use-create-config"
import { mockFetch } from "@/test/helpers/fetch-mock"

describe("useCreateConfig", () => {
  // No submission yet -> idle.
  it("starts idle", () => {
    const { result } = renderHook(() => useCreateConfig())
    expect(result.current.state.status).toBe("idle")
  })

  // A successful submit both resolves with the id and leaves success state.
  it("transitions to success and returns the configId", async () => {
    mockFetch({ "/v1/config": { json: { configId: "cfg-9" } } })
    const { result } = renderHook(() => useCreateConfig())

    let returned: string | null = null
    await act(async () => {
      returned = await result.current.submit("connectors: []")
    })

    expect(returned).toBe("cfg-9")
    expect(result.current.state).toEqual({ status: "success", configId: "cfg-9" })
  })

  // A failed submit resolves with null and surfaces the error in state.
  it("transitions to error and returns null on failure", async () => {
    mockFetch({ "/v1/config": { status: 400, json: { message: "nope" } } })
    const { result } = renderHook(() => useCreateConfig())

    let returned: string | null = "unset"
    await act(async () => {
      returned = await result.current.submit("x")
    })

    expect(returned).toBeNull()
    expect(result.current.state).toEqual({ status: "error", error: "nope" })
  })

  // reset() clears a prior error so the form can be resubmitted.
  it("reset returns to idle", async () => {
    mockFetch({ "/v1/config": { status: 400, json: { message: "nope" } } })
    const { result } = renderHook(() => useCreateConfig())

    await act(async () => {
      await result.current.submit("x")
    })
    expect(result.current.state.status).toBe("error")

    act(() => result.current.reset())
    expect(result.current.state.status).toBe("idle")
  })
})
