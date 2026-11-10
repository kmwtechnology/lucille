// Tests the usePagination hook: page slicing, index ranges, page clamping, page-size changes, and empty lists.
import { describe, it, expect } from "vitest"
import { act, renderHook } from "@testing-library/react"
import { usePagination } from "@/hooks/use-pagination"

const items = (n: number) => Array.from({ length: n }, (_, i) => i + 1)

describe("usePagination", () => {
  // First page: correct page count, item slice, 1-based range, and nav flags.
  it("paginates with the default page size", () => {
    const { result } = renderHook(() => usePagination(items(25), 10))

    expect(result.current.pageCount).toBe(3)
    expect(result.current.totalItems).toBe(25)
    expect(result.current.page).toBe(1)
    expect(result.current.pageItems).toEqual(items(10))
    expect(result.current.startIndex).toBe(1)
    expect(result.current.endIndex).toBe(10)
    expect(result.current.canPrev).toBe(false)
    expect(result.current.canNext).toBe(true)
  })

  // Moving to page 2 returns the next slice and shifts the displayed range.
  it("moves between pages and computes the index range", () => {
    const { result } = renderHook(() => usePagination(items(25), 10))

    act(() => result.current.setPage(2))

    expect(result.current.page).toBe(2)
    expect(result.current.pageItems).toEqual([11, 12, 13, 14, 15, 16, 17, 18, 19, 20])
    expect(result.current.startIndex).toBe(11)
    expect(result.current.endIndex).toBe(20)
    expect(result.current.canPrev).toBe(true)
    expect(result.current.canNext).toBe(true)
  })

  // A page past the end is clamped to the last page (partial final slice).
  it("clamps setPage above pageCount to the last page", () => {
    const { result } = renderHook(() => usePagination(items(25), 10))

    act(() => result.current.setPage(99))

    expect(result.current.page).toBe(3)
    expect(result.current.pageItems).toEqual([21, 22, 23, 24, 25])
    expect(result.current.endIndex).toBe(25)
    expect(result.current.canNext).toBe(false)
  })

  // A page below 1 is clamped to the first page.
  it("clamps setPage below 1 to the first page", () => {
    const { result } = renderHook(() => usePagination(items(25), 10))

    act(() => result.current.setPage(0))

    expect(result.current.page).toBe(1)
  })

  // Changing page size recomputes pages and jumps back to page 1.
  it("resets to page 1 when the page size changes", () => {
    const { result } = renderHook(() => usePagination(items(100), 10))

    act(() => result.current.setPage(5))
    expect(result.current.page).toBe(5)

    act(() => result.current.setPageSize(25))

    expect(result.current.pageSize).toBe(25)
    expect(result.current.page).toBe(1)
    expect(result.current.pageCount).toBe(4)
    expect(result.current.pageItems).toHaveLength(25)
  })

  // An empty list still reports one page, zero items, and both nav flags off.
  it("handles an empty list", () => {
    const { result } = renderHook(() => usePagination<number>([], 10))

    expect(result.current.pageCount).toBe(1)
    expect(result.current.totalItems).toBe(0)
    expect(result.current.pageItems).toEqual([])
    expect(result.current.startIndex).toBe(0)
    expect(result.current.endIndex).toBe(0)
    expect(result.current.canPrev).toBe(false)
    expect(result.current.canNext).toBe(false)
  })
})
