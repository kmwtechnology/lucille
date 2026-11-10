import { useMemo, useState } from "react"

export const PAGE_SIZE_OPTIONS = [10, 25, 50, 100] as const

export interface Pagination<T> {
  pageItems: T[]
  page: number
  pageSize: number
  pageCount: number
  totalItems: number
  startIndex: number
  endIndex: number
  setPage: (page: number) => void
  setPageSize: (size: number) => void
  canPrev: boolean
  canNext: boolean
}

/**
 * Client-side pagination over an already-fetched array. Clamps the current page
 * when the data length or page size changes so the view never lands out of range.
 */
export function usePagination<T>(items: T[], initialPageSize = 10): Pagination<T> {
  const [page, setPage] = useState(1)
  const [pageSize, setPageSizeState] = useState(initialPageSize)

  const totalItems = items.length
  const pageCount = Math.max(1, Math.ceil(totalItems / pageSize))
  const safePage = Math.min(Math.max(1, page), pageCount)

  const pageItems = useMemo(() => {
    const start = (safePage - 1) * pageSize
    return items.slice(start, start + pageSize)
  }, [items, safePage, pageSize])

  const setPageSize = (size: number) => {
    setPageSizeState(size)
    setPage(1)
  }

  const startIndex = totalItems === 0 ? 0 : (safePage - 1) * pageSize + 1
  const endIndex = Math.min(safePage * pageSize, totalItems)

  return {
    pageItems,
    page: safePage,
    pageSize,
    pageCount,
    totalItems,
    startIndex,
    endIndex,
    setPage: (p: number) => setPage(Math.min(Math.max(1, p), pageCount)),
    setPageSize,
    canPrev: safePage > 1,
    canNext: safePage < pageCount,
  }
}
