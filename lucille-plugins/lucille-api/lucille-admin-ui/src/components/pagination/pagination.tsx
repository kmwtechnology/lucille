import { ChevronLeft, ChevronRight } from "lucide-react"
import { Button } from "@/components/ui/button/button"
import { PAGE_SIZE_OPTIONS, type Pagination as PaginationState } from "@/hooks/use-pagination"
import styles from "./pagination.module.css"

/** Pagination controls: rows-per-page selector, range summary, and prev/next. */
export function Pagination<T>({ state, label = "rows" }: { state: PaginationState<T>; label?: string }) {
  const { page, pageCount, pageSize, totalItems, startIndex, endIndex, setPage, setPageSize, canPrev, canNext } = state

  return (
    <div className={styles.bar}>
      <div className={styles.left}>
        <label className={styles.sizeLabel}>
          Rows per page
          <select
            className={styles.select}
            value={pageSize}
            onChange={(e) => setPageSize(Number(e.target.value))}
          >
            {PAGE_SIZE_OPTIONS.map((size) => (
              <option key={size} value={size}>
                {size}
              </option>
            ))}
          </select>
        </label>
        <span className={styles.summary}>
          {totalItems === 0 ? `0 ${label}` : `${startIndex}–${endIndex} of ${totalItems} ${label}`}
        </span>
      </div>

      <div className={styles.right}>
        <Button
          variant="outline"
          size="sm"
          onClick={() => setPage(page - 1)}
          disabled={!canPrev}
          aria-label="Previous page"
        >
          <ChevronLeft className="h-4 w-4" />
        </Button>
        <span className={styles.pageInfo}>
          Page {page} of {pageCount}
        </span>
        <Button
          variant="outline"
          size="sm"
          onClick={() => setPage(page + 1)}
          disabled={!canNext}
          aria-label="Next page"
        >
          <ChevronRight className="h-4 w-4" />
        </Button>
      </div>
    </div>
  )
}
