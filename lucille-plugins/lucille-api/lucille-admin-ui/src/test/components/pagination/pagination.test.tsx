// Tests the Pagination component: range summary, prev/next enabling, page navigation, and page-size changes.
import { describe, it, expect } from "vitest"
import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { Pagination } from "@/components/pagination/pagination"
import { usePagination } from "@/hooks/use-pagination"

function Harness({ total, initialPageSize = 10 }: { total: number; initialPageSize?: number }) {
  const items = Array.from({ length: total }, (_, i) => i + 1)
  const state = usePagination(items, initialPageSize)
  return (
    <div>
      <Pagination state={state} label="configs" />
      <ul>
        {state.pageItems.map((n) => (
          <li key={n} data-testid="item">
            {n}
          </li>
        ))}
      </ul>
    </div>
  )
}

describe("Pagination", () => {
  // Shows the "x–y of N" range and the current/total page count.
  it("shows the range summary and page info", () => {
    render(<Harness total={25} />)

    expect(screen.getByText("1–10 of 25 configs")).toBeInTheDocument()
    expect(screen.getByText("Page 1 of 3")).toBeInTheDocument()
  })

  // With no items the summary reads "0 ...", not a range.
  it("shows a zero summary when empty", () => {
    render(<Harness total={0} />)
    expect(screen.getByText("0 configs")).toBeInTheDocument()
  })

  // On page 1 there is nothing before, so Previous is disabled.
  it("disables Previous on the first page and enables Next", () => {
    render(<Harness total={25} />)
    expect(screen.getByLabelText("Previous page")).toBeDisabled()
    expect(screen.getByLabelText("Next page")).toBeEnabled()
  })

  // Clicking Next advances the page, updates the summary, and enables Previous.
  it("advances to the next page when Next is clicked", async () => {
    const user = userEvent.setup()
    render(<Harness total={25} />)

    await user.click(screen.getByLabelText("Next page"))

    expect(screen.getByText("11–20 of 25 configs")).toBeInTheDocument()
    expect(screen.getByText("Page 2 of 3")).toBeInTheDocument()
    expect(screen.getByLabelText("Previous page")).toBeEnabled()
  })

  // On the final page Next is disabled and the last (partial) slice shows.
  it("disables Next on the last page", async () => {
    const user = userEvent.setup()
    render(<Harness total={25} />)

    await user.click(screen.getByLabelText("Next page"))
    await user.click(screen.getByLabelText("Next page"))

    expect(screen.getByText("Page 3 of 3")).toBeInTheDocument()
    expect(screen.getByLabelText("Next page")).toBeDisabled()
    expect(screen.getAllByTestId("item")).toHaveLength(5)
  })

  // Picking a new page size recomputes pages and returns to page 1.
  it("changes page size via the select and resets to page 1", async () => {
    const user = userEvent.setup()
    render(<Harness total={100} />)

    await user.click(screen.getByLabelText("Next page"))
    expect(screen.getByText("Page 2 of 10")).toBeInTheDocument()

    await user.selectOptions(screen.getByRole("combobox"), "25")

    expect(screen.getByText("Page 1 of 4")).toBeInTheDocument()
    expect(screen.getByText("1–25 of 100 configs")).toBeInTheDocument()
  })
})
