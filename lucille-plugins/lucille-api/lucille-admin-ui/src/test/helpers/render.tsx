import type { ReactElement, ReactNode } from "react"
import { render, type RenderResult } from "@testing-library/react"
import { MemoryRouter, Route, Routes } from "react-router-dom"

export interface RenderWithRouterOptions {
  /** Initial URL the router starts at (default "/"). */
  route?: string
  /** Route pattern to mount under, e.g. "/configs/:id". Required for components using useParams. */
  path?: string
}

/** Render inside a MemoryRouter so router hooks and <Link> work. Pass path + route to drive useParams. */
export function renderWithRouter(
  ui: ReactElement,
  { route = "/", path }: RenderWithRouterOptions = {},
): RenderResult {
  if (path) {
    return render(
      <MemoryRouter initialEntries={[route]}>
        <Routes>
          <Route path={path} element={ui} />
          <Route path="*" element={<div data-testid="router-elsewhere" />} />
        </Routes>
      </MemoryRouter>,
    )
  }

  return render(<MemoryRouter initialEntries={[route]}>{ui}</MemoryRouter>)
}

/** MemoryRouter wrapper for use as an RTL `wrapper` option. */
export function RouterWrapper({ children }: { children: ReactNode }) {
  return <MemoryRouter>{children}</MemoryRouter>
}
