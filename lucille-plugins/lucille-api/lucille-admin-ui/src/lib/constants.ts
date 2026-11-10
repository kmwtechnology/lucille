import { BookOpen, Home, Play, Settings } from "lucide-react"

/** Base URL prefix for all API requests. Empty by default (same-origin). */
export const API_BASE = import.meta.env.VITE_API_BASE ?? ""

/** External Lucille documentation site, linked from the sidebar. */
export const DOCS_URL = "https://kmwtechnology.github.io/lucille/docs/"

/** Sidebar navigation links. The Documentation entry is an external link. */
export const NAV_LINKS = [
  { href: "/", label: "Dashboard", icon: Home },
  { href: "/configs", label: "Configurations", icon: Settings },
  { href: "/runs", label: "Runs", icon: Play },
  { href: DOCS_URL, label: "Documentation", icon: BookOpen, external: true },
] as const

/** Poll interval (ms) for refreshing an in-progress run. */
export const RUN_POLL_INTERVAL_MS = 3000


/** Poll interval (ms) for re-checking API health on the dashboard. */
export const HEALTH_POLL_INTERVAL_MS = 5000

/**
 * Max time (ms) a single health probe (livez + readyz) may take before the API is treated as
 * hung and the probe counts as a failure. Kept below the poll interval so probes don't overlap.
 */
export const HEALTH_PROBE_TIMEOUT_MS = 3000
