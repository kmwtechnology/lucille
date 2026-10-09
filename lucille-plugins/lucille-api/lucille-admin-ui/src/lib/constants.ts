import { BookOpen, Home, Play, Settings } from "lucide-react"

/** External Lucille documentation site, linked from the sidebar. */
export const DOCS_URL = "https://kmwtechnology.github.io/lucille/docs/"

/** Sidebar navigation links. The Documentation entry is an external link. */
export const NAV_LINKS = [
  { href: "/", label: "Dashboard", icon: Home },
  { href: "/configs", label: "Configurations", icon: Settings },
  { href: "/runs", label: "Runs", icon: Play },
  { href: DOCS_URL, label: "Documentation", icon: BookOpen, external: true },
] as const
