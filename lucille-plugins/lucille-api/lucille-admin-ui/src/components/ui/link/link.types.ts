import type { LinkProps as RouterLinkProps } from "react-router-dom"

export type LinkVariant = "default" | "destructive" | "outline" | "secondary" | "ghost" | "link"
export type LinkSize = "default" | "sm" | "lg" | "icon"

export interface LinkProps extends RouterLinkProps {
  variant?: LinkVariant
  size?: LinkSize
}
