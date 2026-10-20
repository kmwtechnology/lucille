import type React from "react"

export type ButtonVariant = "default" | "destructive" | "outline" | "secondary" | "ghost" | "link"
export type ButtonSize = "default" | "sm" | "lg" | "icon"

export interface ButtonProps extends Omit<React.ComponentProps<"button">, "type"> {
  variant?: ButtonVariant
  size?: ButtonSize
}
