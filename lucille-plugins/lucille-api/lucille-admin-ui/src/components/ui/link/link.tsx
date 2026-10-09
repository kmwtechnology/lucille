import { Link as RouterLink } from "react-router-dom"
import { cn } from "@/lib/utils"
import styles from "./link.module.css"
import type { LinkVariant, LinkSize, LinkProps } from "./link.types"

const variantStyles: Record<LinkVariant, string | undefined> = {
  default: styles.default,
  destructive: styles.destructive,
  outline: styles.outline,
  secondary: styles.secondary,
  ghost: styles.ghost,
  link: styles.link,
}

const sizeStyles: Record<LinkSize, string | undefined> = {
  default: styles.sizeDefault,
  sm: styles.sizeSm,
  lg: styles.sizeLg,
  icon: styles.sizeIcon,
}

function Link({ className, variant = "default", size = "default", ...props }: LinkProps) {
  return (
    <RouterLink
      className={cn(styles.base, variantStyles[variant], sizeStyles[size], className)}
      {...props}
    />
  )
}

export { Link }
