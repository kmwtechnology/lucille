import { cn } from "@/lib/utils"
import styles from "./button.module.css"
import type { ButtonVariant, ButtonSize, ButtonProps } from "./button.types"

const variantStyles: Record<ButtonVariant, string | undefined> = {
  default: styles.default,
  destructive: styles.destructive,
  outline: styles.outline,
  secondary: styles.secondary,
  ghost: styles.ghost,
  link: styles.link,
}

const sizeStyles: Record<ButtonSize, string | undefined> = {
  default: styles.sizeDefault,
  sm: styles.sizeSm,
  lg: styles.sizeLg,
  icon: styles.sizeIcon,
}

function Button({ className, variant = "default", size = "default", ...props }: ButtonProps) {
  return (
    <button
      type="button"
      className={cn(styles.base, variantStyles[variant], sizeStyles[size], className)}
      {...props}
    />
  )
}

export { Button }
