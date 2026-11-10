import type { LucideIcon } from "lucide-react"
import type { ReactNode } from "react"
import { Card, CardContent } from "@/components/ui/card/card"
import styles from "./empty-state.module.css"

/** Centered empty-state card: an icon, a title, subtext, and an optional action slot. */
export function EmptyState({
  icon: Icon,
  title,
  subtext,
  action,
}: {
  icon: LucideIcon
  title: string
  subtext: string
  action?: ReactNode
}) {
  return (
    <Card className={styles.emptyCard}>
      <CardContent className={styles.emptyContent}>
        <Icon className="h-8 w-8 text-muted-foreground" />
        <div className={styles.emptyTitle}>{title}</div>
        <p className={styles.emptySubtext}>{subtext}</p>
        {action}
      </CardContent>
    </Card>
  )
}
