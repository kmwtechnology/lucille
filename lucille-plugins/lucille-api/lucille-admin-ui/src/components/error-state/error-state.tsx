import styles from "./error-state.module.css"

/** A two-line error block: a bold red title and a muted red subtext. */
export function ErrorState({ title, subtext }: { title: string; subtext: string }) {
  return (
    <div>
      <div className={styles.errorTitle}>{title}</div>
      <div className={styles.errorSubtext}>{subtext}</div>
    </div>
  )
}
