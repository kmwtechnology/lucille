import styles from "./loading-text.module.css"

/** Muted inline "loading" text used while a fetch is in flight. */
export function LoadingText({ children = "Loading..." }: { children?: string }) {
  return <div className={styles.loadingText}>{children}</div>
}
