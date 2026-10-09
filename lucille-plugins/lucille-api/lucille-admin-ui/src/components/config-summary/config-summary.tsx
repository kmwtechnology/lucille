import { Activity, Layers, Zap } from "lucide-react"
import type { ConfigObject } from "@/types/api"
import styles from "./config-summary.module.css"

/** Returns the last segment of a fully-qualified class name, or the input unchanged. */
export function shortClassName(className?: string): string {
  if (!className) return ""
  const parts = className.split(".")
  return parts[parts.length - 1]
}

/** Human-friendly label for a config's indexer (type, short class name, or fallback). */
export function indexerLabel(config: ConfigObject): string {
  const indexer = config.indexer
  if (!indexer) return "None"
  if (typeof indexer.type === "string" && indexer.type) return indexer.type
  if (typeof indexer.class === "string" && indexer.class) return shortClassName(indexer.class)
  return "Custom"
}

/** Summary chips (connectors / pipelines / indexer) shared by the list and detail views. */
export function ConfigChips({ config }: { config: ConfigObject }) {
  const connectorCount = config.connectors?.length ?? 0
  const pipelineCount = config.pipelines?.length ?? 0

  return (
    <div className={styles.summaryRow}>
      <span className={styles.summaryChip}>
        <Zap className="h-3.5 w-3.5" />
        {connectorCount} {connectorCount === 1 ? "Connector" : "Connectors"}
      </span>
      <span className={styles.summaryChip}>
        <Layers className="h-3.5 w-3.5" />
        {pipelineCount} {pipelineCount === 1 ? "Pipeline" : "Pipelines"}
      </span>
      <span className={styles.summaryChip}>
        <Activity className="h-3.5 w-3.5" />
        {indexerLabel(config)}
      </span>
    </div>
  )
}
