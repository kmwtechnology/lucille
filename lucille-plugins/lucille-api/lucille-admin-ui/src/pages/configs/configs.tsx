import { Link } from "@/components/ui/link/link"
import {
  Card,
  CardContent,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card/card"
import { Settings } from "lucide-react"
import { useFetch } from "@/hooks/use-fetch"
import type { ConfigMap, ConfigObject } from "@/types/api"
import { ConfigChips } from "@/components/config-summary/config-summary"
import { LoadingText } from "@/components/loading-text/loading-text"
import { ErrorState } from "@/components/error-state/error-state"
import { EmptyState } from "@/components/empty-state/empty-state"
import { ViewLink } from "@/components/view-link/view-link"
import styles from "./configs.module.css"

function CreateConfigLink() {
  return (
    <Link className={styles.createButton} to="/configs/create">
      <Settings className="mr-2 h-4 w-4 flex-shrink-0" /> Create New Configuration
    </Link>
  )
}

function names(items: Array<{ name?: unknown }> | undefined, fallbackPrefix: string): string[] {
  return (items ?? []).map((item, i) =>
    typeof item.name === "string" && item.name ? item.name : `${fallbackPrefix} ${i + 1}`,
  )
}

function NameRow({ label, values }: { label: string; values: string[] }) {
  return (
    <div className={styles.nameRow}>
      <span className={styles.nameLabel}>{label}</span>
      {values.length === 0 ? (
        <span className={styles.nameEmpty}>None</span>
      ) : (
        <div className={styles.nameTags}>
          {values.map((name, i) => (
            <span key={`${name}-${i}`} className={styles.nameTag} title={name}>
              {name}
            </span>
          ))}
        </div>
      )}
    </div>
  )
}

function ConfigCard({ id, config }: { id: string; config: ConfigObject }) {
  const connectorNames = names(config.connectors, "connector")
  const pipelineNames = names(config.pipelines, "pipeline")

  return (
    <Card className={styles.configCard}>
      <CardHeader className={styles.configHeader}>
        <CardTitle className={styles.configTitle}>
          <Settings className="h-5 w-5 text-primary-600 flex-shrink-0" />
          <span className={styles.configId} title={id}>
            {id}
          </span>
        </CardTitle>
      </CardHeader>
      <CardContent className={styles.configContent}>
        <ConfigChips config={config} />
        <div className={styles.nameList}>
          <NameRow label="Connectors" values={connectorNames} />
          <NameRow label="Pipelines" values={pipelineNames} />
        </div>
      </CardContent>
      <CardFooter>
        <ViewLink to={`/configs/detail?id=${encodeURIComponent(id)}`} label="View" />
      </CardFooter>
    </Card>
  )
}

export default function Configs() {
  const configs = useFetch<ConfigMap>("/v1/config")

  return (
    <div className={styles.page}>
      <div className={styles.pageHeader}>
        <div>
          <h1 className={styles.pageTitle}>Configurations</h1>
          <p className={styles.pageSubtitle}>
            Manage your Lucille pipeline configurations
          </p>
        </div>
        <CreateConfigLink />
      </div>

      {configs.status === "loading" ? (
        <LoadingText />
      ) : configs.status === "error" ? (
        <ErrorState title="An error has occurred" subtext="Unable to load configuration data" />
      ) : Object.keys(configs.data).length === 0 ? (
        <EmptyState
          icon={Settings}
          title="No configurations yet"
          subtext="Create your first configuration to get started."
          action={<CreateConfigLink />}
        />
      ) : (
        <div className={styles.configGrid}>
          {Object.entries(configs.data).map(([id, config]) => (
            <ConfigCard key={id} id={id} config={config} />
          ))}
        </div>
      )}
    </div>
  )
}
