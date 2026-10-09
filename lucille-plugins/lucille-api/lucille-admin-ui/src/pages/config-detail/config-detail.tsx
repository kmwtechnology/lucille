import { useState } from "react"
import { Navigate, useNavigate, useSearchParams } from "react-router-dom"
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card/card"
import { Activity, ChevronDown, Layers, Play, Settings, Zap } from "lucide-react"
import { cn } from "@/lib/utils"
import { useFetch } from "@/hooks/use-fetch"
import { useStartRun } from "@/hooks/use-start-run"
import type { ConfigObject } from "@/types/api"
import { Button } from "@/components/ui/button/button"
import { BackLink } from "@/components/back-link/back-link"
import { CopyButton } from "@/components/copy-button/copy-button"
import { ConfigChips, shortClassName } from "@/components/config-summary/config-summary"
import { LoadingText } from "@/components/loading-text/loading-text"
import { ErrorState } from "@/components/error-state/error-state"
import styles from "./config-detail.module.css"

function StartRunButton({ configId }: { configId: string }) {
  const navigate = useNavigate()
  const { state, submit } = useStartRun()
  const submitting = state.status === "submitting"

  const handleStart = async () => {
    const runId = await submit(configId)
    if (runId) {
      navigate(`/runs/detail?id=${encodeURIComponent(runId)}`)
    }
  }

  return (
    <>
      <Button variant="default" size="sm" onClick={handleStart} disabled={submitting}>
        <Play className="mr-2 h-4 w-4" />
        {submitting ? "Starting..." : "Start Run"}
      </Button>
      {state.status === "error" && (
        <span className={styles.startError}>{state.error}</span>
      )}
    </>
  )
}

function ConnectorsCard({ config }: { config: ConfigObject }) {
  const connectors = config.connectors ?? []
  return (
    <Card className={styles.sectionCard}>
      <CardHeader className={styles.sectionHeader}>
        <CardTitle className={styles.sectionTitle}>
          <Zap className="h-5 w-5 text-primary-600 flex-shrink-0" />
          <span>Connectors ({connectors.length})</span>
        </CardTitle>
      </CardHeader>
      <CardContent>
        {connectors.length === 0 ? (
          <div className={styles.emptyRow}>No connectors defined</div>
        ) : (
          <ul className={styles.itemList}>
            {connectors.map((connector, i) => {
              const name = typeof connector.name === "string" ? connector.name : `connector ${i + 1}`
              const cls = typeof connector.class === "string" ? connector.class : undefined
              const pipeline = typeof connector.pipeline === "string" ? connector.pipeline : undefined
              return (
                <li key={i} className={styles.item}>
                  <span className={styles.itemName}>{name}</span>
                  {cls && <span className={styles.itemClass}>{shortClassName(cls)}</span>}
                  {pipeline && (
                    <span className={styles.itemMeta}>→ {pipeline}</span>
                  )}
                </li>
              )
            })}
          </ul>
        )}
      </CardContent>
    </Card>
  )
}

function PipelinesCard({ config }: { config: ConfigObject }) {
  const pipelines = config.pipelines ?? []
  return (
    <Card className={styles.sectionCard}>
      <CardHeader className={styles.sectionHeader}>
        <CardTitle className={styles.sectionTitle}>
          <Layers className="h-5 w-5 text-primary-600 flex-shrink-0" />
          <span>Pipelines ({pipelines.length})</span>
        </CardTitle>
      </CardHeader>
      <CardContent>
        {pipelines.length === 0 ? (
          <div className={styles.emptyRow}>No pipelines defined</div>
        ) : (
          <ul className={styles.pipelineList}>
            {pipelines.map((pipeline, i) => (
              <PipelineItem key={i} pipeline={pipeline} index={i} />
            ))}
          </ul>
        )}
      </CardContent>
    </Card>
  )
}

type Pipeline = NonNullable<ConfigObject["pipelines"]>[number]

function PipelineItem({ pipeline, index }: { pipeline: Pipeline; index: number }) {
  const stages = pipeline.stages ?? []
  const [open, setOpen] = useState(false)
  const name = pipeline.name ?? `pipeline ${index + 1}`

  return (
    <li className={styles.pipelineItem}>
      <button
        className={styles.pipelineToggle}
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        <ChevronDown className={cn(styles.chevron, open && styles.chevronOpen)} />
        <span className={styles.itemName}>{name}</span>
        <span className={styles.stageCount}>
          {stages.length} {stages.length === 1 ? "stage" : "stages"}
        </span>
      </button>
      {open &&
        (stages.length === 0 ? (
          <div className={styles.emptyRow}>No stages</div>
        ) : (
          <ol className={styles.stageList}>
            {stages.map((stage, j) => {
              const stageName = typeof stage.name === "string" ? stage.name : undefined
              const stageClass = typeof stage.class === "string" ? stage.class : undefined
              return (
                <li key={j} className={styles.stageItem}>
                  <span className={styles.stageIndex}>{j + 1}.</span>
                  {stageName && <span className={styles.itemName}>{stageName}</span>}
                  {stageClass && (
                    <span className={styles.itemClass}>{shortClassName(stageClass)}</span>
                  )}
                </li>
              )
            })}
          </ol>
        ))}
    </li>
  )
}

const CONNECTION_KEYS = ["solr", "elastic", "opensearch", "csv"] as const

function IndexerCard({ config }: { config: ConfigObject }) {
  const indexer = config.indexer
  const connectionKey = CONNECTION_KEYS.find((k) => config[k] != null)
  const connection = connectionKey ? (config[connectionKey] as Record<string, unknown>) : undefined

  return (
    <Card className={styles.sectionCard}>
      <CardHeader className={styles.sectionHeader}>
        <CardTitle className={styles.sectionTitle}>
          <Activity className="h-5 w-5 text-primary-600 flex-shrink-0" />
          <span>Indexer</span>
        </CardTitle>
      </CardHeader>
      <CardContent>
        {!indexer ? (
          <div className={styles.emptyRow}>No indexer defined</div>
        ) : (
          <div className={styles.kvList}>
            {typeof indexer.type === "string" && (
              <div className={styles.kvRow}>
                <span className={styles.kvKey}>Type</span>
                <span className={styles.kvValue}>{indexer.type}</span>
              </div>
            )}
            {typeof indexer.class === "string" && (
              <div className={styles.kvRow}>
                <span className={styles.kvKey}>Class</span>
                <span className={styles.kvValue}>{shortClassName(indexer.class)}</span>
              </div>
            )}
            {connectionKey && connection && (
              <>
                <div className={styles.kvSubheading}>{connectionKey}</div>
                {Object.entries(connection).map(([k, v]) => (
                  <div key={k} className={styles.kvRow}>
                    <span className={styles.kvKey}>{k}</span>
                    <span className={styles.kvValue}>
                      {typeof v === "object" ? JSON.stringify(v) : String(v)}
                    </span>
                  </div>
                ))}
              </>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function RawConfig({ config }: { config: ConfigObject }) {
  const [open, setOpen] = useState(true)
  const json = JSON.stringify(config, null, 2)

  return (
    <Card className={styles.sectionCard}>
      <CardHeader className={styles.rawHeader}>
        <button className={styles.rawToggle} onClick={() => setOpen((v) => !v)}>
          <ChevronDown className={cn(styles.chevron, open && styles.chevronOpen)} />
          <span>Raw Configuration</span>
        </button>
        <CopyButton value={json} label="Copy raw configuration" />
      </CardHeader>
      {open && (
        <CardContent>
          <pre className={styles.rawPre}>{json}</pre>
        </CardContent>
      )}
    </Card>
  )
}

export default function ConfigDetail() {
  const [searchParams] = useSearchParams()
  const id = searchParams.get("id")

  if (!id || id === "new") {
    return <Navigate to="/configs/create" replace />
  }

  return <ConfigDetailView id={id} />
}

function ConfigDetailView({ id }: { id: string }) {
  const config = useFetch<ConfigObject>(`/v1/config/${encodeURIComponent(id)}`)

  return (
    <div className={styles.page}>
      <BackLink to="/configs" label="Back to Configurations" />

      {config.status === "loading" ? (
        <LoadingText />
      ) : config.status === "error" ? (
        <ErrorState
          title="Configuration not found"
          subtext={`Unable to load configuration ${id} (${config.error})`}
        />
      ) : (
        <>
          <div className={styles.detailHeader}>
            <div className={styles.titleRow}>
              <Settings className="h-6 w-6 text-primary-600 flex-shrink-0" />
              <h1 className={styles.configId} title={id}>{id}</h1>
              <CopyButton value={id} label="Copy configuration ID" />
              <div className={styles.headerActions}>
                <StartRunButton configId={id} />
              </div>
            </div>
            <ConfigChips config={config.data} />
          </div>

          <div className={styles.sectionGrid}>
            <ConnectorsCard config={config.data} />
            <PipelinesCard config={config.data} />
            <IndexerCard config={config.data} />
          </div>

          <RawConfig config={config.data} />
        </>
      )}
    </div>
  )
}
