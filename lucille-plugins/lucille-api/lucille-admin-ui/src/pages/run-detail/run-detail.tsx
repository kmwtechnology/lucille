import { useState } from "react"
import { useNavigate, useSearchParams } from "react-router-dom"
import { Link } from "@/components/ui/link/link"
import { Button } from "@/components/ui/button/button"
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card/card"
import { Play, Settings } from "lucide-react"
import { useFetch } from "@/hooks/use-fetch"
import { useStartRun } from "@/hooks/use-start-run"
import { formatDuration, formatInstant } from "@/lib/utils"
import type { ConfigMap, Run } from "@/types/api"
import { RunStatusBadge } from "@/components/run-status/run-status"
import { BackLink } from "@/components/back-link/back-link"
import { LoadingText } from "@/components/loading-text/loading-text"
import { ErrorState } from "@/components/error-state/error-state"
import styles from "./run-detail.module.css"

function StartRun() {
  const navigate = useNavigate()
  const configs = useFetch<ConfigMap>("/v1/config")
  const { state, submit } = useStartRun()
  const [configId, setConfigId] = useState("")

  const submitting = state.status === "submitting"
  const configIds = configs.status === "success" ? Object.keys(configs.data) : []

  const handleSubmit = async () => {
    if (!configId) return
    const runId = await submit(configId)
    if (runId) {
      navigate(`/runs/detail?id=${encodeURIComponent(runId)}`)
    }
  }

  return (
    <div className={styles.page}>
      <BackLink to="/runs" label="Back to Runs" />

      <div className={styles.detailHeader}>
        <div className={styles.titleRow}>
          <Play className="h-6 w-6 text-primary-600 flex-shrink-0" />
          <h1 className={styles.title}>Start Run</h1>
        </div>
        <p className={styles.subtext}>
          Select an existing configuration to run. A run ID will be generated.
        </p>
      </div>

      <Card className={styles.sectionCard}>
        <CardContent className={styles.formContent}>
          <label htmlFor="config-select" className={styles.formLabel}>
            Configuration
          </label>

          {configs.status === "loading" ? (
            <LoadingText>Loading configurations...</LoadingText>
          ) : configs.status === "error" ? (
            <div className={styles.formError}>Unable to load configurations.</div>
          ) : configIds.length === 0 ? (
            <div className={styles.emptyConfigs}>
              <p className={styles.subtext}>No configurations available.</p>
              <Link variant="outline" size="sm" to="/configs/create">
                <Settings className="mr-2 h-4 w-4" /> Create a Configuration
              </Link>
            </div>
          ) : (
            <select
              id="config-select"
              className={styles.select}
              value={configId}
              onChange={(e) => setConfigId(e.target.value)}
              disabled={submitting}
            >
              <option value="">Select a configuration…</option>
              {configIds.map((id) => (
                <option key={id} value={id}>
                  {id}
                </option>
              ))}
            </select>
          )}

          {state.status === "error" && (
            <div className={styles.formError}>{state.error}</div>
          )}

          <div className={styles.formActions}>
            <Button
              variant="default"
              onClick={handleSubmit}
              disabled={submitting || !configId}
            >
              <Play className="mr-2 h-4 w-4" />
              {submitting ? "Starting..." : "Start Run"}
            </Button>
            <Link variant="outline" size="default" to="/runs">
              Cancel
            </Link>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function RunView({ id }: { id: string }) {
  const run = useFetch<Run>(`/v1/run/${encodeURIComponent(id)}`, 3000)

  return (
    <div className={styles.page}>
      <BackLink to="/runs" label="Back to Runs" />

      {run.status === "loading" ? (
        <LoadingText />
      ) : run.status === "error" ? (
        <ErrorState title="Run not found" subtext={`Unable to load run ${id} (${run.error})`} />
      ) : (
        <>
          <div className={styles.detailHeader}>
            <div className={styles.titleRow}>
              <Play className="h-6 w-6 text-primary-600 flex-shrink-0" />
              <h1 className={styles.runId} title={run.data.runId}>{run.data.runId}</h1>
              <RunStatusBadge run={run.data} />
            </div>
          </div>

          <Card className={styles.sectionCard}>
            <CardHeader className={styles.sectionHeader}>
              <CardTitle className={styles.sectionTitle}>Details</CardTitle>
            </CardHeader>
            <CardContent className={styles.kvList}>
              <div className={styles.kvRow}>
                <span className={styles.kvKey}>Config</span>
                <Link
                  variant="link"
                  size="sm"
                  className={styles.configLink}
                  to={`/configs/detail?id=${encodeURIComponent(run.data.configId)}`}
                >
                  {run.data.configId}
                </Link>
              </div>
              <div className={styles.kvRow}>
                <span className={styles.kvKey}>Started</span>
                <span className={styles.kvValue}>{formatInstant(run.data.startTime)}</span>
              </div>
              <div className={styles.kvRow}>
                <span className={styles.kvKey}>Ended</span>
                <span className={styles.kvValue}>{formatInstant(run.data.endTime)}</span>
              </div>
              <div className={styles.kvRow}>
                <span className={styles.kvKey}>Duration</span>
                <span className={styles.kvValue}>
                  {run.data.done ? formatDuration(run.data.startTime, run.data.endTime) : "In progress"}
                </span>
              </div>
              {run.data.runType && (
                <div className={styles.kvRow}>
                  <span className={styles.kvKey}>Type</span>
                  <span className={styles.kvValue}>{run.data.runType}</span>
                </div>
              )}
            </CardContent>
          </Card>

          {run.data.runResult && (
            <Card className={styles.sectionCard}>
              <CardHeader className={styles.sectionHeader}>
                <CardTitle className={styles.sectionTitle}>Result</CardTitle>
              </CardHeader>
              <CardContent>
                <pre className={styles.resultPre}>{run.data.runResult.message}</pre>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  )
}

export default function RunDetail() {
  const [searchParams] = useSearchParams()
  const id = searchParams.get("id")

  if (!id || id === "new") {
    return <StartRun />
  }

  return <RunView id={id} />
}
