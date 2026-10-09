import { useState } from "react"
import { useNavigate } from "react-router-dom"
import { Link } from "@/components/ui/link/link"
import { Button } from "@/components/ui/button/button"
import { Link as LinkIcon, Play, Settings, Trash2 } from "lucide-react"
import { useFetch } from "@/hooks/use-fetch"
import { useStartRun } from "@/hooks/use-start-run"
import { useDeleteConfig } from "@/hooks/use-delete-config"
import type { ConfigMap, ConfigObject } from "@/types/api"
import { indexerLabel } from "@/components/config-summary/config-summary"
import { LoadingText } from "@/components/loading-text/loading-text"
import { ErrorState } from "@/components/error-state/error-state"
import { EmptyState } from "@/components/empty-state/empty-state"
import { Pagination } from "@/components/pagination/pagination"
import { usePagination } from "@/hooks/use-pagination"
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

function NameTags({ values }: { values: string[] }) {
  if (values.length === 0) {
    return <span className={styles.cellEmpty}>None</span>
  }
  return (
    <div className={styles.tags}>
      {values.map((name, i) => (
        <span key={`${name}-${i}`} className={styles.tag} title={name}>
          {name}
        </span>
      ))}
    </div>
  )
}

function RowStartRunButton({ configId }: { configId: string }) {
  const navigate = useNavigate()
  const { state, submit } = useStartRun()
  const submitting = state.status === "submitting"

  const handleStart = async (e: React.MouseEvent) => {
    e.stopPropagation()
    const runId = await submit(configId)
    if (runId) {
      navigate(`/runs/detail?id=${encodeURIComponent(runId)}`)
    }
  }

  return (
    <div className={styles.actionCell}>
      <Button variant="default" size="sm" onClick={handleStart} disabled={submitting}>
        <Play className="mr-1.5 h-3.5 w-3.5" />
        {submitting ? "Starting..." : "Start Run"}
      </Button>
      {state.status === "error" && (
        <span className={styles.actionError} title={state.error}>
          Failed
        </span>
      )}
    </div>
  )
}

function RowDeleteButton({ configId, onDeleted }: { configId: string; onDeleted: () => void }) {
  const { state, remove } = useDeleteConfig()
  const deleting = state.status === "deleting"

  const handleDelete = async (e: React.MouseEvent) => {
    e.stopPropagation()
    const confirmed = window.confirm(`Delete configuration ${configId}? This cannot be undone.`)
    if (!confirmed) return
    const ok = await remove(configId)
    if (ok) onDeleted()
  }

  return (
    <div className={styles.actionCell}>
      <Button
        variant="destructive"
        size="sm"
        onClick={handleDelete}
        disabled={deleting}
        aria-label={`Delete configuration ${configId}`}
      >
        <Trash2 className="mr-1.5 h-3.5 w-3.5" />
        {deleting ? "Deleting..." : "Delete"}
      </Button>
      {state.status === "error" && (
        <span className={styles.actionError} title={state.error}>
          Failed
        </span>
      )}
    </div>
  )
}

function ConfigRow({ id, config, onDeleted }: { id: string; config: ConfigObject; onDeleted: () => void }) {
  const navigate = useNavigate()
  const to = `/configs/detail?id=${encodeURIComponent(id)}`

  return (
    <tr
      className={styles.row}
      onClick={() => navigate(to)}
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === "Enter") navigate(to)
      }}
    >
      <td className={styles.idCell}>
        <span className={styles.idWrap}>
          <LinkIcon className={styles.idIcon} aria-hidden="true" />
          <span className={styles.rowId} title={id}>
            {id}
          </span>
        </span>
      </td>
      <td>
        <NameTags values={names(config.connectors, "connector")} />
      </td>
      <td>
        <NameTags values={names(config.pipelines, "pipeline")} />
      </td>
      <td className={styles.indexerCell}>{indexerLabel(config)}</td>
      <td className={styles.actionTd} onClick={(e) => e.stopPropagation()}>
        <div className={styles.actions}>
          <RowStartRunButton configId={id} />
          <RowDeleteButton configId={id} onDeleted={onDeleted} />
        </div>
      </td>
    </tr>
  )
}

export default function Configs() {
  const [reloadKey, setReloadKey] = useState(0)
  const configs = useFetch<ConfigMap>("/v1/config", undefined, reloadKey)
  const entries = configs.status === "success" ? Object.entries(configs.data) : []
  const pagination = usePagination(entries, 10)

  const refresh = () => setReloadKey((k) => k + 1)

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
      ) : entries.length === 0 ? (
        <EmptyState
          icon={Settings}
          title="No configurations yet"
          subtext="Create your first configuration to get started."
          action={<CreateConfigLink />}
        />
      ) : (
        <>
          <div className={styles.tableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th className={styles.th}>Config ID</th>
                  <th className={styles.th}>Connectors</th>
                  <th className={styles.th}>Pipelines</th>
                  <th className={styles.th}>Indexer</th>
                  <th className={styles.th}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {pagination.pageItems.map(([id, config]) => (
                  <ConfigRow key={id} id={id} config={config} onDeleted={refresh} />
                ))}
              </tbody>
            </table>
          </div>
          <Pagination state={pagination} label="configs" />
        </>
      )}
    </div>
  )
}
