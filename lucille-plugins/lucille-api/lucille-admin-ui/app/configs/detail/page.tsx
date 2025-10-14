"use client"

import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { ArrowLeft, Play, Save, Check } from "lucide-react"
import JsonEditor from "@/components/json-editor"
import { useState, useEffect } from "react"
import { useSearchParams, useRouter } from "next/navigation"
import type { Run } from "../../../types/run";

import { Suspense } from "react";

export default function ConfigDetailPage() {
  return (
    <Suspense fallback={<div>Loading...</div>}>
      <ConfigDetailPageContent />
    </Suspense>
  );
}

function ConfigDetailPageContent() {
  const searchParams = useSearchParams()
  const router = useRouter()
  const configId = searchParams?.get('id') ?? "new"
  const isNewConfig = configId === "new"

  const [json, setJson] = useState("")
  const [isSaving, setIsSaving] = useState(false)
  const [saveSuccess, setSaveSuccess] = useState(false)
  const [isRunning, setIsRunning] = useState(false)
  const [runSuccess, setRunSuccess] = useState(false)
  const [runError, setRunError] = useState("")
  const [configError, setConfigError] = useState(false)
  const [runsError, setRunsError] = useState(false)



// Helper to format relative time
  function timeAgo(unixSec: number): string {
    const now = Date.now() / 1000
    const diff = now - unixSec
    if (diff < 60) return `${Math.floor(diff)}s ago`
    if (diff < 3600) return `${Math.floor(diff/60)}m ago`
    if (diff < 86400) return `${Math.floor(diff/3600)}h ago`
    return `${Math.floor(diff/86400)}d ago`
  }

  // Recent runs for this config
  const [recentRuns, setRecentRuns] = useState<Run[] | null>(null)
  const [runsLoading, setRunsLoading] = useState<boolean>(true)
  useEffect(() => {
    let active = true
    async function fetchRuns() {
      setRunsLoading(true)
      setRunsError(false)
      try {
        const res = await fetch('http://localhost:8080/v1/run')
        const data = await res.json()
        if (active) setRecentRuns(Array.isArray(data) ? data.filter(r => r.configId === configId) : [])
      } catch (e) {
        console.warn('Failed to fetch runs', e)
        if (active) { setRunsError(true); setRecentRuns([]) }
      } finally {
        if (active) setRunsLoading(false)
      }
    }
    if (!isNewConfig) fetchRuns()
    return () => { active = false }
  }, [configId, isNewConfig])

  useEffect(() => {
    async function fetchConfig() {
      if (!isNewConfig) {
        try {
          const res = await fetch(`http://localhost:8080/v1/config/${configId}`)
          if (!res.ok) throw new Error('Failed to fetch config')
          const data = await res.json()
          setJson(JSON.stringify(data, null, 2))
        } catch (e) {
          console.error('Config fetch error:', e)
          setConfigError(true)
        }
      } else {
        setJson(`{
  "connectors": [
    {
      "class": "com.example.FileConnector",
      "path": "/data/input",
      "name": "file-connector",
      "pipeline": "main-pipeline"
    }
  ],
  "pipelines": [
    {
      "name": "main-pipeline",
      "stages": [
        {
          "name": "parse",
          "class": "com.example.JsonParser"
        },
        {
          "name": "transform",
          "class": "com.example.DataTransformer"
        }
      ]
    }
  ],
  "indexer": {
    "type": "elasticsearch",
    "url": "http://localhost:9200",
    "index": "my-index",
    "batchSize": 1000
  }
}`)
      }
    }
    fetchConfig()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [configId, isNewConfig])

  // Save handler - Always creates a new config as per api.md documentation
  async function handleSave() {
    try {
      setIsSaving(true);
      setSaveSuccess(false);
      
      const response = await fetch("http://localhost:8080/v1/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: json,
      });

      const data = await response.json()
      if (!response.ok) throw new Error("Failed to save config")
      // If new config created, navigate to its page
      if (isNewConfig && data.configId) {
        router.replace(`/configs/detail?id=${data.configId}`)
      }

      // Set saved state for visual feedback
      setSaveSuccess(true);
      
      // Reset success state after 2 seconds
      setTimeout(() => {
        setSaveSuccess(false);
      }, 2000);
    } catch (error) {
      console.error("Error saving config:", error);
    } finally {
      setIsSaving(false);
    }
  }

  // Run handler - No longer dependent on save status
  async function handleRun() {
    if (isNewConfig) {
      setRunError("Please save configuration before running")
      return
    }
    
    setIsRunning(true)
    setRunSuccess(false)
    setRunError("")
    
    try {
      const response = await fetch(`http://localhost:8080/v1/run`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ configId })
      })
      
      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.message || "Failed to start run")
      }
      
      setRunSuccess(true)
      setTimeout(() => setRunSuccess(false), 2000)
    } catch (error) {
      console.error("Run error:", error)
      setRunError(error instanceof Error ? error.message : "Failed to start run")
    } finally {
      setIsRunning(false)
    }
  }

  return (
    <div className="p-4 sm:p-6 lg:p-8">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-6">
        <div className="flex items-center gap-3">
          <Button variant="ghost" size="icon" className="h-8 w-8" asChild>
            <Link href="/configs"><ArrowLeft className="h-4 w-4" /></Link>
          </Button>
          <h1 className="text-xl sm:text-2xl lg:text-3xl font-bold mb-0 text-primary-800">
            {isNewConfig ? "New Configuration" : `Config: ${configId}`}
          </h1>
        </div>
      </div>
      
      {configError ? (
        <div className="rounded-md bg-red-50 p-4 mb-6">
          <div className="flex">
            <div>
              <h3 className="text-sm font-medium text-red-800">Config not found</h3>
              <div className="mt-2 text-sm text-red-700">
                <p>The configuration could not be loaded. Please try again or return to the config list.</p>
              </div>
            </div>
          </div>
        </div>
      ) : (
        <div className="grid gap-6 md:grid-cols-[1fr_340px]">
          <Card className="border shadow-sm overflow-hidden">
            <CardContent className="p-0">
              <JsonEditor value={json} onChange={setJson} />
            </CardContent>
          </Card>
          <div className="space-y-6">
            <Card className="border shadow-sm">
              <CardHeader className="py-3 sm:py-4">
                <CardTitle className="text-base sm:text-lg">Actions</CardTitle>
                <CardDescription className="text-xs sm:text-sm">Operations for this configuration</CardDescription>
              </CardHeader>
              <CardContent className="space-y-3 sm:space-y-4">
                <Button 
                  className={`w-full justify-start text-sm h-9 ${saveSuccess ? "bg-green-600 hover:bg-green-700" : ""}`}
                  onClick={handleSave}
                  disabled={isSaving}
                  variant={saveSuccess ? "default" : "default"}
                >
                  {isSaving ? (
                    <span className="mr-2 h-4 w-4 animate-spin rounded-full border-2 border-current border-t-transparent" />
                  ) : saveSuccess ? (
                    <Check className="mr-2 h-4 w-4 flex-shrink-0" />
                  ) : (
                    <Save className="mr-2 h-4 w-4 flex-shrink-0" />
                  )}
                  {isSaving 
                    ? "Saving..." 
                    : saveSuccess 
                      ? "Saved!"
                      : "Save Configuration"
                  }
                </Button>
                <Button className={`w-full justify-start text-sm h-9 ${isRunning ? "opacity-70" : ""}`} variant="secondary" onClick={handleRun} disabled={isRunning}>
                  {isRunning ? (
                    <span className="mr-2 h-4 w-4 animate-spin rounded-full border-2 border-current border-t-transparent" />
                  ) : runSuccess ? (
                    <Check className="mr-2 h-4 w-4 flex-shrink-0" />
                  ) : (
                    <Play className="mr-2 h-4 w-4 flex-shrink-0" />
                  )}
                  {isRunning
                    ? "Running..."
                    : runSuccess
                      ? "Run Started"
                      : "Run Configuration"}
                </Button>
                {runError && (
                  <div className="text-xs text-red-500">{runError}</div>
                )}
                <Button className="w-full justify-start text-sm h-9" variant="outline" asChild>
                  <Link href="/configs">Back to Configs</Link>
                </Button>
              </CardContent>
            </Card>

            {!isNewConfig && (
              <Card className="border shadow-sm">
                <CardHeader className="py-3 sm:py-4">
                  <CardTitle className="text-base sm:text-lg">Recent Runs</CardTitle>
                  <CardDescription className="text-xs sm:text-sm">Latest executions</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  {runsLoading ? (
                    <div>Loading...</div>
                  ) : runsError ? (
                    <div className="text-sm text-muted-foreground">Lucille Unavailable</div>
                  ) : recentRuns && recentRuns.length > 0 ? (
                    recentRuns.map(run => (
                      <div key={run.runId} className="flex items-center justify-between border-b pb-3 last:border-0 last:pb-0">
                        <div>
                          <div className="font-medium text-sm">Run ID: {run.runId}</div>
                          <div className="text-xs text-muted-foreground">{timeAgo(run.startTime ?? 0)}</div>
                        </div>
                        <div>
                          <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${run.done ? 'bg-green-100 text-green-800' : 'bg-blue-100 text-blue-800'}`}>
                            {run.done ? 'COMPLETED' : 'RUNNING'}
                          </span>
                        </div>
                      </div>
                    ))
                  ) : (
                    <div className="text-sm text-muted-foreground">No runs found</div>
                  )}
                  <Button variant="ghost" size="sm" className="w-full text-xs sm:text-sm h-8" asChild>
                    <Link href={`/runs?configId=${configId}`}>View All Runs</Link>
                  </Button>
                </CardContent>
              </Card>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
