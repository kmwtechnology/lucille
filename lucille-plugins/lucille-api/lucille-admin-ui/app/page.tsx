"use client"

import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { ArrowRight, Play, Settings, Activity, CheckCircle, Database, Server, XCircle } from "lucide-react"
import type { Run } from "../types/run"
import { useEffect, useState } from "react"

// Types for system stats
interface SysStats {
  percent?: number;
  jvm?: { percent?: number; used?: number; total?: number };
  cpu?: { percent?: number; loadAverage?: number };
  ram?: { percent?: number; used?: number; total?: number };
  storage?: { percent?: number; used?: number; total?: number };
}

type BytesStats = { total: number; used: number; available: number; percent: number }
type CpuStats = { total: number; available: number; used: number; loadAverage: number; percent: number }
type Stats = { jvm: BytesStats; cpu: CpuStats; storage: BytesStats; ram: BytesStats }

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 Bytes'
  const k = 1024
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`
}

export default function Dashboard() {
  // Health status
  const [status, setStatus] = useState<'loading' | 'healthy' | 'unavailable' | 'notready'>('loading');
  // Configs
  const [totalConfigs, setTotalConfigs] = useState<number | null>(null);
  const [configsLoading, setConfigsLoading] = useState<boolean>(true);
  const [configsError, setConfigsError] = useState<string | null>(null);
  // Runs
  const [runs, setRuns] = useState<Run[] | null>(null);
  const [runsLoading, setRunsLoading] = useState<boolean>(true);
  // System stats
  const [sysStats, setSysStats] = useState<SysStats | null>(null);
  const [sysStatsLoading, setSysStatsLoading] = useState<boolean>(true);
  // Detailed stats for health section
  const [stats, setStats] = useState<Stats | null>(null);
  const [statsLoading, setStatsLoading] = useState(true);

  useEffect(() => {
    let _isActive = true;
    async function checkHealth() {
      setStatus('loading');
      try {
        const res = await fetch('http://localhost:8080/v1/livez');
        if (res.status !== 200) {
          setStatus('unavailable');
          return;
        }
      } catch {
        setStatus('unavailable');
        return;
      }
      try {
        const res = await fetch('http://localhost:8080/v1/readyz');
        if (res.status !== 200) {
          setStatus('notready');
          return;
        }
        setStatus('healthy');
      } catch {
        setStatus('notready');
      }
    }
    checkHealth();
    return () => { _isActive = false; };
  }, []);

  useEffect(() => {
    let _isActive = true;
    async function fetchConfigs() {
      setConfigsLoading(true);
      setConfigsError(null);
      try {
        const res = await fetch('http://localhost:8080/v1/config');
        if (!res.ok) throw new Error('Failed to fetch configs');
        const data = await res.json();
        let count = null;
        if (data && typeof data === 'object' && !Array.isArray(data)) {
          count = Object.keys(data).length;
        }
        if (count === null) {
          console.error('Unexpected configs response:', data);
          throw new Error('Malformed configs response');
        }
        if (_isActive) setTotalConfigs(count);
      } catch (error) {
        if (_isActive) setConfigsError(error instanceof Error ? error.message : 'Unknown error');
      } finally {
        if (_isActive) setConfigsLoading(false);
      }
    }
    fetchConfigs();
    return () => { _isActive = false; };
  }, []);

  useEffect(() => {
    let active = true;
    async function fetchStats() {
      try {
        const res = await fetch('http://localhost:8080/v1/systemstats');
        const data = await res.json();
        if (active) {
          setSysStats(data);
          setStats(data as Stats); // For detailed stats
        }
      } catch {
        console.warn('Failed to fetch system stats');
      } finally {
        if (active) {
          setSysStatsLoading(false);
          setStatsLoading(false);
        }
      }
    }
    fetchStats();
    const intervalId = setInterval(fetchStats, 5000);
    return () => { active = false; clearInterval(intervalId); };
  }, []);

  useEffect(() => {
    let _isActive = true;
    async function fetchRuns() {
      setRunsLoading(true);
      try {
        const res = await fetch('http://localhost:8080/v1/run');
        const data = await res.json();
        if (_isActive) setRuns(Array.isArray(data) ? data : []);
      } catch (e) {
        console.warn('Failed to fetch runs', e);
      } finally {
        if (_isActive) setRunsLoading(false);
      }
    }
    fetchRuns();
    return () => { _isActive = false; };
  }, []);

  // Compute counts
  const todaysRunsCount = runs?.length ?? 0;
  const completedCount = runs?.filter(r => r.done).length ?? 0;
  const runningCount = runs?.filter(r => !r.done).length ?? 0;

  // Helper to format relative time
  function timeAgo(unixSec: number): string {
    const now = Date.now() / 1000;
    const diff = now - unixSec;
    if (diff < 60) return `${Math.floor(diff)}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    return `${Math.floor(diff / 86400)}d ago`;
  }

  return (
    <div className="p-4 sm:p-6 lg:p-8">
      <div className="mb-6 sm:mb-8">
        <h1 className="text-3xl font-bold mb-2 text-primary-800">Lucille Admin Dashboard</h1>
        <p className="text-muted-foreground">Manage your data pipelines, configurations, and system health</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4 sm:gap-6 mb-8">
        {/* System Health Status Card */}
        <Card className={`border-primary-100 shadow-sm hover:shadow-md transition-shadow flex flex-col h-full ${status === 'unavailable' ? 'border-red-100 bg-red-50' : 'border-teal-100'}`}>
          <CardHeader className="pb-2 space-y-1">
            <CardTitle className={`flex items-center gap-2 text-base sm:text-lg ${status === 'unavailable' ? 'text-red-700' : 'text-teal-700'}`}>
              {status === 'loading' ? (
                <Activity className="h-5 w-5 animate-spin text-teal-500" />
              ) : status === 'healthy' ? (
                <CheckCircle className="h-5 w-5 text-teal-500" />
              ) : (
                <XCircle className="h-5 w-5 text-red-500" />
              )}
              <span>API Status</span>
            </CardTitle>

          </CardHeader>
          <CardContent className="flex-1">
            {status === 'loading' ? (
              <div className="text-2xl font-bold text-teal-700">Checking...</div>
            ) : status === 'healthy' ? (
              <>
                <div className="text-2xl font-bold text-teal-700">Healthy</div>
                <div className="text-sm text-teal-600">All systems operational</div>
              </>
            ) : status === 'unavailable' ? (
              <>
                <div className="text-2xl font-bold text-red-700">Unhealthy</div>
                <div className="text-sm text-red-600">Lucille API server is unavailable.</div>
              </>
            ) : (
              <>
                <div className="text-2xl font-bold text-yellow-700">Not Ready</div>
                <div className="text-sm text-yellow-600">Lucille API server is not ready to take requests yet.</div>
              </>
            )}
          </CardContent>
        </Card>

        {/* Storage Card */}
        <Card className="border-blue-100 shadow-sm">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2">
              <Database className="h-5 w-5 text-blue-500" />
              Storage
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-blue-700">{statsLoading ? 'Loading...' : stats ? `${stats.storage.percent.toFixed(0)}%` : 'N/A'}</div>
            <div className="text-sm text-muted-foreground">{statsLoading ? '' : stats ? `${formatBytes(stats.storage.available)} free of ${formatBytes(stats.storage.total)}` : ''}</div>
          </CardContent>
        </Card>

        {/* Memory Card */}
        <Card className="border-teal-100 shadow-sm">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2">
              <Server className="h-5 w-5 text-teal-500" />
              Memory
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-teal-700">{statsLoading ? 'Loading...' : stats ? `${stats.ram.percent.toFixed(0)}%` : 'N/A'}</div>
            <div className="text-sm text-muted-foreground">{statsLoading ? '' : stats ? `${formatBytes(stats.ram.used)} used of ${formatBytes(stats.ram.total)}` : ''}</div>
          </CardContent>
        </Card>

        {/* CPU Card */}
        <Card className="border-blue-100 shadow-sm">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2">
              <Activity className="h-5 w-5 text-blue-500" />
              CPU
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-blue-700">{statsLoading ? 'Loading...' : stats ? `${stats.cpu.percent.toFixed(0)}%` : 'N/A'}</div>
            <div className="text-sm text-muted-foreground">{statsLoading ? '' : stats ? `Load Avg: ${stats.cpu.loadAverage.toFixed(2)}` : ''}</div>
          </CardContent>
        </Card>

        {/* JVM Card */}
        <Card className="border-orange-100 shadow-sm">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2">
              <Server className="h-5 w-5 text-orange-500" />
              JVM
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-orange-700">{statsLoading ? 'Loading...' : stats ? `${stats.jvm.percent.toFixed(0)}%` : 'N/A'}</div>
            <div className="text-sm text-muted-foreground">{statsLoading ? '' : stats ? `${formatBytes(stats.jvm.used)} used of ${formatBytes(stats.jvm.total)}` : ''}</div>
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-6 mb-8">
        {/* Configurations Card */}
        <Card className="border-primary-100 shadow-sm hover:shadow-md transition-shadow flex flex-col h-full">
          <CardHeader className="pb-2 space-y-1">
            <CardTitle className="flex items-center gap-2 text-base sm:text-lg">
              <Settings className="h-5 w-5 text-primary-600 flex-shrink-0" />
              <span className="truncate">Configurations</span>
            </CardTitle>
            <CardDescription className="text-xs sm:text-sm">Manage your pipeline configurations</CardDescription>
          </CardHeader>
          <CardContent className="flex-1">
            <div className="space-y-2">
              <div className="flex justify-between items-center">
                <span className="text-xs sm:text-sm">Total Configs</span>
                {configsLoading ? (
                  <span className="text-xs font-medium text-muted-foreground">...</span>
                ) : configsError ? (
                  <span className="text-xs font-medium text-red-600">Lucille Unavailable</span>
                ) : (
                  <span className="font-medium text-xs sm:text-sm">{totalConfigs}</span>
                )}
              </div>
            </div>
          </CardContent>
          <CardFooter>
            <Button
              variant="ghost"
              size="sm"
              className="w-full text-primary-600 hover:text-primary-700 hover:bg-primary-50 text-xs sm:text-sm py-1.5"
              asChild
            >
              <Link href="/configs">
                Manage Configs <ArrowRight className="ml-1 h-3 w-3 sm:h-4 sm:w-4" />
              </Link>
            </Button>
          </CardFooter>
        </Card>

        {/* Pipeline Runs Card */}
        <Card className="border-primary-100 shadow-sm hover:shadow-md transition-shadow flex flex-col h-full">
          <CardHeader className="pb-2 space-y-1">
            <CardTitle className="flex items-center gap-2 text-base sm:text-lg">
              <Play className="h-5 w-5 text-primary-600 flex-shrink-0" />
              <span className="truncate">Pipeline Runs</span>
            </CardTitle>
            <CardDescription className="text-xs sm:text-sm">Monitor pipeline executions</CardDescription>
          </CardHeader>
          <CardContent className="flex-1">
            <div className="space-y-2">
              <div className="flex justify-between items-center">
                <span className="text-xs sm:text-sm">Today&apos;s Runs</span>
                {runsLoading ? (
                  <span className="text-xs sm:text-sm">Loading...</span>
                ) : runs ? (
                  <span className="text-xs sm:text-sm">{todaysRunsCount}</span>
                ) : (
                  <span className="text-xs font-medium text-red-600">Lucille Unavailable</span>
                )}
              </div>
              <div className="flex justify-between items-center">
                <span className="text-xs sm:text-sm">Completed</span>
                <span className="font-medium text-xs sm:text-sm">{runsLoading ? '' : runs ? completedCount : ''}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-xs sm:text-sm">Running</span>
                <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium bg-blue-100 text-blue-800">
                  {runsLoading ? '' : runs ? `${runningCount} Active` : ''}
                </span>
              </div>
            </div>
          </CardContent>
          <CardFooter>
            <Button
              variant="ghost"
              size="sm"
              className="w-full text-primary-600 hover:text-primary-700 hover:bg-primary-50 text-xs sm:text-sm py-1.5"
              asChild
            >
              <Link href="/runs">
                View Runs <ArrowRight className="ml-1 h-3 w-3 sm:h-4 sm:w-4" />
              </Link>
            </Button>
          </CardFooter>
        </Card>

        {/* Quick Actions Card */}
        <Card className="border-primary-100 shadow-sm flex flex-col h-full">
          <CardHeader>
            <CardTitle className="text-lg sm:text-xl text-primary-800">Quick Actions</CardTitle>
            <CardDescription className="text-xs sm:text-sm">Common operations</CardDescription>
          </CardHeader>
          <CardContent className="flex-1 grid gap-3 sm:gap-4">
            <Button className="w-full justify-start bg-primary-600 hover:bg-primary-700 text-xs sm:text-sm py-2 h-auto" asChild>
              <Link href="/configs/detail?id=new">
                <Settings className="mr-2 h-4 w-4 flex-shrink-0" /> Create New Configuration
              </Link>
            </Button>
            <Button className="w-full justify-start bg-secondary-600 hover:bg-secondary-700 text-xs sm:text-sm py-2 h-auto" asChild>
              <Link href="/runs?id=new">
                <Play className="mr-2 h-4 w-4 flex-shrink-0" /> Start New Run
              </Link>
            </Button>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
