"use client"

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Activity, CheckCircle, Database, Server, XCircle } from "lucide-react"
import { useEffect, useState } from "react"

// Define stats types and formatting helper

type BytesStats = { total: number; used: number; available: number; percent: number }

type CpuStats = { total: number; available: number; used: number; loadAverage: number; percent: number }

type Stats = { jvm: BytesStats; cpu: CpuStats; storage: BytesStats; ram: BytesStats }

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 Bytes'
  const k = 1024
  const sizes = ['Bytes','KB','MB','GB','TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return `${(bytes / Math.pow(k,i)).toFixed(1)} ${sizes[i]}`
}

export default function HealthPage() {
  const [status, setStatus] = useState<'loading'|'healthy'|'unavailable'|'notready'>('loading');
  const [stats, setStats] = useState<Stats | null>(null);
  const [statsLoading, setStatsLoading] = useState(true);

  useEffect(() => {
    let _isActive = true;
    async function checkHealth() {
      setStatus('loading');
      // First: liveness
      try {
        const res = await fetch('/api/lucille/livez');
        const data = await res.json();
        if (res.ok && data.ok) {
          // Liveness check passed
        } else {
          setStatus('unavailable');
          return;
        }
      } catch {
        setStatus('unavailable');
        return;
      }
      // Second: readiness
      try {
        const res = await fetch('/api/lucille/readyz');
        const data = await res.json();
        if (res.ok && data.ok) {
          setStatus('healthy');
        } else {
          setStatus('notready');
        }
      } catch {
        setStatus('notready');
      }
    }
    checkHealth();
    return () => { _isActive = false; };
  }, []);

  useEffect(() => {
    async function fetchStats() {
      let data = null;
      try {
        const res = await fetch('/api/lucille/systemstats');
        const json = await res.json();
        if (res.ok && json.ok !== false) {
          data = json;
        } else {
          console.warn('System stats error:', json.error || json);
        }
      } catch (error) {
        console.warn('Failed to fetch system stats', error);
      }
      setStats(data);
      setStatsLoading(false);
    }
    fetchStats();
  }, []);

  return (
    <div className="p-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold mb-2 text-teal-800">System Health</h1>
        <p className="text-muted-foreground">Monitor the health and status of your Lucille deployment</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-6 mb-8">
        <Card
          className={
            status === 'unavailable'
              ? 'border-red-100 shadow-sm bg-red-50'
              : 'border-teal-100 shadow-sm'
          }
        >
          <CardHeader className="pb-2">
            <CardTitle className={`flex items-center gap-2 text-${status === 'unavailable' ? 'red' : 'teal'}-700`}>
              {status === 'loading' ? (
                <Activity className="h-5 w-5 animate-spin text-teal-500" />
              ) : status === 'healthy' ? (
                <CheckCircle className="h-5 w-5 text-teal-500" />
              ) : (
                <XCircle className="h-5 w-5 text-red-500" />
              )}
              API Status
            </CardTitle>
          </CardHeader>
          <CardContent>
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

        {/* JVM Stats */}
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

      <Tabs defaultValue="services">
        <TabsList className="mb-4 bg-white border border-blue-100">
          <TabsTrigger value="services" className="data-[state=active]:bg-tech-gradient data-[state=active]:text-white">
            Services
          </TabsTrigger>
          <TabsTrigger
            value="dependencies"
            className="data-[state=active]:bg-tech-gradient data-[state=active]:text-white"
          >
            Dependencies
          </TabsTrigger>
          <TabsTrigger value="logs" className="data-[state=active]:bg-tech-gradient data-[state=active]:text-white">
            System Logs
          </TabsTrigger>
        </TabsList>

        <TabsContent value="services">
          <Card className="border-teal-100 shadow-sm">
            <CardHeader>
              <CardTitle className="text-teal-800">Service Status</CardTitle>
              <CardDescription>Health status of all Lucille services</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                  <div className="border border-teal-100 rounded-md p-4 shadow-sm">
                    <div className="flex items-center justify-between mb-2">
                      <div className="font-medium">API Service</div>
                      <div className="flex items-center text-teal-600">
                        <CheckCircle className="h-4 w-4 mr-1" /> Healthy
                      </div>
                    </div>
                    <div className="text-sm text-muted-foreground">Uptime: 15 days, 7 hours</div>
                  </div>

                  <div className="border border-blue-100 rounded-md p-4 shadow-sm">
                    <div className="flex items-center justify-between mb-2">
                      <div className="font-medium">Connector Service</div>
                      <div className="flex items-center text-teal-600">
                        <CheckCircle className="h-4 w-4 mr-1" /> Healthy
                      </div>
                    </div>
                    <div className="text-sm text-muted-foreground">Uptime: 15 days, 7 hours</div>
                  </div>

                  <div className="border border-teal-100 rounded-md p-4 shadow-sm">
                    <div className="flex items-center justify-between mb-2">
                      <div className="font-medium">Pipeline Service</div>
                      <div className="flex items-center text-teal-600">
                        <CheckCircle className="h-4 w-4 mr-1" /> Healthy
                      </div>
                    </div>
                    <div className="text-sm text-muted-foreground">Uptime: 15 days, 7 hours</div>
                  </div>

                  <div className="border border-blue-100 rounded-md p-4 shadow-sm">
                    <div className="flex items-center justify-between mb-2">
                      <div className="font-medium">Indexer Service</div>
                      <div className="flex items-center text-teal-600">
                        <CheckCircle className="h-4 w-4 mr-1" /> Healthy
                      </div>
                    </div>
                    <div className="text-sm text-muted-foreground">Uptime: 15 days, 7 hours</div>
                  </div>

                  <div className="border border-teal-100 rounded-md p-4 shadow-sm">
                    <div className="flex items-center justify-between mb-2">
                      <div className="font-medium">Scheduler Service</div>
                      <div className="flex items-center text-teal-600">
                        <CheckCircle className="h-4 w-4 mr-1" /> Healthy
                      </div>
                    </div>
                    <div className="text-sm text-muted-foreground">Uptime: 15 days, 7 hours</div>
                  </div>

                  <div className="border border-blue-100 rounded-md p-4 shadow-sm">
                    <div className="flex items-center justify-between mb-2">
                      <div className="font-medium">Monitoring Service</div>
                      <div className="flex items-center text-yellow-600">
                        <Activity className="h-4 w-4 mr-1" /> Degraded
                      </div>
                    </div>
                    <div className="text-sm text-muted-foreground">Uptime: 2 days, 3 hours</div>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="dependencies">
          <Card className="border-blue-100 shadow-sm">
            <CardHeader>
              <CardTitle className="text-blue-800">External Dependencies</CardTitle>
              <CardDescription>Status of connected systems and services</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                  <div className="border border-teal-100 rounded-md p-4 shadow-sm">
                    <div className="flex items-center justify-between mb-2">
                      <div className="font-medium">Elasticsearch</div>
                      <div className="flex items-center text-teal-600">
                        <CheckCircle className="h-4 w-4 mr-1" /> Connected
                      </div>
                    </div>
                    <div className="text-sm text-muted-foreground">Version: 7.17.0</div>
                    <div className="text-sm text-muted-foreground">Endpoint: http://elasticsearch:9200</div>
                  </div>

                  <div className="border border-blue-100 rounded-md p-4 shadow-sm">
                    <div className="flex items-center justify-between mb-2">
                      <div className="font-medium">Database</div>
                      <div className="flex items-center text-teal-600">
                        <CheckCircle className="h-4 w-4 mr-1" /> Connected
                      </div>
                    </div>
                    <div className="text-sm text-muted-foreground">Type: PostgreSQL 14.2</div>
                    <div className="text-sm text-muted-foreground">Host: postgres:5432</div>
                  </div>

                  <div className="border border-teal-100 rounded-md p-4 shadow-sm">
                    <div className="flex items-center justify-between mb-2">
                      <div className="font-medium">Object Storage</div>
                      <div className="flex items-center text-teal-600">
                        <CheckCircle className="h-4 w-4 mr-1" /> Connected
                      </div>
                    </div>
                    <div className="text-sm text-muted-foreground">Type: S3 Compatible</div>
                    <div className="text-sm text-muted-foreground">Endpoint: http://minio:9000</div>
                  </div>

                  <div className="border border-blue-100 rounded-md p-4 shadow-sm">
                    <div className="flex items-center justify-between mb-2">
                      <div className="font-medium">Message Queue</div>
                      <div className="flex items-center text-red-600">
                        <XCircle className="h-4 w-4 mr-1" /> Disconnected
                      </div>
                    </div>
                    <div className="text-sm text-muted-foreground">Type: Kafka</div>
                    <div className="text-sm text-muted-foreground">Last Seen: 3 hours ago</div>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="logs">
          <Card className="border-teal-100 shadow-sm">
            <CardHeader>
              <CardTitle className="text-teal-800">System Logs</CardTitle>
              <CardDescription>Recent system-level log entries</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="bg-teal-50/50 p-4 rounded-md font-mono text-sm h-[400px] overflow-y-auto border border-teal-100">
                <div className="text-teal-600">
                  [2023-04-07 11:30:00] INFO: System health check completed successfully
                </div>
                <div className="text-teal-600">[2023-04-07 11:15:00] INFO: Configuration cache refreshed</div>
                <div className="text-yellow-600">[2023-04-07 11:00:05] WARN: High memory usage detected (75%)</div>
                <div className="text-teal-600">
                  [2023-04-07 10:45:00] INFO: System health check completed successfully
                </div>
                <div className="text-red-600">[2023-04-07 10:30:12] ERROR: Failed to connect to Kafka broker</div>
                <div className="text-red-600">[2023-04-07 10:30:10] ERROR: Connection timeout after 30s</div>
                <div className="text-teal-600">
                  [2023-04-07 10:15:00] INFO: System health check completed successfully
                </div>
                <div className="text-teal-600">[2023-04-07 10:00:00] INFO: Scheduled maintenance tasks completed</div>
                <div className="text-teal-600">
                  [2023-04-07 09:45:00] INFO: System health check completed successfully
                </div>
                <div className="text-teal-600">[2023-04-07 09:30:00] INFO: User &apos;admin&apos; logged in</div>
                <div className="text-teal-600">
                  [2023-04-07 09:15:00] INFO: System health check completed successfully
                </div>
                <div className="text-teal-600">[2023-04-07 09:00:00] INFO: Daily statistics report generated</div>
                <div className="text-teal-600">
                  [2023-04-07 08:45:00] INFO: System health check completed successfully
                </div>
                <div className="text-teal-600">[2023-04-07 08:30:00] INFO: Monitoring service restarted</div>
                <div className="text-yellow-600">[2023-04-07 08:29:45] WARN: Monitoring service not responding</div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  )
}
