import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Button } from "@/components/ui/button"
import { Edit, Trash2 } from "lucide-react"

type Props = {
  params: {
    id: string
  }
}

export default function ConnectorDetailPage({ params }: Props) {
  return (
    <div className="p-8">
      <div className="mb-8">
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-2xl font-bold text-primary-800">Connector #{params.id}</h1>
            <p className="text-muted-foreground">Configuration and settings</p>
          </div>
          <div className="flex gap-2">
            <Button variant="outline" className="border-primary-200 text-primary-700 hover:bg-primary-50">
              <Edit className="mr-2 h-4 w-4" />
              Edit
            </Button>
            <Button variant="destructive" className="hover:bg-destructive/10">
              <Trash2 className="mr-2 h-4 w-4" />
              Delete
            </Button>
          </div>
        </div>
      </div>

      <Tabs defaultValue="settings" className="w-full">
        <TabsList className="grid w-full grid-cols-3 bg-primary-100 rounded-md">
          <TabsTrigger value="settings" className="bg-primary-50 text-primary-800">Settings</TabsTrigger>
          <TabsTrigger value="logs" className="text-primary-800">Logs</TabsTrigger>
          <TabsTrigger value="metrics" className="text-primary-800">Metrics</TabsTrigger>
        </TabsList>
        <TabsContent value="settings" className="mt-4">
          <Card className="border-primary-100 shadow-sm">
            <CardHeader>
              <CardTitle className="text-primary-800">Connector Settings</CardTitle>
            </CardHeader>
            <CardContent>
              <form className="space-y-4">
                <div className="flex flex-col gap-2">
                  <label className="text-sm font-medium">Name</label>
                  <input
                    type="text"
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                    placeholder="Connector name"
                  />
                </div>
                <div className="flex flex-col gap-2">
                  <label className="text-sm font-medium">Type</label>
                  <select
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    <option>Database</option>
                    <option>API</option>
                    <option>File</option>
                  </select>
                </div>
                <div className="flex flex-col gap-2">
                  <label className="text-sm font-medium">Connection String</label>
                  <input
                    type="text"
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                    placeholder="Enter connection string"
                  />
                </div>
              </form>
            </CardContent>
          </Card>
        </TabsContent>
        <TabsContent value="logs" className="mt-4">
          <Card className="border-primary-100 shadow-sm">
            <CardHeader>
              <CardTitle className="text-primary-800">Connection Logs</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="flex justify-between items-center p-2 bg-muted rounded-md">
                  <span className="text-sm">Connection established</span>
                  <span className="text-xs text-muted-foreground">10:30 AM</span>
                </div>
                <div className="flex justify-between items-center p-2 bg-muted rounded-md">
                  <span className="text-sm">Data transfer completed</span>
                  <span className="text-xs text-muted-foreground">10:32 AM</span>
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
        <TabsContent value="metrics" className="mt-4">
          <Card className="border-primary-100 shadow-sm">
            <CardHeader>
              <CardTitle className="text-primary-800">Performance Metrics</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="flex justify-between items-center">
                  <span className="text-sm">Success Rate</span>
                  <span className="font-medium">99.5%</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm">Average Response Time</span>
                  <span className="font-medium">120ms</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm">Data Transfer Rate</span>
                  <span className="font-medium">1.2 MB/s</span>
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  )
}
