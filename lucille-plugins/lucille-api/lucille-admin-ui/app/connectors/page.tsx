import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Plus } from "lucide-react"

export default function ConnectorsPage() {
  return (
    <div className="p-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold mb-2 text-primary-800">Connectors</h1>
        <p className="text-muted-foreground">Manage your data source connections</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        <Card className="border-primary-100 shadow-sm hover:shadow-md transition-shadow">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2">
              <Plus className="h-5 w-5 text-primary-600" />
              New Connector
            </CardTitle>
            <CardDescription>Create a new data source connection</CardDescription>
          </CardHeader>
          <CardContent>
            <Button className="w-full justify-start bg-primary-600 hover:bg-primary-700">
              <Plus className="mr-2 h-4 w-4" />
              Add Connector
            </Button>
          </CardContent>
        </Card>

        <Card className="border-primary-100 shadow-sm hover:shadow-md transition-shadow">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2">
              <Plus className="h-5 w-5 text-primary-600" />
              Database Connector
            </CardTitle>
            <CardDescription>Connect to your database</CardDescription>
          </CardHeader>
          <CardContent>
            <Button variant="outline" className="w-full border-primary-200 text-primary-700 hover:bg-primary-50">
              Configure
            </Button>
          </CardContent>
        </Card>

        <Card className="border-primary-100 shadow-sm hover:shadow-md transition-shadow">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2">
              <Plus className="h-5 w-5 text-primary-600" />
              API Connector
            </CardTitle>
            <CardDescription>Connect to external APIs</CardDescription>
          </CardHeader>
          <CardContent>
            <Button variant="outline" className="w-full border-primary-200 text-primary-700 hover:bg-primary-50">
              Configure
            </Button>
          </CardContent>
        </Card>

        <Card className="border-primary-100 shadow-sm hover:shadow-md transition-shadow">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2">
              <Plus className="h-5 w-5 text-primary-600" />
              File Connector
            </CardTitle>
            <CardDescription>Connect to file systems</CardDescription>
          </CardHeader>
          <CardContent>
            <Button variant="outline" className="w-full border-primary-200 text-primary-700 hover:bg-primary-50">
              Configure
            </Button>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
