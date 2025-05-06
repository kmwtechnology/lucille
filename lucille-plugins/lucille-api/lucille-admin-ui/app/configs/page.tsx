import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { 
  Table, 
  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from "@/components/ui/table"
import { Plus, Eye } from "lucide-react"
import { Suspense } from "react"

// Config table with data fetching
async function ConfigsTable() {
  // Typed array of config IDs
  let configs: { id: string }[] = [];
  let error = false;
  try {
    const res = await fetch(`${process.env.LUCILLE_API_URL || 'http://localhost:8080'}/v1/config`, {
      method: 'GET',
      headers: process.env.LUCILLE_API_AUTH ? { 'authorization': process.env.LUCILLE_API_AUTH } : {},
    });
    if (res.ok) {
      const data = await res.json();
      configs = Object.keys(data || {}).map((id) => ({ id }));
    } else {
      // console.error('Configs fetch bad status:', res.status);
      error = true;
    }
  } catch (e) {
    console.error('Configs fetch network error:', e);
    error = true;
  }
  if (error) {
    return <div className="text-sm text-red-600">Lucille Unavailable</div>;
  }
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Name</TableHead>
          <TableHead className="text-right">Actions</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {configs.map((config) => (
          <TableRow key={config.id}>
            <TableCell className="font-medium">{config.id}</TableCell>
            <TableCell className="text-right">
              <Button variant="ghost" size="icon" asChild>
                <Link href={`/configs/${config.id}`} aria-label="View config">
                  <Eye className="h-4 w-4" />
                </Link>
              </Button>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

export default function ConfigsPage() {
  return (
    <div className="p-8">
      <div className="flex justify-between items-center mb-8">
        <div>
          <h1 className="text-3xl font-bold mb-2">Configurations</h1>
          <p className="text-muted-foreground">Manage your pipeline configurations</p>
        </div>
        <Button asChild className="bg-primary-600 hover:bg-primary-700">
          <Link href="/configs/new">
            <Plus className="mr-2 h-4 w-4" /> New Configuration
          </Link>
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>All Configurations</CardTitle>
        </CardHeader>
        <CardContent>
          <Suspense fallback={<div className="space-y-3">
            {[1, 2, 3].map((i) => (
              <div key={i} className="w-full h-12 bg-gray-100 animate-pulse rounded" />
            ))}
          </div>}>
            <ConfigsTable />
          </Suspense>
        </CardContent>
      </Card>
    </div>
  )
}
