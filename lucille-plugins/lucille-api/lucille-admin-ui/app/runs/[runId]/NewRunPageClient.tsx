'use client'

import { useEffect, useState } from 'react';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Button } from '@/components/ui/button';
import { Play, Check } from 'lucide-react';

interface ConfigItem { id: string }

export default function NewRunPageClient() {
  const [error, setError] = useState<string | null>(null);
  const [configs, setConfigs] = useState<ConfigItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [statuses, setStatuses] = useState<Record<string, 'idle' | 'loading' | 'started'>>({});

  useEffect(() => {
    async function loadConfigs() {
      try {
        const res = await fetch('/api/lucille/config', { cache: 'no-store' });
        if (res.ok) {
          const data = await res.json();
          const cfgs = Object.keys(data || {}).map((id) => ({ id }));
          setConfigs(cfgs);
        } else {
          setError(`Error loading configs: ${res.status}`);
        }
      } catch (e) {
        setError('Network error loading configs');
        console.error('Failed to load configs:', e);
      } finally {
        setLoading(false);
      }
    }
    loadConfigs();
  }, []);

  async function handleRun(configId: string) {
    setStatuses((prev) => ({ ...prev, [configId]: 'loading' }));
    try {
      const res = await fetch('/api/lucille/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ configId }),
      });
      if (!res.ok) throw new Error('Failed to start run');
      await res.json();
      setStatuses((prev) => ({ ...prev, [configId]: 'started' }));
      setTimeout(() => {
        setStatuses((prev) => ({ ...prev, [configId]: 'idle' }));
      }, 4000);
    } catch (e) {
      console.error('Run start error:', e);
      setStatuses((prev) => ({ ...prev, [configId]: 'idle' }));
    }
  }

  if (loading) {
    return <div className="text-center py-8">Loading configurations...</div>;
  }
  if (error) {
    return <div className="text-red-600 text-center py-8">{error}</div>;
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Name</TableHead>
          <TableHead className="text-right">Action</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {configs.length === 0 ? (
          <TableRow>
            <TableCell colSpan={2} className="text-center">No configurations found</TableCell>
          </TableRow>
        ) : (
          configs.map((cfg) => (
            <TableRow key={cfg.id}>
              <TableCell className="font-medium">{cfg.id}</TableCell>
              <TableCell className="text-right">
                <Button
                  variant="ghost"
                  size="icon"
                  disabled={statuses[cfg.id] === 'loading'}
                  onClick={() => handleRun(cfg.id)}
                >
                  {statuses[cfg.id] === 'started' ? (
                    <Check className="h-4 w-4 text-green-500" />
                  ) : (
                    <Play className="h-4 w-4" />
                  )}
                </Button>
              </TableCell>
            </TableRow>
          ))
        )}
      </TableBody>
    </Table>
  );
}
