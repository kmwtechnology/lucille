// API utilities for data fetching

// This would typically connect to your backend API
// For now implementing with mock data but structure is setup for real API integration

export type Config = {
  id: string;
  name: string;
  description: string;
  status: 'active' | 'inactive' | 'draft';
  lastModified: string;
  createdAt: string;
}

export type Run = {
  runId: string;
  configId: string;
  startTime: number;
  endTime?: number;
  runResult?: unknown;
  runType: string;
  done: boolean;
  future?: {
    done?: boolean;
    cancelled?: boolean;
    completedExceptionally?: boolean;
    numberOfDependents?: number;
  };
}

export type SystemHealth = {
  status: 'healthy' | 'degraded' | 'down';
  components: {
    name: string;
    status: 'healthy' | 'degraded' | 'down';
    message?: string;
  }[];
  activeRuns: number;
  totalConfigs: number;
}

// Mock data
const configs: Config[] = [
  {
    id: 'config-1',
    name: 'Production ETL',
    description: 'Main ETL pipeline for production data',
    status: 'active',
    lastModified: '2025-04-07T10:30:00Z',
    createdAt: '2025-03-15T08:45:00Z',
  },
  {
    id: 'config-2',
    name: 'Data Sync',
    description: 'Synchronization between data sources',
    status: 'active',
    lastModified: '2025-04-06T16:20:00Z',
    createdAt: '2025-03-20T09:30:00Z',
  },
  {
    id: 'config-3',
    name: 'Analytics Pipeline',
    description: 'Data processing for analytics',
    status: 'inactive',
    lastModified: '2025-04-05T14:15:00Z',
    createdAt: '2025-03-10T11:00:00Z',
  },
];

const runs: Run[] = [
  {
    runId: 'run-123',
    configId: 'config-1',
    startTime: 1649340600000,
    endTime: 1649341200000,
    runResult: undefined,
    runType: 'run-type-1',
    done: true,
    future: undefined,
  },
  {
    runId: 'run-122',
    configId: 'config-2',
    startTime: 1649340500000,
    endTime: undefined,
    runResult: undefined,
    runType: 'run-type-2',
    done: false,
    future: undefined,
  },
  {
    runId: 'run-121',
    configId: 'config-3',
    startTime: 1649340400000,
    endTime: 1649340500000,
    runResult: undefined,
    runType: 'run-type-3',
    done: true,
    future: undefined,
  },
  {
    runId: 'run-120',
    configId: 'config-1',
    startTime: 1649338200000,
    endTime: 1649338800000,
    runResult: undefined,
    runType: 'run-type-1',
    done: true,
    future: undefined,
  },
];

const healthStatus: SystemHealth = {
  status: 'healthy',
  components: [
    { name: 'API', status: 'healthy' },
    { name: 'Database', status: 'healthy' },
    { name: 'Workers', status: 'healthy' },
  ],
  activeRuns: 3,
  totalConfigs: 12,
};

// API methods with artificial delay to simulate network requests
export async function getConfigs(): Promise<Config[]> {
  await new Promise(resolve => setTimeout(resolve, 500));
  return [...configs];
}

export async function getConfigById(id: string): Promise<Config | null> {
  await new Promise(resolve => setTimeout(resolve, 300));
  return configs.find(config => config.id === id) || null;
}

export async function getRuns(configId?: string): Promise<Run[]> {
  await new Promise(resolve => setTimeout(resolve, 500));
  return configId 
    ? runs.filter(run => run.configId === configId)
    : [...runs];
}

export async function getRunById(id: string): Promise<Run | null> {
  try {
    // Use absolute URL for SSR fetch to internal API
    const baseUrl = process.env.NEXT_PUBLIC_BASE_URL || `http://localhost:${process.env.PORT || 3000}`;
    const res = await fetch(`${baseUrl}/api/lucille/run/${id}`, { cache: 'no-store' });
    if (!res.ok) return null;
    return await res.json();
  } catch (error) {
    console.error('getRunById error', error);
    return null;
  }
}

export async function getSystemHealth(): Promise<SystemHealth> {
  await new Promise(resolve => setTimeout(resolve, 400));
  return {...healthStatus};
}

// Server Actions for mutations (to be implemented in Next.js app)
// These would be actual data modification functions in a real app
export async function createConfig(config: Omit<Config, 'id' | 'createdAt' | 'lastModified'>): Promise<Config> {
  'use server';
  
  await new Promise(resolve => setTimeout(resolve, 700));
  
  const newConfig: Config = {
    id: `config-${Date.now()}`,
    name: config.name,
    description: config.description,
    status: config.status,
    createdAt: new Date().toISOString(),
    lastModified: new Date().toISOString(),
  };
  
  // In a real app, you would save this to a database
  
  return newConfig;
}

export async function updateConfig(id: string, config: Partial<Omit<Config, 'id' | 'createdAt'>>): Promise<Config> {
  'use server';
  
  await new Promise(resolve => setTimeout(resolve, 700));
  
  const existingConfig = configs.find(c => c.id === id);
  if (!existingConfig) {
    throw new Error(`Config with id ${id} not found`);
  }
  
  const updatedConfig: Config = {
    ...existingConfig,
    ...config,
    lastModified: new Date().toISOString(),
  };
  
  // In a real app, you would update this in a database
  
  return updatedConfig;
}
