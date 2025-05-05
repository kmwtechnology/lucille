# Development Plan: Next.js UI for Lucille API

## Phase 1: Project Setup and API Understanding (1-2 weeks)

### 1.1 Project Initialization
```bash
# Create a new Next.js app with TypeScript
npx create-next-app@latest lucille-admin-ui --typescript
cd lucille-admin-ui
npm install axios @tanstack/react-query @mantine/core @mantine/hooks @mantine/form @emotion/react
```

### 1.2 API Research & Documentation
- Review all endpoints from LucilleResource.java:
  - Config management (`/v1/config`)
  - Run management (`/v1/run`) 
  - Health endpoints (`/livez`, `/readyz`)
- Document expected request/response formats
- Create a Postman collection to test API endpoints
- Document authentication requirements

### 1.3 Create TypeScript Interfaces
```typescript
// types/api.ts
export interface LucilleConfig {
  connectors: Connector[];
  pipelines: Pipeline[];
  indexer: Indexer;
  // Additional configuration properties
}

export interface Connector {
  class: string;
  path: string;
  name: string;
  pipeline: string;
  // Other connector properties
}

export interface Pipeline {
  name: string;
  stages: Stage[];
}

export interface Stage {
  name: string;
  class: string;
  // Stage-specific properties
}

export interface Indexer {
  type: string;
  // Indexer-specific properties
}

export interface RunDetails {
  runId: string;
  startTime: string;
  endTime?: string;
  status: 'RUNNING' | 'COMPLETED' | 'FAILED';
  configId: string;
  // Other run details
}
```

## Phase 2: API Client Layer (1 week)

### 2.1 Create API Client
```typescript
// lib/api-client.ts
import axios from 'axios';
import type { LucilleConfig, RunDetails } from '../types/api';

const apiClient = axios.create({
  baseURL: process.env.NEXT_PUBLIC_LUCILLE_API_URL || 'http://localhost:8080/v1',
  timeout: 10000,
});

// Set auth if needed
apiClient.interceptors.request.use((config) => {
  const username = process.env.NEXT_PUBLIC_LUCILLE_USERNAME;
  const password = process.env.NEXT_PUBLIC_LUCILLE_PASSWORD;
  
  if (username && password) {
    config.headers.Authorization = `Basic ${btoa(`${username}:${password}`)}`;
  }
  
  return config;
});

export const configApi = {
  getAll: async () => {
    const response = await apiClient.get('/config');
    return response.data;
  },
  
  getById: async (configId: string) => {
    const response = await apiClient.get(`/config/${configId}`);
    return response.data;
  },
  
  create: async (config: LucilleConfig) => {
    const response = await apiClient.post('/config', config);
    return response.data;
  }
};

export const runApi = {
  getAll: async () => {
    const response = await apiClient.get('/run');
    return response.data;
  },
  
  getById: async (runId: string) => {
    const response = await apiClient.get(`/run/${runId}`);
    return response.data as RunDetails;
  },
  
  start: async (configId: string) => {
    const response = await apiClient.post('/run', { configId });
    return response.data;
  }
};

export const healthApi = {
  getLiveness: async () => {
    const response = await apiClient.get('/livez');
    return response.data;
  },
  
  getReadiness: async () => {
    const response = await apiClient.get('/readyz');
    return response.data;
  }
};
```

## Phase 3: Core UI Components (2 weeks)

### 3.1 Layout & Navigation
- Create app layout with sidebar navigation
- Implement responsive design for desktop and mobile
- Add theme support (light/dark mode)

### 3.2 Configuration Management UI
- Configuration list view
- Configuration detail view
- Configuration creation form with:
  - Connector configuration
  - Pipeline configuration
  - Indexer configuration
  - Validation based on schema

### 3.3 Run Management UI
- Run list view with status indicators
- Run detail view showing:
  - Status
  - Start/end time
  - Associated config
  - Execution metrics (if available)
- Run control buttons (start, stop if supported)

### 3.4 Dashboard
- Overview of system status
- Recent runs
- Available configurations
- Health status indicators

## Phase 4: Advanced Features (2-3 weeks)

### 4.1 Authentication & Authorization
- Implement auth context for storing credentials
- Login screen
- Protected routes
- Token management if API supports it

### 4.2 Configuration Editor
- JSON editor with validation
- Visual pipeline editor
- Templates for common configurations

### 4.3 Run Visualization
- Timeline visualization for run status
- Progress indicators
- Log viewer if logs are available through API

### 4.4 API Health Monitoring
- Real-time health status display
- Connection status indicators
- Error handling and retry mechanisms

## Phase 5: Testing & Deployment (1-2 weeks)

### 5.1 Testing
- Unit tests for API client
- Component tests for UI elements
- Integration tests for form submissions
- End-to-end tests for critical workflows

### 5.2 Optimization
- Performance optimization
- Bundle size reduction
- Lazy loading for large components

### 5.3 Deployment
- Setup CI/CD pipeline
- Production build configuration
- Dockerization option
- Environment configuration for different deployments

### 5.4 Documentation
- User documentation
- API integration documentation
- Deployment instructions

## Technical Considerations

### API Communication
- Use React Query for data fetching, caching, and state management
- Implement proper error handling
- Add retry logic for failed requests

### State Management
- Use React Query for server state
- Use React Context for global UI state
- Use form libraries for complex form state

### UI Framework
- Mantine provides a comprehensive component library with theming
- Consider Chakra UI or Material UI as alternatives

### Authentication
- Implement Basic Auth as shown in the API
- Store credentials securely
- Add option for environment-based configuration

## Getting Started

```bash
# Clone your new repository
git clone https://github.com/yourusername/lucille-admin-ui.git
cd lucille-admin-ui

# Install dependencies
npm install

# Create a .env.local file with your configuration
echo "NEXT_PUBLIC_LUCILLE_API_URL=http://your-api-host:8080/v1" > .env.local
echo "NEXT_PUBLIC_LUCILLE_USERNAME=username" >> .env.local
echo "NEXT_PUBLIC_LUCILLE_PASSWORD=password" >> .env.local

# Start development server
npm run dev
```
