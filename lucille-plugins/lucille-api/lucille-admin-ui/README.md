# Lucille Admin UI

A modern, responsive web interface for managing Lucille data pipelines. Built with Next.js 15 and TypeScript.

## Overview

The Lucille Admin UI is a Next.js application that connects to the Lucille API. It can run in two modes:

- **Development**: UI dev server runs independently on port 3000, connects to API on port 8080
- **Production**: Static site is built and served by the API as a single integrated service

## Prerequisites

- **Node.js** 18+ and **npm** 9+
- **Lucille API** (see [API Setup](#api-setup))
  - For development: API running on `http://localhost:8080`
  - For production: Built static files will be served by the API

## Quick Start

### 1. Install Dependencies

```bash
npm install
```

### 2. Start the Dev Server

```bash
npm run dev
```

The UI will be available at **http://localhost:3000**

The dev server includes:
- Hot module reloading (auto-refresh on file changes)
- TypeScript type checking
- Fast refresh for React components

### 3. Start the API (in another terminal)

From the parent `lucille-api/` directory:

```bash
export LUCILLE_CONF=$(pwd)/conf/simple-config.conf
export DROPWIZARD_CONF=$(pwd)/conf/api.yml
java -Dconfig.file=$LUCILLE_CONF -jar target/lucille-api-plugin.jar server $DROPWIZARD_CONF
```

The API will be available at **http://localhost:8080**

## API Setup

The Admin UI requires the Lucille API to be running. If the API is not yet built:

### Step 1: Generate JavaDoc Documentation

From the parent `lucille/` directory:

```bash
./run-jsondoclet.sh
```

This generates JSON documentation for connectors, stages, and indexers, which the UI displays in the component information modals.

### Step 2: Build the API

From the `lucille-api/` directory:

```bash
mvn clean install
```

This generates the JAR file at `target/lucille-api-plugin.jar` (with the JavaDoc files included)

### Run the API

```bash
export LUCILLE_CONF=$(pwd)/conf/simple-config.conf
export DROPWIZARD_CONF=$(pwd)/conf/api.yml
java -Dconfig.file=$LUCILLE_CONF -jar target/lucille-api-plugin.jar server $DROPWIZARD_CONF
```

For more details, see [../README.md](../README.md)

## Available Scripts

### Development

```bash
npm run dev
```

Starts the Next.js development server with hot reloading on port 3000.

### Production Build

```bash
npm run build
```

Creates an optimized static export in the `out/` directory. The app is exported as static HTML/CSS/JS files ready to be served.

### Serving the Built UI

The built static files can be served in two ways:

**Option 1: Serve Built Files Directly** (current approach)

```bash
# Build the UI
npm run build

# Serve the built files
npx serve out -l 3000
```

This serves the static build on port 3000. The UI will connect to the API on port 8080.

In another terminal, run the API:

```bash
cd ../lucille-api
export LUCILLE_CONF=$(pwd)/conf/simple-config.conf
export DROPWIZARD_CONF=$(pwd)/conf/api.yml
java -Dconfig.file=$LUCILLE_CONF -jar target/lucille-api-plugin.jar server $DROPWIZARD_CONF
```

**Option 2: Integrate UI with API** (requires implementation)

To serve the UI directly from the API as a single service:

1. Create `src/main/resources/public/` directory in lucille-api
2. Copy built UI files: `cp -r out/* ../lucille-api/src/main/resources/public/`
3. Configure Dropwizard to serve static files from the resources directory
4. Rebuild the API: `mvn clean install`

This would enable single-service deployment where all requests (UI + API) go through port 8080.

### Lint Code

```bash
npm run lint
```

Runs ESLint to check code quality.

## How API and UI Work Together

### Development Architecture

```
┌─────────────────────────────────────────┐
│         Browser (port 3000)             │
│    Lucille Admin UI Dev Server          │
│   (Next.js with hot reloading)          │
└─────────────────────────────────────────┘
            ↓ API calls ↓
┌─────────────────────────────────────────┐
│    Lucille API (port 8080)              │
│  (Dropwizard REST API)                  │
│  - Configurations                       │
│  - Pipeline Runs                        │
│  - Component Metadata                   │
│  - System Stats                         │
└─────────────────────────────────────────┘
```

**CORS is enabled** on the API to allow the dev server (running on a different port) to communicate with the API.

### Production Architecture

```
┌─────────────────────────────────────┐
│   Browser                           │
└─────────────────────────────────────┘
             ↓ HTTP ↓
┌─────────────────────────────────────┐
│  Lucille API (port 8080)            │
│  ┌─────────────────────────────────┐│
│  │  Static UI Files (from build)   ││
│  │  - HTML, CSS, JS                ││
│  └─────────────────────────────────┘│
│  ┌─────────────────────────────────┐│
│  │  REST API Endpoints             ││
│  │  - /v1/config, /v1/run, etc.    ││
│  └─────────────────────────────────┘│
└─────────────────────────────────────┘
```

**Single service deployment**: The API serves both the UI static files and the REST API endpoints. No separate frontend service needed.

## Project Structure

```
lucille-admin-ui/
├── app/                      # Next.js app directory (routes & layouts)
│   ├── page.tsx             # Dashboard home page
│   ├── layout.tsx           # Root layout
│   └── (sections)/          # Feature sections (configs, runs, docs)
├── components/              # Reusable React components
│   ├── ui/                  # Base UI components (buttons, cards, etc.)
│   ├── dashboard/           # Dashboard-specific components
│   ├── config-builder/      # Configuration builder components
│   └── ...
├── lib/                     # Utility functions
│   ├── api.ts              # API client for Lucille API
│   └── utils.ts            # Common utilities
├── hooks/                   # Custom React hooks
├── next.config.js          # Next.js configuration
├── tsconfig.json           # TypeScript configuration
└── package.json            # Dependencies & scripts
```

## Features

- **Real-time Dashboard**: View API health, system resources, and pipeline statistics
- **Configuration Builder**: Create and manage Lucille pipeline configurations
- **Run Management**: Start pipeline runs and monitor execution progress
- **Component Documentation**: Browse available connectors, stages, and indexers
- **JavaDocs Viewer**: View auto-generated documentation for configuration parameters
- **Responsive Design**: Works on desktop and tablet devices
- **Dark Mode**: Toggle between light and dark themes

## Environment Configuration

The UI connects to the API at `http://localhost:8080` by default. This is hardcoded in the API client.

To change the API endpoint, modify the `baseUrl` in `lib/api.ts`:

```typescript
const API_BASE_URL = 'http://localhost:8080';
```

## Troubleshooting

### Asset Loading Issues (404 errors)

If you see 404 errors for `/_next/static/...` assets:

1. **Check the build configuration**: Verify `next.config.js` has `basePath: ''` and `assetPrefix: ''`
2. **Clear Next.js cache**: Delete `.next/` and `out/` directories
3. **Rebuild**: Run `npm run build` again
4. **For dev server**: Run `npm run dev` (no caching issues)

### API Connection Issues

If the UI shows "Loading..." or "Checking..." states:

1. **Verify the API is running**: Check `http://localhost:8080/v1/livez`
2. **Check CORS**: The API must have CORS enabled (enabled by default)
3. **Check network tab**: Open browser DevTools and check Network tab for failed requests
4. **Verify ports**: API should be on 8080, UI on 3000

### Development Server Won't Start

If `npm run dev` fails:

1. **Kill existing processes**:
   ```bash
   lsof -ti:3000 | xargs kill -9  # macOS/Linux
   ```
2. **Clear cache**: `rm -rf .next node_modules && npm install && npm run dev`
3. **Check Node version**: `node --version` (requires 18+)

## Static vs. Development

### Development Mode (`npm run dev`)

- Live hot reloading
- Fast refresh for React components
- TypeScript checking
- Better debugging experience
- Used for active development

### Production Mode (`npm run build` + `npx serve out`)

- Static HTML/CSS/JS export
- No server-side rendering
- Optimized for performance
- Used for deployment

## Dependencies

Key technologies:

- **Next.js 15**: React framework with file-based routing
- **TypeScript**: Type-safe JavaScript
- **Tailwind CSS**: Utility-first CSS framework
- **Radix UI**: Accessible UI component library
- **React Hook Form**: Form state management
- **Recharts**: Data visualization
- **Zod**: Schema validation

For full dependency list, see `package.json`

## API Documentation

The Lucille API provides comprehensive endpoints for managing configurations and runs.

### Interactive Swagger UI

Access the interactive API documentation at:

```
http://localhost:8080/swagger
```

### Key Endpoints

- `GET /v1/livez` - Service liveness check
- `GET /v1/readyz` - Service readiness check
- `GET /v1/config` - List all configurations
- `POST /v1/config` - Create new configuration
- `GET /v1/run` - List all pipeline runs
- `POST /v1/run` - Start new pipeline run
- `GET /v1/systemstats` - System resource usage

For detailed endpoint documentation, see [api.md](./api.md)

## Contributing

When making changes to the UI:

1. Use the dev server for active development
2. Run `npm run lint` to check code quality
3. Build and test with `npm run build` before committing
4. Keep components modular and reusable

## Support

For issues or questions:

1. Check the [Lucille documentation](https://kmwtechnology.github.io/lucille/docs/)
2. Review the [API documentation](../README.md)
3. Check this README's troubleshooting section
