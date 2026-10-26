# Lucille Admin UI

A React + Vite single-page app for administering the Lucille API: browse and create configs,
start runs, and watch run and system status. It talks to the Lucille API's `/v1` endpoints and is
served by the API itself at `/admin` in packaged builds.

## Prerequisites

- **Node.js 24.10.0** (see [`.nvmrc`](./.nvmrc)). If you use `nvm`, run `nvm use` in this directory.
  The Maven build installs this exact version automatically, but for local `npm` work you need it
  on your PATH.
- The bundled **npm** (11.x) that ships with Node 24.

You only need Node/npm for day-to-day UI development. To build or serve the UI as part of Lucille,
use the Maven profile described in [Build as part of Lucille](#build-as-part-of-lucille) — that path
installs its own Node, so no local Node is required.

## Quick start (local development)

From this directory (`lucille-plugins/lucille-api/lucille-admin-ui`):

```bash
npm ci          # install exact dependencies from package-lock.json
npm run dev     # start the Vite dev server with hot reload
```

The dev server prints a local URL (default `http://localhost:5173`). Open it in a browser.

### Talking to the API in dev

The app calls the API at `/v1/...` on the same origin. In development, Vite proxies `/v1` to a
locally running Lucille API (see [`vite.config.ts`](./vite.config.ts)):

```
/v1  ->  http://localhost:8080
```

So to exercise the UI against a live backend, start the Lucille API on port `8080` first, then run
`npm run dev`. Without the API running, pages that fetch data will show errors — that is expected.

If your API runs somewhere other than `http://localhost:8080`, update the `proxy` target in
`vite.config.ts`.

## Run the served UI locally

The dev server above is for iterating on the UI. To run the **production, API-served** version
locally — the same way it is served in a release (static assets at `/admin`, no Vite proxy) — build
the UI into the API jar and start the API.

From the repository root:

```bash
# 1. Build the lucille-api jar with the UI bundled in. -Padmin-ui pulls the built UI assets
#    into the jar; frontend-maven-plugin installs its own pinned Node, so no local Node is needed.
mvn -B -Padmin-ui -pl lucille-plugins/lucille-api -am package -DskipTests
```

```bash
# 2. Start the API. conf/api.yml ships with auth disabled, so no credentials are needed.
cd lucille-plugins/lucille-api
java -jar target/lucille-api-plugin.jar server conf/api.yml
```

Then open:

```
http://localhost:8080/admin
```

Because the UI and API are now the same origin, the app's `/v1` calls hit the running API directly
(no proxy involved). Deep links and refreshes such as `http://localhost:8080/admin/configs/<id>`
work via the servlet's `index.html` fallback.

To rebuild after a UI change, re-run step 1 and restart the API.

## Scripts

| Command | What it does |
|---|---|
| `npm run dev` | Start the Vite dev server with hot module reload. |
| `npm run build` | Type-check (`tsc -b`) and produce a production build in `dist/`. A type or syntax error fails this command. |
| `npm run preview` | Serve the already-built `dist/` locally to sanity-check a production build. |
| `npm test` | Run the Vitest unit/component suite once. |
| `npm run test:watch` | Run Vitest in watch mode while developing tests. |
| `npm run test:coverage` | Run the suite and emit a coverage report. |
| `npm run audit:report` | Print `npm audit` results. Non-blocking (never fails). |

## Testing

Tests use [Vitest](https://vitest.dev/) with [React Testing Library](https://testing-library.com/)
in a `jsdom` environment. They are unit and component tests — no network and no running API — so
they run fast and in isolation. Components and pages render against canned data with `fetch`
stubbed; the API itself is never started.

Coverage spans the logic-bearing surface of the app:

- **lib**: `utils` formatting/parsing, `validate-config` HOCON checks, and the `api` client.
- **hooks**: `use-fetch` (including polling), `use-pagination`, and the config/run mutation and
  health-status hooks.
- **components**: `run-status`, `pagination`, `config-summary`.
- **pages**: loading / error / empty / success render states for `configs`, `config-detail`,
  `run-detail`, and `dashboard`.

```bash
npm test              # one-shot, as CI runs it
npm run test:watch    # iterate locally
npm run test:coverage # with a coverage report
```

Test files live under `src/test/`, mirroring the `src/` layout they cover (for example
`src/lib/utils.ts` is tested by `src/test/lib/utils.test.ts`), as `*.test.ts` / `*.test.tsx`.
Shared setup is in `src/test/setup.ts`; reusable helpers (a `renderWithRouter` wrapper and a
`fetch` mock) live in `src/test/helpers/`.

## Build as part of Lucille

The UI is a Maven module (`lucille-admin-ui`) that is **off by default**. A plain `mvn verify` does
not build it, so Java-only work needs no Node toolchain. Activate it with the `admin-ui` profile.

Build and test just the UI module (installs the pinned Node automatically):

```bash
# from the repository root
mvn -B -Padmin-ui -pl lucille-plugins/lucille-api/lucille-admin-ui verify
```

This runs, in order: install Node 24.10.0 locally, `npm ci`, a non-blocking `npm audit`,
`npm run build`, and the Vitest suite, then packages the built assets into the module jar.

## How it is served

In packaged/release builds, the API bundles the UI and serves it. When the `admin-ui` profile is
active, `lucille-api` depends on this module's jar (which carries the built assets under `admin/`),
and `AdminUiServlet` serves them at:

```
http://<api-host>:8080/admin
```

The servlet falls back to `index.html` for client-side routes (for example
`/admin/configs/<id>`), so refreshing or deep-linking into the app works. When the UI is not bundled
(a plain Java build), the API simply does not register the servlet and `/admin` is not served.

## Project layout

```
src/
  App.tsx            # routes (react-router)
  main.tsx           # app entry
  components/        # shared UI components
  hooks/             # data-fetching and UI hooks
  lib/               # api client, helpers, config validation, constants
  pages/             # route screens (dashboard, configs, runs, ...)
  types/             # API response types
  test/              # Vitest setup and all test files, mirroring the src/ layout
```

Configuration lives in `vite.config.ts` (build `base: /admin`, dev `/v1` proxy, Vitest config) and
the `tsconfig*.json` files.
