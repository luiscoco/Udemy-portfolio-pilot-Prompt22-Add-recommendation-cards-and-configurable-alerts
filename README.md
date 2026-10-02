# Recommendation Cards and Configurable Alerts

This learning activity adds a usable research and alert experience to PortfolioPilot, a teaching
application for managing USD stock portfolios. It builds on the news ingestion, portfolio impact,
authentication, and streaming features from earlier milestones.

**Status:** implemented and verified in mock mode on 2026-10-02. Mock mode uses invented news and
deterministic analysis, so students can run the activity without market-data or AI credentials.
The application offers research steps; it does not place trades.

## 1. Purpose and Learning Objectives

The implementation prompt asked the coding agent to:

- Build recommendation cards showing affected holdings, proposed research actions, evidence,
  caveats, source timestamps, and expandable explanations.
- Persist save/dismiss actions and provide recommendation history for each signed-in user.
- Add user-configured alert rules for news categories, watched securities, concentration, and relevance.
- Store in-app notifications, deliver updates through the existing outbox and SSE connection,
  prevent duplicate notifications, and apply cooldowns to noisy feeds.
- Recover notifications after reconnect, keep dismissals, protect private details from other users,
  and show an honest empty state when no relevant news produces a research action.

These features matter because news streams can repeat messages and browsers can disconnect.
Students learn how to make a notification durable without treating every network delivery as a
new event. They also learn to separate financial calculations, public article analysis, private
portfolio context, and user preferences.

### Terms Used in This Activity

| Term | Meaning |
| --- | --- |
| Fixture | Invented input used to demonstrate or test a feature safely and repeatably. |
| Deterministic | The same inputs produce the same result; numeric threshold checks do not ask an AI model to decide. |
| Migration | A versioned change to the database structure. Prisma applies these changes in order. |
| Transactional outbox | A database table that stores an event in the same transaction as its business change, so the event is not lost if delivery fails. |
| SSE | Server-Sent Events: a browser connection that receives updates from the server. |
| Deduplication | Recognizing repeated delivery of the same event and avoiding a second business result. |
| Revision | A version of a rule or accepted article content. A correction can create a new article revision. |
| Cooldown | A configured interval that suppresses additional notifications from the same rule. |
| Owner-scoped | A read or write is restricted to the user who owns the record. |

## 2. Implementation Steps Actually Performed

### Step 1 — Inspect the Existing Project and Delivery Pipeline

The agent read [AGENTS.md](AGENTS.md), [project state](docs/project-state.md), the milestone plan,
and the existing research, authentication, outbox, streaming, and UI code. Milestone 21 already
stored private recommendations with evidence that could become stale after a correction.

The implementation reused that service. It kept a user's save/dismiss choice separate from the
recommendation's evidence status. Installed Prisma 7.10.0 types and Next.js 16.3.8 route-handler
documentation were checked, along with their official documentation.

### Step 2 — Define Contracts and Deterministic Matching Rules

Created:

- `packages/contracts/src/alerts.ts`: validated alert inputs, notification responses, and recommendation dispositions.
- `packages/contracts/test/alerts.test.ts`: rejection of invalid thresholds, categories, cooldowns, and injected user IDs.
- `packages/domain/src/alerts.ts` and `alerts.test.ts`: pure alert matching and exact decimal boundary tests.

Updated research and event contracts to support saved/dismissed history and `research.updated` events.
A contract is the agreed shape of data passed between the API, worker, and browser.

Key matching decisions:

- Separate filters combine with **AND**. Choices within a category or security list combine with **OR**.
- An empty category/security list means all news relevant to the user's current holdings or watchlist.
- Thresholds are decimal strings between `0` and `1`; `"0.2"` means 20%. Equality passes the threshold.
- Concentration uses the largest affected holding's fraction of its portfolio.
- Relevance uses affected value divided by value across the user's active portfolios.
- Watchlist-only news has no numeric exposure and cannot satisfy a numeric threshold, even zero.

### Step 3 — Persist Rules, Notifications, and User Actions

Modified `packages/db/prisma/schema.prisma` and generated the Prisma client. Added two migrations:

```text
20261009100000_recommendation_alerts
20261009110000_alert_constraint_names
```

The second aligned new database constraint/index names and foreign-key update behavior with Prisma.
An initial deployment attempt failed before the dedicated database had been created. After creating
the database through the PostgreSQL client, migration deployment succeeded.
All 13 project migrations were applied to the local milestone database: the first 12 on the fresh
database, followed by migration 13.

Created `packages/db/src/alert-service.ts` and updated `research-service.ts` and `repositories.ts`.
Notification uniqueness includes the user, rule ID, rule revision, article ID, and accepted article
revision. Database row locks serialize concurrent cooldown decisions and rule edits.

Suppressed notifications are recorded so a later replay cannot make them visible. Rule edits keep
the existing cooldown; deleting a rule retains notification history. Recalculating research does
not reset a saved or dismissed recommendation.

### Step 4 — Add Authenticated APIs and Worker Processing

Added these API routes:

| Route | Operations |
| --- | --- |
| `/api/alerts/rules` | List and create rules |
| `/api/alerts/rules/[id]` | Update or delete a rule |
| `/api/alerts/notifications` | Read stored visible notification history |
| `/api/alerts/notifications/[id]` | Persist dismissal |
| `/api/recommendations/[id]` | Save, unsave, dismiss, or restore a recommendation |

Updated API authorization and browser-event mapping. The server derives the user from the session;
requests do not provide a user ID to select another person's data.

Updated `apps/worker/src/index.ts` and `outbox.ts`. Before acknowledging a stored `news.available`
event, the outbox worker rechecks the owner's interests, reuses public article analysis, persists
private research, and evaluates alert rules. Notification writes and their delivery events share
a database transaction. SSE carries only resource identity and change kind; the browser refetches
the authorized records.

The worker manifest and lockfile gained dependencies on existing local agent/contracts workspaces.
No resolved dependency versions were upgraded.

### Step 5 — Build Cards, History, and Alert Configuration

Created `apps/web/src/alerts.tsx`. Updated `app.tsx`, `research.tsx`, `styles.css`, and the browser
stream manager. **Research & alerts** opens `/research`; news detail also uses the recommendation cards.

Cards show holdings or watched securities, research actions, safe source links, publication times,
as-of time, and expandable caveats. Users can save, dismiss, restore, and browse history. Stream
reconnect/reset refreshes recommendation, rule, and notification reads from PostgreSQL.

Updated `apps/worker/src/fixture.ts` to use fictional guidance changes that produce material
research in the mock analyzer. The synthetic development provider was added to the evidence allowlist.

### Step 6 — Verify and Record the Milestone

Created `apps/api/lib/alerts.integration.test.ts` and `apps/web/e2e/alerts.spec.ts`, and extended
stream reconciliation tests. The agent ran installation, migration, typecheck, build, unit/integration,
browser, and browser-dependency checks. It reviewed a 390px-wide browser screenshot.

The first acceptance attempt used a neutral fixture that correctly produced no recommendation;
the demonstration fixture was changed to fictional guidance news. A test initially supplied an
invalid replay cursor; it was corrected to capture a real cursor before delivery. Final checks passed.

Updated [lesson 22](docs/lessons/22-recommendation-cards-and-alerts.md),
[ADR 0015](docs/decisions/0015-durable-in-app-alerts.md), versions, and project state.
ADR means Architecture Decision Record: a document explaining a significant design choice.

## 3. Results Achieved and Observed Verification

The implemented path is:

```text
Relevant fixture -> news ingestion -> durable news event -> shared article analysis
                 -> private recommendations -> rule evaluation -> stored notification
                 -> outbox delivery -> SSE update -> authorized browser refetch
```

The verified tests observed:

- Duplicate injection and concurrent/repeated evaluation produced one stored notification for the
  matching rule/event revision. One notification can reference several research cards.
- Repeated delivery did not create another recommendation identity or reset dismissal.
- Saved and dismissed recommendations were recovered from private history.
- News injected while the browser was offline appeared once after reconnect.
- Alert dismissal survived a browser reload.
- Bob could not read Alice's private records or mutate her recommendation/rule/notification IDs.
- Expand/collapse interaction and a 390px layout without horizontal overflow worked in Chrome.

### Recorded Check Results — 2026-10-02

| Check | Observed result |
| --- | --- |
| Offline dependency installation | Passed; 0 vulnerabilities reported; workspace links restored |
| `npm run typecheck` | All workspaces passed |
| `npm run build` | Packages, React/Vite, Next.js API, and worker passed with warnings listed below |
| Full test suite with `ALERT_TEST_DATABASE_URL` set | **251 passed; 68 skipped** |
| Milestone PostgreSQL/Redis/API acceptance | **4/4 passed**; included in the 251 total |
| Chrome end-to-end test | **1/1 passed**; separate from the 251 total |
| `npm run check:browser-boundary` | Passed; browser dependencies stayed browser-safe |
| Prisma generation and migration deployment | Passed; all 13 migrations applied locally |

The 68 skipped tests belong to other opt-in infrastructure suites: 57 API tests and 11 worker tests.
They were not counted as passes. The browser screenshot was saved at
`apps/web/test-results/alerts-mobile.png` during verification.

## 4. How to Run the Activity

### Prerequisites

- Node.js **24.21.0** and npm **11.19.0** (the repository pins the Node 24/npm 11 ranges).
- A reachable local PostgreSQL instance and Redis on `127.0.0.1:6379`.
- Google Chrome for the configured Playwright browser test.
- PowerShell for the commands below. Run commands from the repository root.
- For the acceptance tests, a dedicated database named **`portfolio_m22_verify`** on `127.0.0.1`.
  The verified environment used PostgreSQL port **5546**. Tests enforce the dedicated database name.

No AI or market-data credentials are needed for the mock activity.

### Prepare Dependencies and Local Services

For a fresh student environment:

```powershell
npm ci --ignore-scripts
npm run infra:start
npm run infra:status
```

The infrastructure commands use the root `compose.yaml`. See
[lesson 05](docs/lessons/05-local-infrastructure.md) for its setup. During milestone 22, Docker CLI
access was denied; existing PostgreSQL/Redis services were used directly. These fresh-environment
Docker instructions were not verified during milestone 22.

If port 5546 has no PostgreSQL service, this optional command creates a separate teaching database
container. Do not run it if a service already occupies that port:

```powershell
docker run --detach --name portfolio-pilot-m22-lab --publish 127.0.0.1:5546:5432 --env POSTGRES_USER=portfolio_local --env POSTGRES_PASSWORD=local_only_change_me --env POSTGRES_DB=portfolio_m22_verify postgres:17.6-alpine
```

This is a suggested fresh-environment setup command, not a command successfully run in milestone 22.
The credentials below are local teaching values, not production credentials.

### Terminal 1 — Configure, Migrate, Seed, and Start the App

```powershell
$env:DATABASE_URL='postgresql://portfolio_local:local_only_change_me@127.0.0.1:5546/portfolio_m22_verify'
$env:REDIS_URL='redis://127.0.0.1:6379'
$env:DATA_MODE='mock'
$env:AGENT_MODE='mock'
$env:NODE_ENV='development'
$env:DEMO_AUTH_ENABLED='true'
$env:AUTH_BASE_URL='http://127.0.0.1:5173'
$env:AUTH_SECRET='local-session-verification-secret-only-1234567890'
$env:ALLOW_DEMO_SEED='true'

npm run build:types
npm run build -w @portfolio-pilot/worker
npm run migrate:deploy -w @portfolio-pilot/db
npm run seed:demo -w @portfolio-pilot/db
npm run dev
```

Seeding creates the invented Alice/Bob accounts and portfolio data. The browser app runs on
`http://127.0.0.1:5173`; it forwards `/api` requests to the Next.js API on port 3001.
The API has route handlers, not a second frontend.

### Terminal 2 — Start Outbox Delivery

Copy the environment-variable assignments from Terminal 1 into this terminal first.
PowerShell environment variables are not automatically shared between separate terminals.

```powershell
$env:WORKER_ROLE='outbox'
npm run start -w @portfolio-pilot/worker
```

Keep this worker running for the manual demonstration. The fixture command below performs
ingestion itself, so an additional ingestion worker is not needed for this activity.

### Terminal 3 — Inject Relevant News

Copy the same environment-variable assignments here. Before injection:

1. Open the app and sign in as **Alice Demo**.
2. Add **NOVA / XNAS** to the watchlist if necessary.
3. Open **Research & alerts** and create a rule named `NOVA research`.
4. Select NOVA, leave categories and numeric thresholds empty, and set cooldown to `0`.
   This configuration makes duplicate delivery easy to observe.

Inject the same fixture twice:

```powershell
npm run news:inject -w @portfolio-pilot/worker -- lesson22 1
npm run news:inject -w @portfolio-pilot/worker -- lesson22 1
```

The CLI's expected success message is:

```text
Fixture lesson22 revision 1 committed through ingestion; outbox dispatch delivers it.
```

This example is the message defined in the CLI source. The acceptance tests exercised fixture
ingestion and dispatch programmatically; this exact manual CLI output was not captured as evidence.

Expected manual behavior, corresponding to the verified automated journey:

1. The matching rule gets one notification despite the repeated injection. Research cards appear.
2. Save/dismiss cards and dismiss the alert. Reload; the choices persist. Select
   **History including dismissed** to find dismissed cards.
3. Take the browser offline, inject a new ID such as `lesson22-reconnect`, then reconnect:

   ```powershell
   npm run news:inject -w @portfolio-pilot/worker -- lesson22-reconnect 1
   ```

4. The new notification is recovered once. Sign in as Bob in a separate browser profile to check isolation.
5. To explore correction handling, inject revision 2:

   ```powershell
   npm run news:inject -w @portfolio-pilot/worker -- lesson22 2
   ```

A correction is a new content revision. With a nonzero cooldown, a matching event inside that
interval is durably suppressed and will not become visible merely because time passes or it is replayed.
It is not queued for later delivery. Use a fresh fixture ID if an earlier exercise already used `lesson22`.

## 5. How to Verify the Results

### Type, Build, and Dependency Checks

```powershell
npm run typecheck
npm run check:browser-boundary
```

The boundary check prevents browser code from depending on server-only packages. For a production
build, use a separate terminal with demo authentication disabled and no development `NODE_ENV` override:

```powershell
$env:DATA_MODE='mock'
$env:DEMO_AUTH_ENABLED='false'
Remove-Item Env:NODE_ENV -ErrorAction SilentlyContinue
npm run build
```

This checks compilation and build output. It does not start a production server; production startup
also requires HTTPS and the server authentication settings described in the configuration package.

### Unit and Real Database Acceptance Tests

With PostgreSQL and Redis running and the database migrated:

```powershell
$env:DATA_MODE='mock'
$env:ALERT_TEST_DATABASE_URL='postgresql://portfolio_local:local_only_change_me@127.0.0.1:5546/portfolio_m22_verify'
npm run test

# Run just the four milestone acceptance tests:
npm run test -w @portfolio-pilot/api -- lib/alerts.integration.test.ts
```

The recorded full-suite result was 251 passes and 68 skips. Without `ALERT_TEST_DATABASE_URL`,
the four alert integration tests skip as well; that does not verify database acceptance.

### Browser End-to-End Test

Keep the mock API/web dev servers running on the dedicated database. Stop the separately running
outbox worker for this automated test: the test injects and dispatches events itself.
The worker's compiled files must exist from the earlier build command.

```powershell
$env:ALERT_E2E_DATABASE_URL='postgresql://portfolio_local:local_only_change_me@127.0.0.1:5546/portfolio_m22_verify'
npm run test:browser -w @portfolio-pilot/web -- alerts.spec.ts --workers=1
```

The observed result was `1 passed`. This journey checks offline recovery, notification uniqueness,
save/dismiss persistence, expandable explanations, another user's isolation, and mobile width.
If the environment variable is missing, the browser test intentionally skips.

### npm Launcher Issue Encountered During Implementation

The local nvm launcher rejected npm with `NVM4306` because its delegated script was not trusted.
The agent used the already-installed npm CLI; it did not change nvm configuration:

```powershell
$npmCli='C:/Users/luisc/AppData/Local/Author Software/nvm/installs/v24.21.0/node_modules/npm/bin/npm-cli.js'
node $npmCli run typecheck
node $npmCli run test
```

The milestone's npm commands were invoked using this form. The offline installation commands
actually run were:

```powershell
node $npmCli install --package-lock-only --ignore-scripts --offline --cache .npm-cache
node $npmCli install --ignore-scripts --offline --cache .npm-cache
```

These succeeded with the existing cache and restored workspace links. Students with a working npm
launcher can use the ordinary commands above. Offline installation requires a populated cache.

## 6. Limitations, Skipped Checks, and Unfinished Work

- **Live Claude was not verified:** credentials were absent. Category classification can depend on
  the analyzer; decimal threshold comparisons, deduplication, and cooldown decisions are deterministic.
  Live checking requires server-only `ANTHROPIC_API_KEY`, `AGENT_MODE=claude`, `AGENT_MODEL_ID`,
  and `AGENT_WORKSPACE_DIR`, then the app/outbox worker and a supported real article. No model ID
  is assumed by this README.
- **68 other infrastructure tests were skipped.** The milestone acceptance and browser tests did run.
- **Docker CLI access failed.** Direct connections to existing PostgreSQL and Redis allowed acceptance
  verification. Fresh Docker provisioning commands in this README are setup guidance, not verified results.
- **Build warnings remain:** Vite reported `use client` directive and large-bundle warnings; Next.js
  reported three existing instrumentation warnings about Node APIs during Edge-runtime analysis.
- **Schema comparison is not completely empty:** it still reports pre-existing `NewsRead` foreign-key
  update differences and the intentional SQL-only recommendation GIN index. A GIN index helps search
  array-valued evidence references. No remaining alert-schema drift was reported; comparison SQL was not applied.
- **History reads are bounded:** the latest 50 recommendations and 100 notifications are returned;
  older rows remain stored.
- **Evaluation is news-triggered:** quote changes and rule edits alone do not retroactively notify
  about every stored article. Only current holdings/watchlist interests are considered.
- **Long live analysis shares outbox retry/lease limits.** Failed events can reach `DEAD`, a stored
  state requiring operator requeue after fixing the cause. Independent durable agent execution is milestone 27.
- **Scope remains in-app:** no email, third-party messages, automatic trades, or agent-driven rule edits.
  No public deployment or cloud resources were created. The workspace was not a Git repository, so
  no commit or push was performed. Verification dev servers were stopped after the checks.

## Further Reading

- [Current project state and actual check results](docs/project-state.md)
- [Lesson 22 teaching notes](docs/lessons/22-recommendation-cards-and-alerts.md)
- [ADR 0015: durable in-app alerts](docs/decisions/0015-durable-in-app-alerts.md)
- [ADR 0014: shared analysis and private research](docs/decisions/0014-shared-analysis-private-research.md)
- [Pinned versions](docs/versions.md)
- [Milestone plan](docs/project-plan.md)

The next planned activity is milestone 23. This README update documents milestone 22; it does not
implement the next milestone or claim that the application checks were rerun for this documentation edit.
