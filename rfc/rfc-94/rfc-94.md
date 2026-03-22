<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# RFC-94: Hudi Timeline User Interface (UI)

## Proposers

- @voonhous

## Approvers

- @danny0405
- @codope

## Status

JIRA: [HUDI-9315](https://issues.apache.org/jira/browse/HUDI-9315)

## Abstract

Hudi Timeline metadata in the filesystem is currently exposed as timestamped files representing
state transitions of actions such as `commit`, `deltacommit` and `compaction`. While these files are
accessible via the CLI or a file explorer, this method presents significant usability challenges:
it's difficult to visualize concurrent actions, identify missing transitions, or understand the
duration of each step. Manually interpreting sequences of timeline files hinders effective debugging
and monitoring.

To address these limitations, we propose a UI-based timeline visualization tool that can parse these
metadata files, group related actions, and render them in a time-ordered, interactive view. This
interface would enable users to intuitively track the lifecycle of each operation, observe
concurrency patterns, and quickly spot anomalies or long-running tasks. The implementation extends
`hudi-timeline-service` with new `/v2/` REST APIs and a static HTML + JavaScript frontend powered by
[vis-timeline](https://github.com/visjs/vis-timeline), served via Javalin's built-in static file
serving — introducing zero new Java compile-time dependencies. This would significantly improve
observability and operational insight of jobs operating on a Hudi table.

## Background

In our current workflow, we rely on the command-line interface (CLI) or direct filesystem inspection
to understand the internal state of the system through metadata files. These files represent
different actions (e.g., `deltacommit`, `compaction`) and their lifecycle states (`requested`,
`inflight`, `completed`), encoded in file names as such:

```shell
20250409102118815.deltacommit.inflight
20250409102118815.deltacommit.requested
20250409102118815_20250409102124339.deltacommit
20250409102121593.compaction.inflight
20250409102121593.compaction.requested
20250409102121593_20250409102122232.commit
20250409102124581.deltacommit.inflight
20250409102124581.deltacommit.requested
20250409102124581_20250409102125667.deltacommit
20250409102124612.compaction.inflight
20250409102124612.compaction.requested
20250409102124612_20250409102124892.commit
20250409102127348.deltacommit.inflight
20250409102127348.deltacommit.requested
20250409102127348_20250409102128481.deltacommit
20250409102127500.compaction.inflight
20250409102127500.compaction.requested
20250409102127500_20250409102127721.commit
```

While this approach works at a basic level, several challenges arise:

1. No visibility into concurrency
    - Multiple actions (e.g., `deltacommit` and `compaction`) often occur concurrently.
    - CLI does not provide an intuitive way to correlate or visualize overlapping operations.
2. Lack of temporal context
    - Timestamps are embedded in filenames but are hard to compare visually, while year, month and
      day can be quickly determined, it is harder to see the minutes and seconds.
    - No easy way to perceive how long an action took or whether actions are stalling unless users
      manually calculate requested and completion time,
3. Difficult to spot inconsistencies or missing states
    - An `inflight` compaction action without a corresponding `commit` can indicate a starved/stuck
      compaction, which usually blocks archiving/cleaning.
    - The CLI makes it hard to spot such gaps or incomplete transitions easily.

On top of that, we have recently standardized the format of all timeline files such that they are
stored as Avro binaries. As such, inspecting the contents of timeline files will require custom
Avro readers to convert the binaries to a JSON format.

## Scope

The current scope of work is to allow for visualisation of metadata that are available in Hudi
tables.
Hence, all features offered are on a **READ-ONLY** basis.
There will be no feature in this RFC that offers starting/spawning of jobs to mutate a Hudi table.

The following are explicitly **out of scope** for this RFC:

- **Archived timeline:** Only the active timeline is rendered. Loading instants from LSM-based
  archive files is deferred to a future improvement.
- **Metadata table overlay:** The metadata table's own timeline is not shown alongside the main
  table timeline.
- **Write/mutation operations:** The UI cannot trigger compactions, clustering, or any write action.
- **Authentication/authorization:** No access control is introduced. The timeline server is assumed
  to run in a trusted network, consistent with its current usage.

## Implementation

With being light-weighted as one of the key priorities, implementation should introduce as little
dependencies as possible to reduce bloat. Changes will be made to the existing
`hudi-timeline-service` module. This module contains a Javalin web-application that caches useful
filesystem metadata of a Hudi table for job executors whenever tagging/writing is performed.

Therefore, to use the Hudi Timeline UI, users are only required to start the Timeline Server in
**STANDALONE** mode, which is already supported as of now.

The Hudi Timeline UI can be split into two large sections. The frontend and backend.

### Architecture

The overall request flow is as follows:

```
┌─────────┐       HTTP        ┌──────────────────────────────────┐
│ Browser │ ◄───────────────► │     Javalin (Timeline Server)    │
└─────────┘                   │                                  │
                              │  /ui/*  ──► Static Files         │
                              │             (HTML, JS, CSS)      │
                              │                                  │
                              │  /v2/timeline/* ──► UiHandler    │
                              │        │                         │
                              │        ▼                         │
                              │  FileSystemViewManager           │
                              │        │                         │
                              │        ▼                         │
                              │  HoodieTimeline / MetaClient     │
                              └──────────────────────────────────┘
```

There are two categories of requests:

1. **Static file requests** (`/ui/*`) — Javalin serves HTML, JavaScript, and CSS files from the
   classpath (`src/main/resources/public/`). No server-side rendering or template engine is needed.
2. **REST API requests** (`/v2/timeline/*`) — A new `UiHandler` processes these requests, reading
   timeline data from the `FileSystemViewManager` and `HoodieTableMetaClient`, then returning JSON
   responses.

### Frontend

The frontend is implemented as static HTML pages with vanilla JavaScript, similar to how the Spark
Web UI works. Javalin's built-in static file serving is used to serve files from the classpath —
no template engine (e.g., Thymeleaf) is needed. This eliminates any new Java compile-time
dependencies for the frontend.

No frontend build pipeline (npm, webpack, vite) is introduced. Contributing to the UI requires only
a text editor. The only external library is vis-timeline for timeline rendering.

#### File Structure

```
hudi-timeline-service/src/main/resources/public/
├── index.html                     # Landing page with basepath input form
├── js/
│   └── timeline.js                # vis-timeline initialization and REST API calls
├── css/
│   └── style.css                  # Basic styling
└── lib/
    └── vis-timeline/              # Bundled fallback copy of vis-timeline
        ├── vis-timeline-graph2d.min.js
        └── vis-timeline-graph2d.min.css
```

#### JavaScript Delivery: CDN with Bundled Fallback

The vis-timeline library is loaded using a two-tier strategy:

1. **Primary:** Load vis-timeline from the `unpkg.com` CDN. This provides automatic patch updates
   and avoids bundling a large asset for the common case.
2. **Fallback:** If the CDN is unreachable (e.g., air-gapped environments), load from the bundled
   copy at `/lib/vis-timeline/`.

Trade-off: The CDN approach gives users the latest patches automatically. The bundled fallback adds
~300KB to the JAR but ensures the UI works in environments without internet access.

#### vis-timeline Configuration

The timeline is configured with groups and items that map directly to Hudi's timeline model:

- **Groups:** One row per action type — `commit`, `deltacommit`, `compaction`, `clean`, `rollback`,
  `clustering`, `savepoint`, `logcompaction`, `indexing`, `restore`, `replacecommit`. These
  correspond to the actions defined in `HoodieTimeline.VALID_ACTIONS_IN_TIMELINE`.
- **Items:** Completed instants are rendered as range bars spanning from `requestedTime` to
  `completionTime`. Non-completed instants (requested or inflight) are rendered as point items at
  `requestedTime`.
- **Color coding:** Items are colored by state:
    - Green → `COMPLETED`
    - Yellow → `INFLIGHT`
    - Red → `REQUESTED`
- **Tooltip:** On hover, a tooltip displays the action type, requested time, completion time, and
  duration (completion time minus requested time).
- **Click handler:** Clicking an instant fetches its detail via
  `/v2/timeline/instant/details` and displays the deserialized JSON content in a `<pre>` detail
  panel below the timeline.

### Backend

As of now, a `hudi-timeline-service` instance is able to serve filesystem metadata for multiple
table basePath as the `FileSystemView`s are cached in a map that is keyed by the basepath.

As such, we extend this module with a set of `/v2/` APIs to serve the timeline metadata that
is required for the UI.

#### API Specification

| Method | Path                           | Parameters                                                           | Response                | Description                                                                         |
|--------|--------------------------------|----------------------------------------------------------------------|-------------------------|-------------------------------------------------------------------------------------|
| GET    | `/v2/timeline/instants`        | `basepath` (required)                                                | `List<InstantDTO>` (v2) | Returns all active instants with requested time, completion time, action, and state |
| GET    | `/v2/timeline/instant/details` | `basepath` (required), `instantTime` (required), `action` (required) | JSON string             | Returns the deserialized content of a specific instant's metadata (Avro → JSON)     |

Static files are served from the `/ui/` path, mapped to classpath resources under
`src/main/resources/public/`.

#### DTO Design

A new v2 `InstantDTO` is introduced in a `v2` package to avoid modifying the existing `/v1/` API
contract. The existing `InstantDTO` (in `o.a.h.common.table.timeline.dto`) only exposes `action`,
`timestamp` (requested time), and `state` — it lacks `completionTime`, which is essential for
rendering range bars on the timeline.

The v2 `InstantDTO` contains:

- `requestedTime` — the instant's requested timestamp (`HoodieInstant.requestedTime()`)
- `completionTime` — the instant's completion timestamp (`HoodieInstant.getCompletionTime()`), null
  for non-completed instants
- `action` — the action type (e.g., `commit`, `deltacommit`, `compaction`)
- `state` — the instant state (`REQUESTED`, `INFLIGHT`, `COMPLETED`)

#### Handler Design

A new `UiHandler` class is introduced, extending `Handler` (following the `InstantStateHandler`
pattern). It provides two methods:

1. `getActiveInstants(basePath)` — Retrieves the active timeline via
   `viewManager.getFileSystemView(basePath).getTimeline()`, iterates over all instants, and maps
   each `HoodieInstant` to a v2 `InstantDTO` including completion time.

2. `getInstantDetails(basePath, instantTime, action)` — Constructs a `HoodieTableMetaClient` for
   the given basepath, reads the instant's Avro content via `ActiveTimeline.getInstantDetails()`,
   and deserializes it to a JSON string for display in the UI.

#### Registration in RequestHandler

The `UiHandler` is registered in `RequestHandler` following the existing pattern:

- A `UiHandler` field is added alongside the existing handler fields.
- A `registerUiAPI()` method is added and called from `register()`.
- Registration is gated behind an `--enable-ui` config flag, following the pattern used by
  `enableMarkerRequests` and `enableInstantStateRequests`.

#### Error Handling

- **Invalid basepath** → HTTP 400 with a descriptive error message (e.g., "Not a valid Hudi table
  path").
- **Empty timeline** → Returns an empty list `[]`. The frontend displays "No instants found".
- **Failed instant detail read** → HTTP 500 with error details (e.g., Avro deserialization failure).

### Feature

The main feature that we are offering users as a first cut is to visualise the timeline for a Hudi
table.

The main permitted user actions are:

1. User is able to input a Hudi table basepath
2. User is able to click submit after inputting Hudi table basepath
3. The timeline of the Hudi table is rendered
4. User is able to scroll through timeline (horizontally)
5. User is able to zoom in and out of timeline
6. User is able to hover over instant for more details
7. User is able to click on a specific instant and the JSON string of the timeline details are
   rendered

The visual encoding of the timeline is as follows: each action type occupies its own horizontal
group (row) so that concurrent actions across different types are visually separated. Completed
instants appear as horizontal bars whose width represents duration (requested → completed). Inflight
and requested instants appear as point markers. Color distinguishes state at a glance: green for
completed, yellow for inflight, red for requested.

### Examples

Attached below are proof of concept (PoC) snapshots

**Main Page with Timeline Rendered**
![timeline_main](images/timeline_main.png)

**Hovering Over an Instant**
![timeline_hover](images/timeline_hover.png)

**Selecting an Instant**
![timeline_select](images/timeline_instant_select.png)

## Configuration

To start the Timeline Server in standalone mode with the UI enabled:

```shell
java -cp hudi-timeline-server-bundle-*.jar \
  org.apache.hudi.timeline.service.TimelineService \
  --server-port 26754 \
  --enable-ui
```

Once started, the UI is accessible at `http://localhost:26754/ui/`.

The server port is configurable via the existing `--server-port` (or `-p`) flag (default: `26754`).
The `--enable-ui` flag controls whether the UI static files and `/v2/timeline/` API endpoints are
registered. When the flag is not set, the timeline server behaves exactly as it does today —
no UI-related routes are added.

## Dependency Impact

- **Zero new Java compile-time dependencies.** The frontend uses Javalin's built-in static file
  serving; no template engine (Thymeleaf or otherwise) is added.
- **vis-timeline JS/CSS:** ~300KB bundled as static resources under
  `src/main/resources/public/lib/vis-timeline/`.
- **No impact on Spark/Flink bundles.** The `hudi-timeline-server-bundle` is a separate artifact;
  adding static resources to it does not affect engine-specific bundles.
- **No frontend build pipeline.** No npm, webpack, or vite is introduced. The JS/CSS files are
  committed directly and served as-is.

## Future Improvements

There are other features/improvements that we can add to the UI. For example:

1. **Visualising incremental clean range** — When an incremental clean is performed, render a
   translucent overlay spanning from `lastCompletedCommitTimestamp` to `earliestCommitToRetain`.
   This would give operators an at-a-glance view of which instants are protected from cleaning and
   which are eligible for removal.

2. **Visualising partition/filegroup level write heatmap** — Within one or multiple instants, render
   a grid colored by write volume, parsed from `HoodieCommitMetadata.partitionToWriteStats`. This
   allows users to identify skewed partitions or heavily-modified filegroups across commits.

3. **Overlay metadata table timeline** — Render the metadata table's own timeline alongside the main
   table timeline. This would help diagnose issues where the metadata table is lagging behind or has
   stale entries relative to the main table.

4. **Visualising archived timeline** — Load and render instants from the LSM-based archive files.
   This extends the timeline view beyond the active timeline, allowing users to inspect historical
   actions that have been archived.

## Rollout/Adoption Plan

- **Backward compatibility:** This is an additive change only. All existing `/v1/` endpoints remain
  unchanged. The UI endpoints are registered under a new `/v2/timeline/` prefix, and static file
  serving is added at `/ui/`. No existing behavior is modified.
- **Release target:** 1.1.x
- **Documentation:** A new page will be added to hudi.apache.org under the "Operations" section,
  including screenshots, a startup guide, and a debugging walkthrough demonstrating how to use the
  timeline UI to diagnose common issues (stuck compactions, long-running commits, etc.).
- **No migration needed:** As a net-new feature behind an opt-in flag (`--enable-ui`), there is no
  impact on existing users or running systems.

## Test Plan

### Unit Tests

- **`UiHandler.getActiveInstants()`**: Verify correct mapping from `HoodieInstant` to v2
  `InstantDTO`, including:
    - All action types in `HoodieTimeline.VALID_ACTIONS_IN_TIMELINE` are mapped correctly.
    - `completionTime` is populated for completed instants and null for requested/inflight instants.
    - Instants are returned in timeline order.
- **`UiHandler.getInstantDetails()`**: Verify correct Avro deserialization to JSON for various
  action types (commit metadata, compaction plan, clean plan, etc.).
- **Error cases**: Invalid basepath returns an appropriate error. Empty timeline returns an empty
  list.

### Integration Tests

Following the pattern established by `TestTimelineService.java`:

- Start `TimelineService` with the UI enabled (`--enable-ui`).
- Create synthetic instants via `HoodieTestUtils` covering multiple action types and states.
- Verify `GET /v2/timeline/instants` returns the expected list of instants with correct fields.
- Verify `GET /v2/timeline/instant/details` returns valid JSON for a completed instant.
- Verify `GET /ui/` returns HTTP 200 with HTML content (static file serving works).

### Manual Testing Checklist

- Timeline renders correctly for tables with various action types (`commit`, `deltacommit`,
  `compaction`, `clean`, `rollback`, `clustering`).
- Concurrent actions are visually distinguishable across separate group rows.
- Zoom and scroll work smoothly with 100+ instants on the timeline.
- Detail panel shows correct JSON content when clicking on a completed instant.
- Hover tooltip displays action, requested time, completion time, and duration.
- Empty table basepath shows "No instants found" message gracefully.
- Invalid basepath shows a user-friendly error message.
- CDN fallback: UI loads correctly when the CDN is unreachable (using bundled vis-timeline).
