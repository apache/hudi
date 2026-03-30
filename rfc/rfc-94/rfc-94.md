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
- @rahil-c
- @yihua

## Status

JIRA: [HUDI-9315](https://issues.apache.org/jira/browse/HUDI-9315)

## Abstract

Hudi Timeline metadata is stored as timestamped files representing state transitions of actions like
`commit`, `deltacommit` and `compaction`. These files are accessible via the CLI or a file explorer,
but it's hard to visualize concurrent actions, spot missing transitions, or tell how long each step
took. Debugging timeline issues by reading filenames is tedious.

This RFC proposes a UI-based timeline visualization tool that parses these metadata files, groups
related actions, and renders them in a time-ordered, interactive view. Users can track the lifecycle
of each operation, see concurrency patterns, and spot anomalies or long-running tasks. The
implementation extends `hudi-timeline-service` with new `/v2/` REST APIs and a static HTML +
JavaScript frontend powered by [vis-timeline](https://github.com/visjs/vis-timeline), served via
Javalin's built-in static file serving with zero new Java compile-time dependencies.

## Background

Today, we rely on the CLI or direct filesystem inspection to understand timeline state through
metadata files. These files represent different actions (e.g., `deltacommit`, `compaction`) and
their lifecycle states (`requested`, `inflight`, `completed`), encoded in file names like:

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

This works, but has a few problems:

1. No visibility into concurrency
    - Multiple actions (e.g., `deltacommit` and `compaction`) often run concurrently.
    - The CLI doesn't help correlate or visualize overlapping operations.
2. Lack of temporal context
    - Timestamps are embedded in filenames but are hard to compare visually - year, month and
      day can be quickly determined, but minutes and seconds are harder to parse.
    - No easy way to tell how long an action took or whether it's stalling unless you
      manually calculate the difference between requested and completion time.
3. Hard to spot inconsistencies or missing states
    - An `inflight` compaction without a corresponding `commit` can indicate a starved/stuck
      compaction, which usually blocks archiving/cleaning.
    - These gaps are easy to miss when scanning filenames.

On top of that, all timeline files are now stored as Avro binaries. Inspecting their contents
requires custom Avro readers to convert the binaries to JSON.

## Scope

This RFC covers visualization of metadata available in Hudi tables. All features are **READ-ONLY** -
there is no support for starting or spawning jobs that mutate a Hudi table.

The following are **out of scope**:

- **Archived timeline:** Only the active timeline is rendered. Loading instants from LSM-based
  archive files is left for future work.
- **Metadata table overlay:** The metadata table's own timeline is not shown alongside the main
  table timeline.
- **Write/mutation operations:** The UI cannot trigger compactions, clustering, or any write action.
- **Authentication/authorization:** No access control is added. The timeline server is assumed to
  run in a trusted network, same as today.

## Implementation

Keeping the implementation lightweight is a priority - we should add as few dependencies as
possible. Changes go into the existing `hudi-timeline-service` module, which contains a Javalin
web-application that caches filesystem metadata of a Hudi table for job executors during
tagging/writing.

To use the Hudi Timeline UI, users just need to start the Timeline Server in **STANDALONE** mode,
which is already supported.

The Hudi Timeline UI has two parts: the frontend and backend.

### Architecture

The overall request flow is as follows:

```
┌─────────┐       HTTP        ┌──────────────────────────────────┐
│ Browser │ <---------------> │     Javalin (Timeline Server)    │
└─────────┘                   │                                  │
                              │  /ui/*  --> Static Files         │
                              │             (HTML, JS, CSS)      │
                              │                                  │
                              │  /v2/timeline/* --> UiHandler    │
                              │        │                         │
                              │        V                         │
                              │  FileSystemViewManager           │
                              │        │                         │
                              │        V                         │
                              │  HoodieTimeline / MetaClient     │
                              └──────────────────────────────────┘
```

There are two categories of requests:

1. **Static file requests** (`/ui/*`) - Javalin serves HTML, JavaScript, and CSS files from the
   classpath (`src/main/resources/public/`). No server-side rendering or template engine is needed.
2. **REST API requests** (`/v2/timeline/*`) - A new `UiHandler` processes these requests, reading
   timeline data from the `FileSystemViewManager` and `HoodieTableMetaClient`, then returning JSON
   responses.

### Frontend

The frontend is static HTML pages with vanilla JavaScript, similar to the Spark Web UI. Javalin's
built-in static file serving handles files from the classpath - no template engine (e.g.,
Thymeleaf) is needed and no new Java compile-time dependencies are added.

No frontend build pipeline (npm, webpack, vite) is needed. Contributing to the UI requires only a
text editor. The only external library is vis-timeline for timeline rendering.

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

The vis-timeline library is loaded with a two-tier strategy:

1. **Primary:** Load from the `unpkg.com` CDN for automatic patch updates.
2. **Fallback:** If the CDN is unreachable (e.g., air-gapped environments), load from the bundled
   copy at `/lib/vis-timeline/`.

The bundled fallback adds ~300KB to the JAR but ensures the UI works without internet access.

#### vis-timeline Configuration

The timeline is configured with groups and items that map to Hudi's timeline model:

- **Groups:** One row per action type - `commit`, `deltacommit`, `compaction`, `clean`, `rollback`,
  `clustering`, `savepoint`, `logcompaction`, `indexing`, `restore`, `replacecommit`. These
  correspond to the actions in `HoodieTimeline.VALID_ACTIONS_IN_TIMELINE`.
- **Items:** Completed instants are rendered as range bars spanning from `requestedTime` to
  `completionTime`. Non-completed instants (requested or inflight) are rendered as point items at
  `requestedTime`.
- **Color coding:** Items are colored by state:
    - Green -> `COMPLETED`
    - Yellow -> `INFLIGHT`
    - Red -> `REQUESTED`
- **Tooltip:** On hover, shows the action type, requested time, completion time, and duration.
- **Click handler:** Clicking an instant fetches its detail via `/v2/timeline/instant/details` and
  shows the deserialized JSON in a detail panel below the timeline.

### Backend

A `hudi-timeline-service` instance already serves filesystem metadata for multiple table basePaths
since the `FileSystemView`s are cached in a map keyed by basepath.

We extend this module with `/v2/` APIs to serve the timeline metadata needed by the UI.

#### API Specification

| Method | Path                           | Parameters                                                           | Response                | Description                                                                         |
|--------|--------------------------------|----------------------------------------------------------------------|-------------------------|-------------------------------------------------------------------------------------|
| GET    | `/v2/timeline/instants`        | `basepath` (required)                                                | `List<InstantDTO>` (v2) | Returns all active instants with requested time, completion time, action, and state |
| GET    | `/v2/timeline/instant/details` | `basepath` (required), `instantTime` (required), `action` (required) | JSON string             | Returns the deserialized content of a specific instant's metadata (Avro -> JSON)    |

Static files are served from the `/ui/` path, mapped to classpath resources under
`src/main/resources/public/`.

#### DTO Design

A new v2 `InstantDTO` is introduced in a `v2` package to avoid modifying the existing `/v1/` API
contract. The existing `InstantDTO` (in `o.a.h.common.table.timeline.dto`) only exposes `action`,
`timestamp` (requested time), and `state` - it lacks `completionTime`, which is needed for
rendering range bars on the timeline.

The v2 `InstantDTO` contains:

- `requestedTime` - the instant's requested timestamp (`HoodieInstant.requestedTime()`)
- `completionTime` - the instant's completion timestamp (`HoodieInstant.getCompletionTime()`), null
  for non-completed instants
- `action` - the action type (e.g., `commit`, `deltacommit`, `compaction`)
- `state` - the instant state (`REQUESTED`, `INFLIGHT`, `COMPLETED`)

#### Handler Design

A new `UiHandler` class extends `Handler` (following the `InstantStateHandler` pattern) with two
methods:

1. `getActiveInstants(basePath)` - Retrieves the active timeline via
   `viewManager.getFileSystemView(basePath).getTimeline()`, iterates over all instants, and maps
   each `HoodieInstant` to a v2 `InstantDTO` including completion time.

2. `getInstantDetails(basePath, instantTime, action)` - Constructs a `HoodieTableMetaClient` for
   the given basepath, reads the instant's Avro content via `ActiveTimeline.getInstantDetails()`,
   and deserializes it to JSON for display in the UI.

#### Registration in RequestHandler

The `UiHandler` is registered in `RequestHandler` following the existing pattern:

- A `UiHandler` field is added alongside the existing handler fields.
- A `registerUiAPI()` method is added and called from `register()`.
- Registration is gated behind an `--enable-ui` config flag, same as `enableMarkerRequests` and
  `enableInstantStateRequests`.

#### Error Handling

- **Invalid basepath** -> HTTP 400 with a descriptive error message (e.g., "Not a valid Hudi table
  path").
- **Empty timeline** -> Returns an empty list `[]`. The frontend displays "No instants found".
- **Failed instant detail read** -> HTTP 500 with error details (e.g., Avro deserialization failure).

### Feature

The main feature for the first cut is timeline visualization for a Hudi table.

The permitted user actions are:

1. User is able to input a Hudi table basepath
2. User is able to click submit after inputting Hudi table basepath
3. The timeline of the Hudi table is rendered
4. User is able to scroll through timeline (horizontally)
5. User is able to zoom in and out of timeline
6. User is able to hover over instant for more details
7. User is able to click on a specific instant and the JSON string of the timeline details are
   rendered

Each action type occupies its own horizontal row so concurrent actions are visually separated.
Completed instants appear as horizontal bars whose width represents duration (requested ->
completed). Inflight and requested instants appear as point markers. Color indicates state: green
for completed, yellow for inflight, red for requested.

### Examples

Proof of concept (PoC) snapshots:

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
registered. When the flag is not set, the timeline server behaves exactly as it does today -
no UI-related routes are added.

## Dependency Impact

- **Zero new Java compile-time dependencies.** The frontend uses Javalin's built-in static file
  serving; no template engine is added.
- **vis-timeline JS/CSS:** ~300KB bundled as static resources under
  `src/main/resources/public/lib/vis-timeline/`.
- **No impact on Spark/Flink bundles.** `hudi-timeline-server-bundle` is a separate artifact;
  adding static resources does not affect engine-specific bundles.
- **No frontend build pipeline.** No npm, webpack, or vite. The JS/CSS files are committed directly
  and served as-is.

## Additional Quality of Life (QoL) Features to be Considered

The following client-side enhancements were implemented alongside the core features above. These are
purely frontend additions - no new APIs, Java dependencies, or architectural changes beyond what is
specified in this RFC.

- **Bootstrap 5 + renderjson styling** - Bootstrap 5 for layout and renderjson for collapsible,
  syntax-highlighted JSON in the detail panel.
- **Instant search and "Go to Now" button** - A search input that filters and focuses the timeline
  on a specific instant by requested time, plus a button to jump to the latest instant.
- **Summary statistics bar** - Counts of instants by state (completed, inflight, requested)
  computed client-side from the loaded data.
- **Filter controls (state + action)** - Dropdown filters for state and action type using
  vis-timeline's `DataView` filtering. Users can isolate specific instant types.
- **Color legend** - Maps colors to instant states for quick reference.
- **URL `?path=` persistence** - The basepath is stored in the URL query parameter so the view can
  be bookmarked and shared.
- **Keyboard navigation** - Left/right arrow keys to move between instants, Escape to close the
  detail panel.
- **Help modal** - Pressing `?` opens a modal listing all keyboard shortcuts and UI interactions.

## Future Improvements

Other features we can add later:

1. **Incremental clean range overlay** - A translucent overlay from `lastCompletedCommitTimestamp`
   to `earliestCommitToRetain` when clean metadata is available, showing which instants are
   protected from cleaning and which are eligible for removal.

2. **Partition/filegroup write heatmap** - Within one or multiple instants, render a grid colored
   by write volume from `HoodieCommitMetadata.partitionToWriteStats`. Helps identify skewed
   partitions or heavily-modified filegroups across commits.

3. **Metadata table timeline overlay** - Render the metadata table's timeline alongside the main
   table timeline. Useful for diagnosing cases where the metadata table is lagging or has stale
   entries.

4. **Archived timeline** - Load and render instants from the LSM-based archive files, extending
   the view beyond the active timeline to include historical actions.

5. **Per-state instant details** - Currently the plan for the UI is to only render the highest state
   for each instant (e.g., if requested, inflight, and completed all exist, only completed is shown).
   We should keep this as the default but also let users view the other states' metadata. 
   The simplest approach would be adding state toggle buttons after selecting an instant, so users 
   can fetch and view the JSON content for each state individually.

## Rollout/Adoption Plan

- **Backward compatibility:** Additive change only. All existing `/v1/` endpoints remain unchanged.
  UI endpoints are under `/v2/timeline/` and static files at `/ui/`. No existing behavior changes.
- **Release target:** 1.2.x
- **Documentation:** A new page on hudi.apache.org under the "Operations" section with screenshots,
  a startup guide, and a debugging walkthrough showing how to use the timeline UI to diagnose common
  issues (stuck compactions, long-running commits, etc.).
- **No migration needed:** Net-new feature behind an opt-in flag (`--enable-ui`), no impact on
  existing users or running systems.

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
