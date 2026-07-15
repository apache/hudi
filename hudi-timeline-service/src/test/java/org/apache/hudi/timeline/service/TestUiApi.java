/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.timeline.service;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.BASEPATH_PARAM;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.LAST_INSTANT_URL;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightCompaction;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedCleanFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the Timeline UI API (routes under {@code /ui} and {@code /ui/api}), gated behind
 * {@code TimelineService.Config.enableUi}. Exercises the JSON contract consumed by the browser
 * UI, the path-traversal defense on the instant-details route, and the enable-ui flag gating.
 */
class TestUiApi extends HoodieCommonTestHarness {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String UI_TIMELINE_URL = "/ui/api/timeline/instants/all";
  private static final String UI_INSTANT_URL = "/ui/api/timeline/instant";
  private static final String UI_CONFIG_URL = "/ui/api/table/config";
  private static final String UI_SCHEMA_URL = "/ui/api/table/schema/history";
  private static final String UI_PAGE_URL = "/ui";
  private static final String UI_STATIC_JS_URL = "/ui/static/js/timeline.js";

  private static final String INSTANT_PARAM = "instant";
  private static final String INSTANT_ACTION_PARAM = "instantaction";
  private static final String INSTANT_STATE_PARAM = "instantstate";
  private static final String LIMIT_PARAM = "limit";

  // A minimal but valid Avro schema, so TableSchemaResolver can parse it when computing currentSchema.
  private static final String SCHEMA_A =
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";
  private static final String SCHEMA_B =
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},"
          + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}]}";
  private static final String SCHEMA_C =
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},"
          + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"amount\",\"type\":[\"null\",\"double\"],\"default\":null}]}";

  private Configuration configuration;
  private TimelineService server;
  private int port;

  @BeforeEach
  void setUp() throws Exception {
    configuration = new Configuration();
    server = startServer(true);
    port = server.getServerPort();
    awaitUiReady(port);
  }

  // Guards against a brief startup window where the freshly bound server can 404 a route.
  private void awaitUiReady(int targetPort) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      try {
        if (httpGet(targetPort, UI_PAGE_URL, Collections.emptyMap()).code == 200) {
          return;
        }
      } catch (IOException ignored) {
        // server not yet accepting connections
      }
      Thread.sleep(50);
    }
    throw new IllegalStateException("UI server did not become ready on port " + targetPort);
  }

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.close();
    }
  }

  private TimelineService startServer(boolean enableUi) throws IOException {
    FileSystemViewStorageConfig sConf =
        FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).build();
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().build();
    HoodieLocalEngineContext ctx = new HoodieLocalEngineContext(new HadoopStorageConfiguration(configuration));
    TimelineService svc = TimelineServiceTestHarness.newBuilder().build(
        configuration,
        TimelineService.Config.builder().serverPort(0).enableUi(enableUi).build(),
        FileSystemViewManager.createViewManager(ctx, metadataConfig, sConf, commonConfig));
    svc.startService();
    return svc;
  }

  // ---------------------------------------------------------------------------
  // Test-table helpers
  // ---------------------------------------------------------------------------

  private HoodieTableMetaClient initTable(String name) throws IOException {
    return HoodieTestUtils.init(tempDir.resolve(name).toAbsolutePath().toString());
  }

  private HoodieCommitMetadata commitMetadata(HoodieTableMetaClient mc, String basePath, String ts,
                                              Map<String, String> extra) throws IOException {
    return getCommitMetadata(mc, basePath, "par", ts, 1, extra).get();
  }

  private HoodieCommitMetadata commitMetadataWithSchema(HoodieTableMetaClient mc, String basePath, String ts,
                                                        String schema) throws IOException {
    Map<String, String> extra = new HashMap<>();
    extra.put(HoodieCommitMetadata.SCHEMA_KEY, schema);
    return commitMetadata(mc, basePath, ts, extra);
  }

  // Writes an EMPTY instant file (any requested/inflight extension) straight into the timeline
  // directory for actions with no HoodieTestTable helper. The instants/all route only lists instants,
  // and the production inflight files of plan-carrying actions are empty by design.
  private void createEmptyInstantFile(HoodieTableMetaClient mc, String ts, String extension)
      throws IOException {
    Path timelineDir = Paths.get(mc.getTimelinePath().toUri().getPath());
    Files.createDirectories(timelineDir);
    Files.createFile(timelineDir.resolve(ts + extension));
  }

  // ---------------------------------------------------------------------------
  // HTTP helpers
  // ---------------------------------------------------------------------------

  private static final class Http {
    final int code;
    final String body;
    final String contentType;

    Http(int code, String body, String contentType) {
      this.code = code;
      this.body = body;
      this.contentType = contentType;
    }
  }

  private Http httpGet(int targetPort, String path, Map<String, String> params) throws IOException {
    StringBuilder url = new StringBuilder("http://localhost:").append(targetPort).append(path);
    boolean first = true;
    for (Map.Entry<String, String> e : params.entrySet()) {
      url.append(first ? '?' : '&');
      url.append(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8))
          .append('=')
          .append(URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8));
      first = false;
    }
    HttpURLConnection conn = (HttpURLConnection) new URL(url.toString()).openConnection();
    conn.setRequestMethod("GET");
    // Avoid reusing a keep-alive connection to a previously closed server on a recycled port.
    conn.setRequestProperty("Connection", "close");
    int code = conn.getResponseCode();
    String contentType = conn.getContentType();
    InputStream is = code >= 400 ? conn.getErrorStream() : conn.getInputStream();
    String body = is == null ? "" : new String(is.readAllBytes(), StandardCharsets.UTF_8);
    conn.disconnect();
    return new Http(code, body, contentType);
  }

  private Map<String, String> params(String... kv) {
    Map<String, String> m = new LinkedHashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }

  private JsonNode getJsonOk(String path, Map<String, String> params) throws IOException {
    Http r = httpGet(port, path, params);
    assertEquals(200, r.code, r.body);
    return MAPPER.readTree(r.body);
  }

  private JsonNode findInstant(JsonNode root, String requestTs) {
    for (JsonNode n : root.get("instants")) {
      if (requestTs.equals(n.get("requestTs").asText())) {
        return n;
      }
    }
    return null;
  }

  private static boolean isJsonNull(JsonNode node) {
    return node == null || node.isNull();
  }

  // ---------------------------------------------------------------------------
  // 1. getUiTimeline mapping and ordering
  // ---------------------------------------------------------------------------

  @Test
  void testUiTimelineMappingAndOrder() throws Exception {
    HoodieTableMetaClient mc = initTable("mapping");
    String base = mc.getBasePath().toString();
    String commitTs = "20240101000001";
    String compactionTs = "20240101000002";
    String cleanTs = "20240101000003";
    String logCompactionTs = "20240101000004";
    String clusteringTs = "20240101000005";

    HoodieTestTable table = HoodieTestTable.of(mc);
    table.addCommit(commitTs, Option.of(commitMetadata(mc, base, commitTs, Collections.emptyMap())));
    // A pending (requested) compaction: not yet completed.
    table.addRequestedCompaction(compactionTs);
    // A clean: a non-foldable action whose comparableAction equals its action.
    table.addClean(cleanTs);
    // Two more pending instants that pin the remaining comparable-action folds
    // (logcompaction -> deltacommit, clustering -> replacecommit). No HoodieTestTable helper writes a
    // requested logcompaction and the clustering helper needs Avro metadata, so write empty requested
    // instant files directly; the instants/all route never reads their content.
    createEmptyInstantFile(mc, logCompactionTs, HoodieTimeline.REQUESTED_LOG_COMPACTION_EXTENSION);
    createEmptyInstantFile(mc, clusteringTs, HoodieTimeline.REQUESTED_CLUSTERING_COMMIT_EXTENSION);

    JsonNode root = getJsonOk(UI_TIMELINE_URL, params(BASEPATH_PARAM, base));
    JsonNode instants = root.get("instants");
    assertNotNull(instants, root.toString());

    // Completed plain commit: comparableAction == action == commit, completionTs populated.
    JsonNode commit = findInstant(root, commitTs);
    assertNotNull(commit, root.toString());
    assertEquals("commit", commit.get("action").asText());
    assertEquals("commit", commit.get("comparableAction").asText());
    assertEquals("COMPLETED", commit.get("state").asText());
    assertFalse(isJsonNull(commit.get("completionTs")), "completed commit must carry a completionTs");

    // Pending compaction: action=compaction, comparableAction folds to commit, completionTs null.
    JsonNode compaction = findInstant(root, compactionTs);
    assertNotNull(compaction, root.toString());
    assertEquals("compaction", compaction.get("action").asText());
    assertEquals("commit", compaction.get("comparableAction").asText());
    assertEquals("REQUESTED", compaction.get("state").asText());
    assertTrue(isJsonNull(compaction.get("completionTs")), "pending compaction must have null completionTs");

    // Clean: non-foldable, comparableAction == action.
    JsonNode clean = findInstant(root, cleanTs);
    assertNotNull(clean, root.toString());
    assertEquals("clean", clean.get("action").asText());
    assertEquals("clean", clean.get("comparableAction").asText());

    // Pending logcompaction: action=logcompaction, comparableAction folds to deltacommit, completionTs null.
    JsonNode logCompaction = findInstant(root, logCompactionTs);
    assertNotNull(logCompaction, root.toString());
    assertEquals("logcompaction", logCompaction.get("action").asText());
    assertEquals("deltacommit", logCompaction.get("comparableAction").asText());
    assertEquals("REQUESTED", logCompaction.get("state").asText());
    assertTrue(isJsonNull(logCompaction.get("completionTs")), "pending logcompaction must have null completionTs");

    // Pending clustering: action=clustering, comparableAction folds to replacecommit, completionTs null.
    JsonNode clustering = findInstant(root, clusteringTs);
    assertNotNull(clustering, root.toString());
    assertEquals("clustering", clustering.get("action").asText());
    assertEquals("replacecommit", clustering.get("comparableAction").asText());
    assertEquals("REQUESTED", clustering.get("state").asText());
    assertTrue(isJsonNull(clustering.get("completionTs")), "pending clustering must have null completionTs");

    // Instants returned in timeline (request-time ascending) order.
    String previous = "";
    for (JsonNode n : instants) {
      String ts = n.get("requestTs").asText();
      assertTrue(ts.compareTo(previous) >= 0, "instants not in ascending request-time order: " + root);
      previous = ts;
    }
  }

  @Test
  void testUiTimelineCompletedCompactionFoldsToCommit() throws Exception {
    HoodieTableMetaClient mc = initTable("completed-compaction");
    String base = mc.getBasePath().toString();
    String ts = "20240101000010";
    HoodieCommitMetadata meta = commitMetadata(mc, base, ts, Collections.emptyMap());
    // Writes .compaction.requested, .compaction.inflight and a completed .commit for the same instant.
    HoodieTestTable.of(mc).addCompaction(ts, meta);

    JsonNode root = getJsonOk(UI_TIMELINE_URL, params(BASEPATH_PARAM, base));

    int matches = 0;
    for (JsonNode n : root.get("instants")) {
      if (ts.equals(n.get("requestTs").asText())) {
        matches++;
        // The active-timeline layout filter folds the (requested, inflight, completed) triple; the
        // completed file is a .commit, so a completed compaction surfaces as action=commit.
        assertEquals("commit", n.get("action").asText(), "completed compaction must surface as commit");
        assertEquals("COMPLETED", n.get("state").asText());
        assertFalse(isJsonNull(n.get("completionTs")));
      }
      assertFalse("compaction".equals(n.get("action").asText()) && ts.equals(n.get("requestTs").asText()),
          "completed compaction must never surface as action=compaction");
    }
    assertEquals(1, matches, "completed compaction must surface exactly once: " + root);
  }

  @Test
  void testUiTimelineEmptyTableReturnsEmptyList() throws Exception {
    HoodieTableMetaClient mc = initTable("empty-timeline");
    String base = mc.getBasePath().toString();

    JsonNode root = getJsonOk(UI_TIMELINE_URL, params(BASEPATH_PARAM, base));
    JsonNode instants = root.get("instants");
    assertNotNull(instants, root.toString());
    assertTrue(instants.isArray(), root.toString());
    assertEquals(0, instants.size(), root.toString());
  }

  // ---------------------------------------------------------------------------
  // 2. getInstantDetails
  // ---------------------------------------------------------------------------

  @Test
  void testGetInstantDetailsCommitRoundTrip() throws Exception {
    HoodieTableMetaClient mc = initTable("instant-commit");
    String base = mc.getBasePath().toString();
    String ts = "20240101000021";
    Map<String, String> extra = new HashMap<>();
    extra.put("myUiTestKey", "myUiTestValue");
    HoodieTestTable.of(mc).addCommit(ts, Option.of(commitMetadata(mc, base, ts, extra)));

    JsonNode root = getJsonOk(UI_INSTANT_URL,
        params(BASEPATH_PARAM, base, INSTANT_PARAM, ts, INSTANT_ACTION_PARAM, "commit", INSTANT_STATE_PARAM, "COMPLETED"));
    assertEquals("myUiTestValue", root.get("extraMetadata").get("myUiTestKey").asText(), root.toString());
  }

  @Test
  void testGetInstantDetailsCompactionPlan() throws Exception {
    HoodieTableMetaClient mc = initTable("instant-compaction-plan");
    String base = mc.getBasePath().toString();
    String ts = "20240101000022";
    HoodieTestTable.of(mc).addRequestedCompaction(ts);

    JsonNode root = getJsonOk(UI_INSTANT_URL,
        params(BASEPATH_PARAM, base, INSTANT_PARAM, ts, INSTANT_ACTION_PARAM, "compaction", INSTANT_STATE_PARAM, "REQUESTED"));
    // HoodieCompactionPlan is converted to a Map; the requested plan carries operations for its file slices.
    assertTrue(root.has("operations"), "compaction plan must expose operations: " + root);
    assertTrue(root.get("operations").isArray());
  }

  @Test
  void testGetInstantDetailsCleanPlan() throws Exception {
    HoodieTableMetaClient mc = initTable("instant-clean-plan");
    String base = mc.getBasePath().toString();
    String ts = "20240101000024";
    // A REQUESTED-only clean carrying a HoodieCleanerPlan built the way HoodieTestTable.addClean does,
    // but with a recognizable policy string. Requested-only is deliberate: the active timeline keeps only
    // the highest state per instant, so writing inflight/completed files too would surface the completed
    // HoodieCleanMetadata instead of the plan.
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant("", "", ""), "",
        "KEEP_LATEST_COMMITS", new HashMap<>(), CleanPlanV2MigrationHandler.VERSION, new HashMap<>(),
        new ArrayList<>(), Collections.emptyMap());
    createRequestedCleanFile(mc, ts, cleanerPlan);

    JsonNode root = getJsonOk(UI_INSTANT_URL,
        params(BASEPATH_PARAM, base, INSTANT_PARAM, ts, INSTANT_ACTION_PARAM, "clean", INSTANT_STATE_PARAM, "REQUESTED"));
    // The server converts the Avro HoodieCleanerPlan to a Map, so field names are the Avro field names.
    assertEquals("KEEP_LATEST_COMMITS", root.get("policy").asText(), root.toString());
    assertTrue(root.has("version"), "clean plan must expose version: " + root);
  }

  @Test
  void testGetInstantDetailsCompletedReplaceCommit() throws Exception {
    HoodieTableMetaClient mc = initTable("instant-replacecommit");
    String base = mc.getBasePath().toString();
    String ts = "20240101000025";
    // A completed replacecommit is avro HoodieReplaceCommitMetadata on disk; reading it as plain
    // HoodieCommitMetadata previously 500ed on the avro record-name mismatch. The POJO carries
    // partitionToReplaceFileIds, surfaced directly (POJO, so no avro->Map conversion).
    HoodieReplaceCommitMetadata completeReplaceMetadata = new HoodieReplaceCommitMetadata();
    completeReplaceMetadata.setOperationType(WriteOperationType.INSERT_OVERWRITE);
    completeReplaceMetadata.addReplaceFileId("par", "file-1");
    HoodieTestTable.of(mc).addReplaceCommit(ts, Option.empty(), Option.empty(), completeReplaceMetadata);

    // A completed replacecommit surfaces in the folded active timeline as action=replacecommit
    // (unlike compaction, which completes as commit).
    JsonNode root = getJsonOk(UI_INSTANT_URL, params(BASEPATH_PARAM, base, INSTANT_PARAM, ts,
        INSTANT_ACTION_PARAM, "replacecommit", INSTANT_STATE_PARAM, "COMPLETED"));
    JsonNode replaced = root.get("partitionToReplaceFileIds");
    assertNotNull(replaced, root.toString());
    assertEquals("file-1", replaced.get("par").get(0).asText(), root.toString());
  }

  @Test
  void testGetInstantDetailsCompletedIndexing() throws Exception {
    HoodieTableMetaClient mc = initTable("instant-indexing");
    String base = mc.getBasePath().toString();
    String ts = "20240101000026";
    // No HoodieTestTable helper writes an indexing instant. Lay down the empty requested+inflight
    // pending files, then complete it exactly as RunIndexActionExecutor does. A completed indexing
    // instant stores avro HoodieIndexCommitMetadata, not HoodieCommitMetadata; reading it as the
    // latter previously 500ed.
    createEmptyInstantFile(mc, ts, HoodieTimeline.REQUESTED_INDEX_COMMIT_EXTENSION);
    createEmptyInstantFile(mc, ts, HoodieTimeline.INFLIGHT_INDEX_COMMIT_EXTENSION);
    HoodieIndexPartitionInfo partitionInfo =
        new HoodieIndexPartitionInfo(1, "column_stats", ts, Collections.emptyMap());
    HoodieIndexCommitMetadata indexCommitMetadata = HoodieIndexCommitMetadata.newBuilder()
        .setVersion(1).setIndexPartitionInfos(Collections.singletonList(partitionInfo)).build();
    // saveAsComplete checks the inflight file straight against storage; reloading the active timeline
    // makes it observe the just-written pending files.
    mc.reloadActiveTimeline().saveAsComplete(false,
        mc.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.INDEXING_ACTION, ts),
        Option.of(indexCommitMetadata));

    JsonNode root = getJsonOk(UI_INSTANT_URL, params(BASEPATH_PARAM, base, INSTANT_PARAM, ts,
        INSTANT_ACTION_PARAM, "indexing", INSTANT_STATE_PARAM, "COMPLETED"));
    // The avro metadata is converted to a Map with avro field names.
    JsonNode infos = root.get("indexPartitionInfos");
    assertNotNull(infos, root.toString());
    assertTrue(infos.isArray() && infos.size() >= 1, root.toString());
    assertEquals("column_stats", infos.get(0).get("metadataPartitionPath").asText(), root.toString());
  }

  @Test
  void testGetInstantDetailsInflightCompactionReturnsPlan() throws Exception {
    HoodieTableMetaClient mc = initTable("instant-inflight-compaction");
    String base = mc.getBasePath().toString();
    String ts = "20240101000027";
    // addRequestedCompaction writes a real HoodieCompactionPlan into the requested file;
    // createInflightCompaction writes the production-like EMPTY inflight file. This pins the
    // requested-twin read: the inflight compaction file is empty by design, so reading the plan from
    // the inflight instant previously 500ed - it must be read from the requested twin.
    HoodieTestTable.of(mc).addRequestedCompaction(ts);
    createInflightCompaction(mc, ts);

    JsonNode root = getJsonOk(UI_INSTANT_URL, params(BASEPATH_PARAM, base, INSTANT_PARAM, ts,
        INSTANT_ACTION_PARAM, "compaction", INSTANT_STATE_PARAM, "INFLIGHT"));
    assertTrue(root.has("operations"), "inflight compaction must expose its plan operations: " + root);
    assertTrue(root.get("operations").isArray());
  }

  @Test
  void testGetInstantDetailsMalformedStateOrActionReturns400() throws Exception {
    HoodieTableMetaClient mc = initTable("instant-bad-state");
    String base = mc.getBasePath().toString();
    String ts = "20240101000023";
    HoodieTestTable.of(mc).addCommit(ts, Option.of(commitMetadata(mc, base, ts, Collections.emptyMap())));

    // A malformed state is rejected before the timeline lookup.
    Http badState = httpGet(port, UI_INSTANT_URL,
        params(BASEPATH_PARAM, base, INSTANT_PARAM, ts, INSTANT_ACTION_PARAM, "commit", INSTANT_STATE_PARAM, "NOT_A_STATE"));
    assertEquals(400, badState.code, badState.body);

    // A valid state but an action outside VALID_ACTIONS_IN_TIMELINE is likewise a 400, not a 404.
    Http badAction = httpGet(port, UI_INSTANT_URL,
        params(BASEPATH_PARAM, base, INSTANT_PARAM, ts, INSTANT_ACTION_PARAM, "NOT_AN_ACTION", INSTANT_STATE_PARAM, "COMPLETED"));
    assertEquals(400, badAction.code, badAction.body);
  }

  @Test
  void testGetInstantDetailsUnknownTimestampReturns404() throws Exception {
    HoodieTableMetaClient mc = initTable("instant-unknown");
    String base = mc.getBasePath().toString();
    HoodieTestTable.of(mc).addCommit("20240101000030",
        Option.of(commitMetadata(mc, base, "20240101000030", Collections.emptyMap())));

    // A well-formed timestamp that is not present in the timeline.
    Http r = httpGet(port, UI_INSTANT_URL,
        params(BASEPATH_PARAM, base, INSTANT_PARAM, "20991231235959999", INSTANT_ACTION_PARAM, "commit",
            INSTANT_STATE_PARAM, "COMPLETED"));
    assertEquals(404, r.code, r.body);
  }

  /**
   * Load-bearing: pins the path-traversal defense. The probe is pinned to REQUESTED state (a
   * COMPLETED probe would resolve to a real instant even against a vulnerable handler). A naive
   * handler that built {@code <timelinePath>/<instant>.<action>.<state>} and opened it would leak
   * the planted marker file located outside the table. The current handler resolves the (instant,
   * action, state) triple against the active timeline instead, so the traversal is not found (404)
   * and the marker never appears in the response.
   */
  @Test
  void testGetInstantDetailsPathTraversalReturns404() throws Exception {
    HoodieTableMetaClient mc = initTable("instant-traversal");
    String base = mc.getBasePath().toString();
    HoodieTestTable.of(mc).addCommit("20240101000040",
        Option.of(commitMetadata(mc, base, "20240101000040", Collections.emptyMap())));

    Path timelineDir = Paths.get(mc.getTimelinePath().toUri().getPath());
    Path outsideDir = Files.createTempDirectory("hudi-ui-traversal");
    String marker = "HUDI_UI_TRAVERSAL_SECRET_" + UUID.randomUUID();
    // Plant the marker under several plausible names a vulnerable handler might have built.
    Files.write(outsideDir.resolve("leak.commit.requested"), marker.getBytes(StandardCharsets.UTF_8));
    Files.write(outsideDir.resolve("leak.commit.REQUESTED"), marker.getBytes(StandardCharsets.UTF_8));
    Files.write(outsideDir.resolve("leak"), marker.getBytes(StandardCharsets.UTF_8));

    // Traversal instant that, once ".commit.requested" is appended and normalized, points at the marker.
    String traversalInstant = timelineDir.relativize(outsideDir.resolve("leak")).toString();

    Http r = httpGet(port, UI_INSTANT_URL,
        params(BASEPATH_PARAM, base, INSTANT_PARAM, traversalInstant, INSTANT_ACTION_PARAM, "commit",
            INSTANT_STATE_PARAM, "REQUESTED"));
    assertEquals(404, r.code, r.body);
    assertFalse(r.body.contains(marker), "path traversal leaked planted file content: " + r.body);
  }

  // ---------------------------------------------------------------------------
  // 3. Schema history
  // ---------------------------------------------------------------------------

  @Test
  void testSchemaHistoryEmptyTable() throws Exception {
    HoodieTableMetaClient mc = initTable("schema-empty");
    String base = mc.getBasePath().toString();

    JsonNode root = getJsonOk(UI_SCHEMA_URL, params(BASEPATH_PARAM, base));
    assertTrue(isJsonNull(root.get("currentSchema")), root.toString());
    assertEquals(0, root.get("history").size(), root.toString());
    assertTrue(isJsonNull(root.get("window").get("oldestInstantScanned")), root.toString());
    assertFalse(root.get("window").get("truncated").asBoolean(), root.toString());
  }

  @Test
  void testSchemaHistoryBaselineAndChange() throws Exception {
    HoodieTableMetaClient mc = initTable("schema-history");
    String base = mc.getBasePath().toString();
    HoodieTestTable table = HoodieTestTable.of(mc);
    // schema A, A, B, then C via a replacecommit: baseline, change, change (the A,A repeat is folded).
    table.addCommit("20240101000101", Option.of(commitMetadataWithSchema(mc, base, "20240101000101", SCHEMA_A)));
    table.addCommit("20240101000102", Option.of(commitMetadataWithSchema(mc, base, "20240101000102", SCHEMA_A)));
    table.addCommit("20240101000103", Option.of(commitMetadataWithSchema(mc, base, "20240101000103", SCHEMA_B)));
    // A schema change delivered via an insert_overwrite replacecommit: its completed file is avro
    // HoodieReplaceCommitMetadata, which getSchemaHistory must read as the POJO or the SCHEMA_C change
    // is silently skipped.
    HoodieReplaceCommitMetadata replaceMetadata = new HoodieReplaceCommitMetadata();
    replaceMetadata.setOperationType(WriteOperationType.INSERT_OVERWRITE);
    replaceMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, SCHEMA_C);
    table.addReplaceCommit("20240101000104", Option.empty(), Option.empty(), replaceMetadata);

    JsonNode root = getJsonOk(UI_SCHEMA_URL, params(BASEPATH_PARAM, base));
    JsonNode history = root.get("history");
    assertEquals(3, history.size(), root.toString());

    JsonNode baseline = history.get(0);
    assertEquals("baseline", baseline.get("type").asText());
    assertEquals(SCHEMA_A, baseline.get("schema").asText());
    assertEquals("20240101000101", baseline.get("instant").asText());
    assertFalse(isJsonNull(baseline.get("completionTime")), "history entry must carry a completionTime");

    JsonNode change = history.get(1);
    assertEquals("change", change.get("type").asText());
    assertEquals(SCHEMA_B, change.get("schema").asText());
    assertEquals("20240101000103", change.get("instant").asText());
    assertFalse(isJsonNull(change.get("completionTime")));

    // The insert_overwrite schema change is not skipped: it lands as a third "change" entry.
    JsonNode overwrite = history.get(2);
    assertEquals("change", overwrite.get("type").asText());
    assertEquals(SCHEMA_C, overwrite.get("schema").asText());
    assertEquals("20240101000104", overwrite.get("instant").asText());
    assertFalse(isJsonNull(overwrite.get("completionTime")));
  }

  @Test
  void testSchemaHistoryWindowingAndLimitBounds() throws Exception {
    HoodieTableMetaClient mc = initTable("schema-window");
    String base = mc.getBasePath().toString();
    HoodieTestTable table = HoodieTestTable.of(mc);
    table.addCommit("20240101000201", Option.of(commitMetadataWithSchema(mc, base, "20240101000201", SCHEMA_A)));
    table.addCommit("20240101000202", Option.of(commitMetadataWithSchema(mc, base, "20240101000202", SCHEMA_A)));
    table.addCommit("20240101000203", Option.of(commitMetadataWithSchema(mc, base, "20240101000203", SCHEMA_B)));

    // limit smaller than the number of commits -> window is truncated to the most recent `limit` instants.
    JsonNode windowed = getJsonOk(UI_SCHEMA_URL, params(BASEPATH_PARAM, base, LIMIT_PARAM, "2"));
    assertTrue(windowed.get("window").get("truncated").asBoolean(), windowed.toString());
    assertEquals("20240101000202", windowed.get("window").get("oldestInstantScanned").asText(), windowed.toString());

    // limit=0 is rejected.
    Http zero = httpGet(port, UI_SCHEMA_URL, params(BASEPATH_PARAM, base, LIMIT_PARAM, "0"));
    assertEquals(400, zero.code, zero.body);

    // limit above the cap is clamped (not an error).
    Http large = httpGet(port, UI_SCHEMA_URL, params(BASEPATH_PARAM, base, LIMIT_PARAM, "5000"));
    assertEquals(200, large.code, large.body);
    assertFalse(MAPPER.readTree(large.body).get("window").get("truncated").asBoolean(), large.body);
  }

  // ---------------------------------------------------------------------------
  // 4. Table config and invalid base path
  // ---------------------------------------------------------------------------

  @Test
  void testTableConfigContainsTableName() throws Exception {
    HoodieTableMetaClient mc = initTable("table-config");
    String base = mc.getBasePath().toString();

    JsonNode root = getJsonOk(UI_CONFIG_URL, params(BASEPATH_PARAM, base));
    JsonNode props = root.get("properties");
    assertNotNull(props, root.toString());
    assertTrue(props.has("hoodie.table.name"), root.toString());
  }

  @Test
  void testInvalidBasePathReturns400() throws Exception {
    // A directory that exists but is not a Hudi table.
    Path notATable = Files.createDirectories(tempDir.resolve("not-a-table"));
    String base = notATable.toAbsolutePath().toString();

    assertEquals(400, httpGet(port, UI_TIMELINE_URL, params(BASEPATH_PARAM, base)).code);
    assertEquals(400, httpGet(port, UI_CONFIG_URL, params(BASEPATH_PARAM, base)).code);
  }

  // ---------------------------------------------------------------------------
  // 5. Static UI page and assets
  // ---------------------------------------------------------------------------

  @Test
  void testUiPageAndStaticAssetServed() throws Exception {
    Http page = httpGet(port, UI_PAGE_URL, Collections.emptyMap());
    assertEquals(200, page.code, page.body);
    assertNotNull(page.contentType);
    assertTrue(page.contentType.toLowerCase().contains("html"), "unexpected content-type: " + page.contentType);
    assertTrue(page.body.contains("Hudi Timeline"), "UI page missing expected title text");

    Http js = httpGet(port, UI_STATIC_JS_URL, Collections.emptyMap());
    assertEquals(200, js.code, js.body);
  }

  // ---------------------------------------------------------------------------
  // 6. Flag gating (load-bearing): every /ui route is absent without --enable-ui.
  // ---------------------------------------------------------------------------

  @Test
  void testUiDisabledReturns404ForAllUiRoutes() throws Exception {
    HoodieTableMetaClient mc = initTable("gating");
    String base = mc.getBasePath().toString();
    HoodieTestTable.of(mc).addCommit("20240101000301",
        Option.of(commitMetadata(mc, base, "20240101000301", Collections.emptyMap())));

    TimelineService noUi = startServer(false);
    try {
      int noUiPort = noUi.getServerPort();

      // The UI page and static assets are not registered.
      assertEquals(404, httpGet(noUiPort, UI_PAGE_URL, Collections.emptyMap()).code);
      assertEquals(404, httpGet(noUiPort, UI_STATIC_JS_URL, Collections.emptyMap()).code);

      // None of the /ui/api routes are registered.
      assertEquals(404, httpGet(noUiPort, UI_TIMELINE_URL, params(BASEPATH_PARAM, base)).code);
      assertEquals(404, httpGet(noUiPort, UI_CONFIG_URL, params(BASEPATH_PARAM, base)).code);
      assertEquals(404, httpGet(noUiPort, UI_SCHEMA_URL, params(BASEPATH_PARAM, base)).code);
      assertEquals(404, httpGet(noUiPort, UI_INSTANT_URL,
          params(BASEPATH_PARAM, base, INSTANT_PARAM, "20240101000301", INSTANT_ACTION_PARAM, "commit",
              INSTANT_STATE_PARAM, "COMPLETED")).code);

      // A regular /v1 route still responds while the UI is disabled.
      Http v1 = httpGet(noUiPort, LAST_INSTANT_URL, params(BASEPATH_PARAM, base));
      assertEquals(200, v1.code, v1.body);
    } finally {
      noUi.close();
    }
  }
}
