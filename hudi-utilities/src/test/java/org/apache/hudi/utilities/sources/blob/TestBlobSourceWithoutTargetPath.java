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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.config.SqlTransformerConfig;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.transform.InstantAwareTransformer;
import org.apache.hudi.utilities.transform.SqlQueryBasedTransformer;
import org.apache.hudi.utilities.unstructured.storage.ManagedBlobPaths;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The blob source starts without the streamer target path, a new table commits, and the instant a transformer receives is
 * the one the streamer commits.
 */
@Tag("functional")
class TestBlobSourceWithoutTargetPath extends UtilitiesTestBase {

  private static String receivedInstant;
  private static List<HoodieInstant> pendingAtApply;
  private static final long THRESHOLD = 4096;

  @TempDir
  Path tempDir;

  @BeforeAll
  static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  static void cleanupClass() {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @Test
  void newTableCommits() throws Exception {
    BlobJobHarness harness = BlobJobHarness.under(tempDir.resolve("new")).install(jsc.hadoopConfiguration());
    String tableBasePath = tempDir.resolve("new-table").toUri().toString();
    Map<String, Integer> files = new TreeMap<>();
    files.put("docs/big.pdf", 64 * 1024);
    files.put("docs/small.pdf", 100);
    harness.putAndAnnounce(sparkSession, files);

    runStreamer(harness.jobProperties(THRESHOLD), tableBasePath);
    assertEquals(1, completedCommits(tableBasePath).size());
    List<String> sourcePaths = sparkSession.read().format("hudi").load(tableBasePath).collectAsList().stream()
        .map(r -> r.<String>getAs(BlobTableSchema.SOURCE_FILE_PATH)).sorted().collect(Collectors.toList());
    assertEquals(java.util.Arrays.asList(BlobJobHarness.sourcePath("docs/big.pdf"), BlobJobHarness.sourcePath("docs/small.pdf")),
        sourcePaths);
    assertEquals(0, outOfLineFiles(tableBasePath).size(), "the large object is referenced where it is");
  }

  /** The blob source starts when the streamer target path is not set. */
  @Test
  void sourceStartsWithNoTargetBasePath() throws Exception {
    BlobJobHarness harness = BlobJobHarness.under(tempDir.resolve("start")).install(jsc.hadoopConfiguration());
    TypedProperties props = harness.jobProperties(THRESHOLD);
    new UnstructuredDataSource(props, jsc, sparkSession, null, new org.apache.hudi.utilities.streamer.DefaultStreamContext(null, Option.empty()));
    assertFalse(props.containsKey("hoodie.streamer.ingestion.targetBasePath"), props.toString());
  }

  /**
   * A job that sets a {@code hoodie.blob.table.*} key the blob source does not have runs as if it were unset: the same rows (inline
   * bytes compared by digest) and nothing under .hoodie/.blob, as the run without it, over the same announced objects.
   */
  @Test
  void unknownBlobTableKeyIsIgnored() throws Exception {
    BlobJobHarness harness = BlobJobHarness.under(tempDir.resolve("unknown-key")).install(jsc.hadoopConfiguration());
    Map<String, Integer> files = new TreeMap<>();
    files.put("docs/big.pdf", 64 * 1024);
    files.put("docs/small.pdf", 100);
    harness.putAndAnnounce(sparkSession, files);
    String plainTable = tempDir.resolve("plain-table").toUri().toString();
    String unknownKeyTable = tempDir.resolve("unknown-key-table").toUri().toString();

    runStreamer(harness.jobProperties(THRESHOLD), plainTable);
    TypedProperties withUnknownKey = harness.jobProperties(THRESHOLD);
    withUnknownKey.setProperty(BlobTableConfig.PREFIX + "never.defined.option", "1");
    runStreamer(withUnknownKey, unknownKeyTable);

    assertEquals(1, completedCommits(unknownKeyTable).size());
    assertEquals(blobRows(plainTable), blobRows(unknownKeyTable));
    assertEquals(2, blobRows(unknownKeyTable).size(), blobRows(unknownKeyTable).toString());
    assertEquals(outOfLineFiles(plainTable).size(), outOfLineFiles(unknownKeyTable).size());
    assertEquals(0, outOfLineFiles(unknownKeyTable).size(), "the large object is referenced where it is");
  }

  /** Every blob row's columns that do not depend on the table path or the commit (inline bytes as their SHA-256), sorted. */
  private static List<String> blobRows(String basePath) {
    return sparkSession.read().format("hudi").load(basePath)
        .selectExpr(BlobTableSchema.SOURCE_FILE_PATH, BlobTableSchema.SOURCE_PATH_SHA, BlobTableSchema.SOURCE_MODIFICATION_TIME,
            BlobTableSchema.SOURCE_FILE_NAME, BlobTableSchema.SOURCE_FILE_EXTENSION, BlobTableSchema.SOURCE_FILE_SIZE, BlobTableSchema.CONTENT_TYPE,
            "blob.type", "sha2(blob.data, 256)")
        .collectAsList().stream().map(Row::toString).sorted().collect(Collectors.toList());
  }

  /** The single-table streamer does not put its target path into the properties. */
  @Test
  void streamerDoesNotSetTheTargetBasePathProperty() {
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetBasePath = "file:///data/blob_table";
    cfg.tableType = "COPY_ON_WRITE";
    TypedProperties combined = HoodieStreamer.combineProperties(cfg, Option.empty(), new Configuration());
    assertFalse(combined.containsKey("hoodie.streamer.ingestion.targetBasePath"), combined.toString());
  }

  @ParameterizedTest
  @ValueSource(strings = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void inflightInstantReachesTheTransformerAndIsCommitted(String tableType) throws Exception {
    BlobJobHarness harness = BlobJobHarness.under(tempDir.resolve("landing")).install(jsc.hadoopConfiguration());
    String table = tempDir.resolve("table").toUri().toString();
    HoodieStreamer.Config cfg = BlobJobHarness.streamerConfig(table);
    cfg.tableType = tableType;
    cfg.transformerClassNames = Arrays.asList(BlobQuarantineTransformer.class.getName(), RecordingInstantTransformer.class.getName(),
        SqlQueryBasedTransformer.class.getName());
    TypedProperties props = harness.jobProperties(4096);
    props.setProperty(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(), "LAZY");
    props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key(), "SELECT *, 'applied' AS transformer_marker FROM <SRC>");

    List<String> received = new ArrayList<>();
    for (String key : Arrays.asList("docs/first.pdf", "docs/second.pdf")) {
      harness.putAndAnnounce(sparkSession, Collections.singletonMap(key, 100));
      receivedInstant = null;
      pendingAtApply = Collections.emptyList();
      HoodieStreamer streamer = new HoodieStreamer(cfg, jsc, Option.of(props));
      try {
        streamer.sync();
      } finally {
        streamer.shutdownGracefully();
      }
      assertNotNull(receivedInstant);
      assertEquals(1, pendingAtApply.size(), pendingAtApply.toString());
      assertEquals(receivedInstant, pendingAtApply.get(0).requestedTime());
      assertEquals(HoodieInstant.State.INFLIGHT, pendingAtApply.get(0).getState(), "the instant is INFLIGHT at apply");
      received.add(receivedInstant);
    }

    List<String> completed = metaClient(jsc, table).getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
        .getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
    assertEquals(received, completed, "each round commits exactly the instant its transformer received");
    List<Row> rows = sparkSession.read().format("hudi").load(table).select(BlobTableSchema.SOURCE_FILE_NAME, "transformer_marker")
        .orderBy(BlobTableSchema.SOURCE_FILE_NAME).collectAsList();
    assertEquals(Arrays.asList(RowFactory.create("first.pdf", "applied"), RowFactory.create("second.pdf", "applied")), rows);
  }

  private static HoodieTableMetaClient metaClient(JavaSparkContext context, String table) {
    return HoodieTableMetaClient.builder().setBasePath(table)
        .setConf(HadoopFSUtils.getStorageConf(context.hadoopConfiguration())).build();
  }

  /** Records the timeline state without writing any side files. */
  public static class RecordingInstantTransformer implements InstantAwareTransformer {

    @Override
    public Dataset<Row> apply(JavaSparkContext context, SparkSession session, Dataset<Row> rows, TypedProperties props) {
      receivedInstant = null;
      return rows;
    }

    @Override
    public Dataset<Row> apply(JavaSparkContext context, SparkSession session, Dataset<Row> rows, TypedProperties props, String instantTime) {
      receivedInstant = instantTime;
      pendingAtApply = metaClient(context, props.getString(HoodieWriteConfig.BASE_PATH.key()))
          .getActiveTimeline().filterInflightsAndRequested().getInstants();
      return rows;
    }
  }

  private static void runStreamer(TypedProperties props, String tableBasePath) throws Exception {
    HoodieStreamer.Config cfg = BlobJobHarness.streamerConfig(tableBasePath);
    cfg.sourceLimit = Long.MAX_VALUE;
    cfg.forceDisableCompaction = true;
    HoodieStreamer streamer = new HoodieStreamer(cfg, jsc, Option.of(props));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
    }
  }

  private static HoodieTableMetaClient metaClient(String basePath) {
    return HoodieTableMetaClient.builder().setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).setBasePath(basePath).build();
  }

  private static List<HoodieInstant> completedCommits(String basePath) {
    return metaClient(basePath).getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants();
  }

  private static List<String> outOfLineFiles(String basePath) throws Exception {
    Path base = Paths.get(java.net.URI.create(basePath));
    Path blobRoot = base.resolve(ManagedBlobPaths.FOLDER);
    if (!Files.exists(blobRoot)) {
      return Collections.emptyList();
    }
    try (Stream<Path> walk = Files.walk(blobRoot)) {
      return walk.filter(Files::isRegularFile).filter(p -> !p.getFileName().toString().startsWith("."))
          .map(p -> base.relativize(p).toString()).sorted().collect(Collectors.toList());
    }
  }
}
