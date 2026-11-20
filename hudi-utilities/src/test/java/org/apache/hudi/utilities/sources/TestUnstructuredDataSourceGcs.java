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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.sources.blob.GcsBucketFolderFileSystem;
import org.apache.hudi.utilities.sources.blob.GcsEventsTableFixture;
import org.apache.hudi.utilities.sources.blob.GcsEventsTableFixture.Notification;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredCloud;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceConfig;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * {@link UnstructuredDataSource} on {@code cloud=gcs}, best effort: a GCS events table on the local disk, in the row shape the
 * stock GCS events source writes, whose {@code gs://bucket/name} objects are local files ({@link GcsBucketFolderFileSystem}).
 * One round of each job, the stock selection's duplicate handling and the caught-up checkpoint.
 */
@Tag("functional")
class TestUnstructuredDataSourceGcs extends UtilitiesTestBase {

  private static final String BUCKET = "landing";
  private static final long INLINE_MAX = 64;

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

  @BeforeEach
  void installGcs() {
    GcsBucketFolderFileSystem.installGcs(jsc.hadoopConfiguration(), objects());
  }

  @AfterEach
  void uninstallGcs() {
    GcsBucketFolderFileSystem.uninstallGcs(jsc.hadoopConfiguration());
  }

  private Path objects() {
    return tempDir.resolve("objects");
  }

  private String eventsPath() {
    return tempDir.resolve("gcs-events").toUri().toString();
  }

  private String table(String name) {
    return tempDir.resolve(name).toUri().toString();
  }

  /** Writes {@code bytes} as the object {@code gs://landing/<name>} and returns its notification. */
  private Notification put(String name, byte[] bytes) throws IOException {
    Path file = objects().resolve(BUCKET).resolve(name);
    Files.createDirectories(file.getParent());
    Files.write(file, bytes);
    return new Notification(BUCKET, name, bytes.length);
  }

  private void announce(Notification... notifications) {
    GcsEventsTableFixture.append(sparkSession, eventsPath(), Arrays.asList(notifications));
  }

  /** Puts the GCS-specific keys on a job's properties: the cloud, the GCS events table, and no S3 path prefix. */
  private TypedProperties onGcs(TypedProperties props) {
    props.remove(S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX.key());
    props.setProperty(UnstructuredSourceConfig.CLOUD.key(), "gcs");
    props.setProperty(HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key(), eventsPath());
    return props;
  }

  /** The blob job's properties of blob-table.properties, on GCS. */
  private TypedProperties blobJob() throws IOException {
    return onGcs(BlobJobHarness.under(tempDir.resolve("unused")).jobProperties(INLINE_MAX));
  }

  private UnstructuredDataSource source(TypedProperties props) {
    return new UnstructuredDataSource(props, jsc, sparkSession, mock(HoodieIngestionMetrics.class),
        new DefaultStreamContext(null, Option.empty()));
  }

  private static void sync(HoodieStreamer.Config cfg, TypedProperties props) throws Exception {
    HoodieStreamer streamer = new HoodieStreamer(cfg, jsc, Option.of(props));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
    }
  }

  private static List<HoodieInstant> commits(String basePath) {
    return HoodieTableMetaClient.builder().setBasePath(basePath).setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).build()
        .getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants();
  }

  /** The files under the table's {@code .hoodie/.blob}: the blob job writes none. */
  private static long outOfLineFiles(String table) throws IOException {
    Path blobs = Paths.get(URI.create(table)).resolve(".hoodie").resolve(".blob");
    if (!Files.exists(blobs)) {
      return 0;
    }
    try (Stream<Path> files = Files.walk(blobs)) {
      return files.filter(Files::isRegularFile).filter(f -> !f.getFileName().toString().startsWith(".")).count();
    }
  }

  @Test
  void gcsBlobRoundWritesBlobRows() throws Exception {
    announce(put("docs/a.pdf", BlobJobHarness.fileBytes("a.pdf", 32)), put("docs/b.pdf", BlobJobHarness.fileBytes("b.pdf", 1024)));
    String table = table("blob-table");
    sync(BlobJobHarness.streamerConfig(table), blobJob());

    assertEquals(1, commits(table).size());
    List<Row> rows = sparkSession.read().format("hudi").load(table).collectAsList();
    assertEquals(Arrays.asList("gs://landing/docs/a.pdf", "gs://landing/docs/b.pdf"), rows.stream()
        .map(row -> row.<String>getAs(BlobTableSchema.SOURCE_FILE_PATH)).sorted().collect(Collectors.toList()));
    assertEquals(0, outOfLineFiles(table), "the large object is referenced where it is");
    assertEquals(String.join(",", BlobTableSchema.KEY_COLUMNS),
        HoodieTableMetaClient.builder().setBasePath(table).setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).build()
            .getTableConfig().getRecordKeyFieldProp());
  }

  @Test
  void gcsEmbeddingPrepOnlyRoundWritesChunks() throws Exception {
    announce(put("docs/c.pdf", PdfFixtures.text("Omicron pi rho sigma.")));
    String table = table("embedding-table");
    TypedProperties props = onGcs(EmbeddingStreamers.jobProps(table, eventsPath(), "unused", 1024 * 1024,
        EmbeddingTableConfig.MODE_PREP_ONLY, new TypedProperties()));
    sync(EmbeddingStreamers.config(table), props);

    assertEquals(1, commits(table).size());
    List<Row> rows = sparkSession.read().format("hudi").load(table).collectAsList();
    assertFalse(rows.isEmpty(), "the PDF was prepared");
    assertTrue(rows.stream().allMatch(row -> "gs://landing/docs/c.pdf".equals(row.getAs(EmbeddingTableSchema.SOURCE_FILE_PATH))),
        rows.toString());
    assertTrue(rows.stream().anyMatch(row -> EmbeddingTableSchema.MODALITY_TEXT.equals(row.getAs(EmbeddingTableSchema.MODALITY))), rows.toString());
  }

  /** GCS notifications carry no ordering token: two announcements of one object in a batch build one row, of the object as it is. */
  @Test
  void gcsDuplicateAnnouncementsKeepFirst() throws Exception {
    Notification same = put("docs/same.pdf", BlobJobHarness.fileBytes("same.pdf", 10));
    announce(same, same, new Notification(BUCKET, "docs/same.pdf", 11));
    Pair<Option<Dataset<Row>>, Checkpoint> batch = source(blobJob()).fetchNextBatch(Option.empty(), Long.MAX_VALUE);
    List<Row> rows = batch.getLeft().get().collectAsList();
    assertEquals(1, rows.size(), rows.toString());
    assertEquals(10L, rows.get(0).<Long>getAs(BlobTableSchema.SOURCE_FILE_SIZE), "the row is the object's current version");
    assertEquals(UnstructuredCloud.GCS, source(blobJob()).cloud());
  }

  /**
   * A GCS events table with no commit yet: the first round is caught up, so it is empty and its checkpoint is the end instant, the
   * same checkpoint the stock GCS source gives there. After the table's first commit, the round reads it, and the next round from
   * its checkpoint is empty and keeps it, as the stock GCS source does.
   */
  @Test
  void gcsCaughtUpReturnsEndInstant() throws Exception {
    HoodieTableMetaClient.newTableBuilder().setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("gcs_events")
        .setRecordKeyFields("id").initTable(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()), eventsPath());
    TypedProperties props = blobJob();
    Pair<Option<Dataset<Row>>, Checkpoint> empty = source(props).fetchNextBatch(Option.empty(), Long.MAX_VALUE);
    assertFalse(empty.getLeft().isPresent(), "no rows yet");
    assertEquals(stock(props, Option.empty()).getRight(), empty.getRight());

    announce(put("docs/a.pdf", BlobJobHarness.fileBytes("a.pdf", 10)));
    String commit = commits(eventsPath()).get(0).requestedTime();
    Pair<Option<Dataset<Row>>, Checkpoint> first = source(props).fetchNextBatch(Option.empty(), Long.MAX_VALUE);
    assertEquals(1, first.getLeft().get().count());
    assertEquals(commit + "#docs/a.pdf", first.getRight().getCheckpointKey());
    Pair<Option<Dataset<Row>>, Checkpoint> next = source(props).fetchNextBatch(Option.of(first.getRight()), Long.MAX_VALUE);
    assertTrue(!next.getLeft().isPresent() || next.getLeft().get().isEmpty(), "nothing follows the checkpoint");
    assertEquals(stock(props, Option.of(first.getRight())).getRight(), next.getRight());
  }

  /** The stock GCS source's round over the same events table. */
  private Pair<Option<Dataset<Row>>, Checkpoint> stock(TypedProperties props, Option<Checkpoint> last) {
    return new GcsEventsHoodieIncrSource(props, jsc, sparkSession, (SchemaProvider) null, mock(HoodieIngestionMetrics.class))
        .fetchNextBatch(last, Long.MAX_VALUE);
  }
}
