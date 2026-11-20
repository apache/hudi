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

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.config.SqlTransformerConfig;
import org.apache.hudi.utilities.exception.HoodieTransformPlanException;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.testutils.ParquetFooters;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.transform.Transformer;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.streamer.EmbeddingTableTransformer;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Blob job commits over a real S3 events Hudi table. */
@Tag("functional")
class TestBlobTableStreamer extends UtilitiesTestBase {
  private static final long THRESHOLD = 64;

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

  private BlobJobHarness harness() {
    return BlobJobHarness.under(tempDir.resolve("src")).install(jsc.hadoopConfiguration());
  }

  private String table() {
    return tempDir.resolve("table").toUri().toString();
  }

  private static void runStreamer(TypedProperties props, String table, long limit) throws Exception {
    runStreamer(props, table, limit, HoodieTableType.COPY_ON_WRITE);
  }

  private static void runStreamer(TypedProperties props, String table, long limit, HoodieTableType tableType) throws Exception {
    HoodieStreamer.Config cfg = BlobJobHarness.streamerConfig(table, tableType);
    cfg.sourceLimit = limit;
    HoodieStreamer streamer = new HoodieStreamer(cfg, jsc, Option.of(props));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
    }
  }

  private static HoodieTableMetaClient metaClient(String table) {
    return HoodieTableMetaClient.builder().setBasePath(table)
        .setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).build();
  }

  private static List<HoodieInstant> commits(String table) {
    return metaClient(table).getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants();
  }

  private static String checkpoint(String table) throws Exception {
    List<HoodieInstant> commits = commits(table);
    return metaClient(table).getActiveTimeline().readCommitMetadata(commits.get(commits.size() - 1))
        .getMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1);
  }

  private static List<Row> rows(String table) {
    return sparkSession.read().format("hudi").load(table).orderBy(BlobTableSchema.SOURCE_FILE_NAME).collectAsList();
  }

  private static byte[] storedBytes(String table, Row row) throws Exception {
    Row blob = row.getAs(BlobTableSchema.BLOB);
    if (HoodieSchema.Blob.INLINE.equals(blob.getString(0))) {
      assertNull(blob.get(2));
      return (byte[]) blob.get(1);
    }
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, blob.getString(0));
    assertNull(blob.get(1));
    // a large object stays where it is: an unmanaged reference to its own URI, as listed, never a copy under the table
    Row reference = blob.getStruct(2);
    assertEquals(row.<String>getAs(BlobTableSchema.SOURCE_FILE_PATH), reference.getString(0));
    assertNull(reference.get(1));
    assertNull(reference.get(2));
    assertFalse(reference.getBoolean(3));
    org.apache.hadoop.fs.Path source = new org.apache.hadoop.fs.Path(reference.getString(0));
    try (org.apache.hadoop.fs.FSDataInputStream in = source.getFileSystem(jsc.hadoopConfiguration()).open(source)) {
      return in.readAllBytes();
    }
  }

  /** No committed row of the blob table has a null blob: an object without bytes is quarantined, never committed. */
  private static void assertNoNullBlob(String table) {
    List<String> nullBlobs = sparkSession.read().format("hudi").load(table).filter(functions.col(BlobTableSchema.BLOB).isNull())
        .select(BlobTableSchema.SOURCE_FILE_PATH).collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
    assertEquals(Collections.emptyList(), nullBlobs, "committed rows with a null blob");
  }

  /** Every row is the current version of its object: one row per expected key, keyed by the object's version tag. */
  private void assertContent(String table, Map<String, byte[]> expected) throws Exception {
    assertNoNullBlob(table);
    List<Row> rows = rows(table);
    assertEquals(expected.size(), rows.size());
    BlobJobHarness harness = harness();
    for (Row row : rows) {
      String path = row.getAs(BlobTableSchema.SOURCE_FILE_PATH);
      String key = path.substring(BlobJobHarness.sourcePath("").length());
      byte[] bytes = expected.get(key);
      assertNotNull(bytes, path);
      String sha = BlobFileNames.sourcePathSha(path);
      assertEquals(sha, row.getAs(BlobTableSchema.SOURCE_PATH_SHA));
      assertEquals(Files.getLastModifiedTime(harness.file(key)).toMillis(), row.<Long>getAs(BlobTableSchema.SOURCE_MODIFICATION_TIME));
      assertEquals(harness.versionKey(key), row.getAs("_hoodie_record_key"));
      assertEquals(BlobFileNames.fileName(path), row.getAs(BlobTableSchema.SOURCE_FILE_NAME));
      String extension = BlobFileNames.extension(path);
      assertEquals(extension, row.getAs(BlobTableSchema.SOURCE_FILE_EXTENSION));
      assertEquals(BlobFileNames.contentType(extension), row.getAs(BlobTableSchema.CONTENT_TYPE));
      assertEquals((long) bytes.length, row.<Long>getAs(BlobTableSchema.SOURCE_FILE_SIZE));
      byte[] actual = storedBytes(table, row);
      assertArrayEquals(bytes, actual, key);
      assertEquals(BlobJobHarness.sha256Hex(bytes), BlobJobHarness.sha256Hex(actual), key);
    }
  }

  /** Whether the blob job created the table's out-of-line folder: it never copies, so it never should. */
  private static boolean blobFolderExists(String table) {
    return Files.exists(Paths.get(java.net.URI.create(table + "/.hoodie/.blob")));
  }

  @Test
  void ingestsIncrementallyWithBlobStorageRule() throws Exception {
    BlobJobHarness harness = harness();
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("docs/a.pdf", 32);
    sizes.put("img/b.png", 1024);
    sizes.put("video/c.mp4", 256);
    Map<String, byte[]> expected = new TreeMap<>(harness.putAndAnnounce(sparkSession, sizes));
    TypedProperties props = harness.jobProperties(THRESHOLD);
    runStreamer(props, table(), Long.MAX_VALUE);
    assertContent(table(), expected);
    expected.putAll(harness.putAndAnnounce(sparkSession, Collections.singletonMap("docs/d.pdf", 10)));
    runStreamer(props, table(), Long.MAX_VALUE);
    runStreamer(props, table(), Long.MAX_VALUE);
    assertEquals(2, commits(table()).size(), "no-new-data starts no additional commit");
    assertContent(table(), expected);
    HoodieSchema schema = new TableSchemaResolver(metaClient(table())).getTableSchema(false);
    assertEquals(new HashSet<>(Arrays.asList(BlobTableSchema.schema().fieldNames())),
        schema.getFields().stream().map(field -> field.name()).collect(Collectors.toSet()));
    assertTrue(schema.toAvroSchema().getField(BlobTableSchema.BLOB).schema().toString().contains("\"logicalType\":\"blob\""));
    assertEquals(BlobTableSchema.SOURCE_PATH_SHA + "," + BlobTableSchema.SOURCE_MODIFICATION_TIME + "," + BlobTableSchema.SOURCE_FILE_SIZE,
        metaClient(table()).getTableConfig().getRecordKeyFieldProp());
    assertFalse(blobFolderExists(table()), "nothing is copied under .hoodie/.blob");
  }

  /** A small real commit at the actual default threshold, including an out-of-line reference larger than the default streaming buffer. */
  @Test
  void defaultThresholdBatchPreservesInlineAndOutOfLineBytes() throws Exception {
    BlobJobHarness harness = harness();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    props.remove(BlobTableConfig.INLINE_MAX_BYTES.key());
    long threshold = BlobTableConfig.inlineMaxBytes(props);
    boolean statisticsSwitch;
    try {
      org.apache.parquet.hadoop.ParquetWriter.Builder.class.getMethod("withStatisticsEnabled", String.class, boolean.class);
      statisticsSwitch = true;
    } catch (NoSuchMethodException e) {
      statisticsSwitch = false;
    }
    assertEquals(statisticsSwitch ? 1048576L : 10240L, threshold);
    int inlineBytes = Math.toIntExact(threshold);
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("docs/near.pdf", inlineBytes - 1024);
    sizes.put("img/below.png", inlineBytes - 1);
    sizes.put("img/at.png", inlineBytes);
    sizes.put("video/above.mp4", inlineBytes + 1);
    Map<String, byte[]> expected = harness.putAndAnnounce(sparkSession, sizes);

    runStreamer(props, table(), Long.MAX_VALUE);

    assertEquals(1, commits(table()).size());
    assertContent(table(), expected);
    for (Row row : rows(table())) {
      Row blob = row.getAs(BlobTableSchema.BLOB);
      long length = row.getAs(BlobTableSchema.SOURCE_FILE_SIZE);
      assertEquals(length <= threshold ? HoodieSchema.Blob.INLINE : HoodieSchema.Blob.OUT_OF_LINE, blob.getString(0));
    }
    assertFalse(blobFolderExists(table()), "nothing is copied under .hoodie/.blob");
    HoodieSchema schema = new TableSchemaResolver(metaClient(table())).getTableSchema(false);
    assertEquals(new HashSet<>(Arrays.asList(BlobTableSchema.schema().fieldNames())),
        schema.getFields().stream().map(field -> field.name()).collect(Collectors.toSet()));
    assertTrue(schema.toAvroSchema().getField(BlobTableSchema.BLOB).schema().toString().contains("\"logicalType\":\"blob\""));
  }

  /** Stock key-generator inference for no partition column and no key generator: the non-partitioned generator. */
  @Test
  void nonPartitionedJobWithNoKeyGeneratorWritesNonPartitionedRows() throws Exception {
    BlobJobHarness harness = harness();
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("docs/flat.pdf", 32);
    sizes.put("img/large.png", 128);
    Map<String, byte[]> expected = harness.putAndAnnounce(sparkSession, sizes);
    TypedProperties props = harness.jobProperties(THRESHOLD);
    props.remove("hoodie.datasource.write.partitionpath.field");
    props.remove("hoodie.datasource.write.keygenerator.class");
    runStreamer(props, table(), Long.MAX_VALUE);
    assertEquals(KeyGeneratorType.NON_PARTITION.name(), metaClient(table()).getTableConfig().getString(HoodieTableConfig.KEY_GENERATOR_TYPE));
    assertContent(table(), expected);
    for (Row row : rows(table())) {
      assertEquals("", row.getAs("_hoodie_partition_path"));
    }
    assertFalse(blobFolderExists(table()), "nothing is copied under .hoodie/.blob");
  }

  /** Stock key-generator inference for one configured partition column and the three key columns: the complex generator. */
  @Test
  void onePartitionFieldWithNoKeyGeneratorWritesComplexPartitionedRows() throws Exception {
    BlobJobHarness harness = harness();
    Map<String, byte[]> expected = harness.putAndAnnounce(sparkSession, Collections.singletonMap("docs/by-extension.pdf", 128));
    TypedProperties props = harness.jobProperties(THRESHOLD);
    props.remove("hoodie.datasource.write.keygenerator.class");
    runStreamer(props, table(), Long.MAX_VALUE);
    assertEquals(KeyGeneratorType.COMPLEX.name(), metaClient(table()).getTableConfig().getString(HoodieTableConfig.KEY_GENERATOR_TYPE));
    assertContent(table(), expected);
    assertEquals("pdf", rows(table()).get(0).getAs("_hoodie_partition_path"));
  }

  @Test
  void splitRoundRestartResumesStockV1Checkpoint() throws Exception {
    BlobJobHarness harness = harness();
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("a.pdf", 32);
    sizes.put("b.pdf", 32);
    sizes.put("c.pdf", 32);
    Map<String, byte[]> expected = harness.putAndAnnounce(sparkSession, sizes);
    String event = commits(harness.eventsPath()).get(0).requestedTime();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    props.setProperty(CloudSourceConfig.SOURCE_MAX_FILES_PER_SYNC.key(), "2");
    runStreamer(props, table(), 64);
    assertEquals(event + "#b.pdf", checkpoint(table()));
    assertEquals(2, rows(table()).size());
    runStreamer(props, table(), 64);
    assertEquals(event + "#c.pdf", checkpoint(table()));
    assertContent(table(), expected);
    runStreamer(props, table(), 64);
    assertEquals(2, commits(table()).size());
  }

  /**
   * Both version-tag fields independently retain history; reannouncing current versions also exercises real MOR log payloads.
   * Once a large object is overwritten, its version rows reference the same source file: every row reads its full current
   * content, including when several versions are read together with {@code read_blob}.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void versionHistoryKeepsMetadataAndOverwrittenReferencesShareTheSourceFile(HoodieTableType tableType) throws Exception {
    BlobJobHarness harness = harness();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    long firstTime = Instant.parse("2026-01-02T10:00:00Z").toEpochMilli();
    long secondTime = Instant.parse("2026-01-02T10:00:01Z").toEpochMilli();
    ExpectedBlobVersion inlineV1 = new ExpectedBlobVersion("docs/inline.pdf", firstTime, BlobJobHarness.fileBytes("inline v1", 32));
    ExpectedBlobVersion referenceV1 = new ExpectedBlobVersion("docs/reference.pdf", firstTime, BlobJobHarness.fileBytes("reference v1", 128));
    harness.announce(sparkSession, putVersions(harness, inlineV1, referenceV1));
    runStreamer(props, table(), Long.MAX_VALUE, tableType);
    String firstCommit = commits(table()).get(0).requestedTime();
    assertEquals(tableType, metaClient(table()).getTableType());
    Map<String, ExpectedBlobVersion> expected = new TreeMap<>();
    expected.put(inlineV1.recordKey(), inlineV1);
    expected.put(referenceV1.recordKey(), referenceV1);
    assertHistory(expected);

    // Same length and changed mtime; changed length and unchanged mtime: each key component creates a new version.
    ExpectedBlobVersion inlineV2 = new ExpectedBlobVersion("docs/inline.pdf", secondTime, BlobJobHarness.fileBytes("inline v2", 32));
    ExpectedBlobVersion referenceV2 = new ExpectedBlobVersion("docs/reference.pdf", firstTime, BlobJobHarness.fileBytes("reference v2", 257));
    List<S3EventsTableFixture.Event> currentEvents = putVersions(harness, inlineV2, referenceV2);
    harness.announce(sparkSession, currentEvents);
    runStreamer(props, table(), Long.MAX_VALUE, tableType);
    assertEquals(2, commits(table()).size());
    expected.put(inlineV2.recordKey(), inlineV2);
    expected.put(referenceV2.recordKey(), referenceV2);
    Map<String, Row> before = assertHistory(expected);
    assertFalse(blobFolderExists(table()), "no version is copied under .hoodie/.blob");

    harness.announce(sparkSession, currentEvents);
    runStreamer(props, table(), Long.MAX_VALUE, tableType);
    assertEquals(3, commits(table()).size(), "the unchanged announcements are consumed");
    String reannouncementCommit = commits(table()).get(2).requestedTime();
    Map<String, Row> after = assertHistory(expected);
    assertEquals(firstCommit, after.get(inlineV1.recordKey()).getAs("_hoodie_commit_time"));
    assertEquals(firstCommit, after.get(referenceV1.recordKey()).getAs("_hoodie_commit_time"));
    assertEquals(reannouncementCommit, after.get(inlineV2.recordKey()).getAs("_hoodie_commit_time"));
    assertEquals(reannouncementCommit, after.get(referenceV2.recordKey()).getAs("_hoodie_commit_time"));
    assertEquals(referencePath(before.get(referenceV2.recordKey())), referencePath(after.get(referenceV2.recordKey())),
        "a re-announced object is referenced where it is, as before");
    assertEquals(BlobJobHarness.sourcePath(referenceV2.objectKey), referencePath(after.get(referenceV2.recordKey())));

    if (tableType == HoodieTableType.MERGE_ON_READ) {
      Map<String, ExpectedBlobVersion> updated = new TreeMap<>();
      updated.put(inlineV2.recordKey(), inlineV2);
      updated.put(referenceV2.recordKey(), referenceV2);
      assertLogPayloads(reannouncementCommit, updated, after);
      List<Row> baseRows = sparkSession.read().format("hudi").option("hoodie.datasource.query.type", "read_optimized")
          .load(table()).collectAsList();
      assertFalse(baseRows.isEmpty(), "the initial insert supplies base files");
      for (Row row : baseRows) {
        assertNotEquals(reannouncementCommit, row.getAs("_hoodie_commit_time"), "current-version provenance must come from logs");
        assertArrayEquals(expectedRead(expected.get(row.<String>getAs("_hoodie_record_key"))), storedBytes(table(), row));
      }
    }
    runStreamer(props, table(), Long.MAX_VALUE, tableType);
    assertEquals(3, commits(table()).size(), "no-new-data retry must not commit");
    assertHistory(expected);
  }

  private static List<S3EventsTableFixture.Event> putVersions(BlobJobHarness harness, ExpectedBlobVersion... versions) throws IOException {
    List<S3EventsTableFixture.Event> events = new ArrayList<>();
    for (ExpectedBlobVersion version : versions) {
      events.add(harness.put(version.objectKey, version.bytes));
      harness.touch(version.objectKey, Instant.ofEpochMilli(version.modificationTime));
    }
    return events;
  }

  /**
   * Exact historical metadata. An INLINE version keeps its own bytes; an OUT_OF_LINE version references the object where it is, so
   * it reads the whole object as it is now: a later overwrite is not frozen (an accepted cost of referencing in place).
   */
  private Map<String, Row> assertHistory(Map<String, ExpectedBlobVersion> expected) throws Exception {
    List<Row> snapshot = rows(table());
    assertEquals(expected.size(), snapshot.size());
    Map<String, Row> actual = snapshot.stream().collect(Collectors.toMap(row -> row.<String>getAs("_hoodie_record_key"), row -> row));
    assertEquals(expected.keySet(), actual.keySet());
    for (Map.Entry<String, ExpectedBlobVersion> entry : expected.entrySet()) {
      ExpectedBlobVersion version = entry.getValue();
      Row row = actual.get(entry.getKey());
      String path = BlobJobHarness.sourcePath(version.objectKey);
      assertEquals(BlobJobHarness.sha256Hex(path.getBytes(StandardCharsets.UTF_8)).substring(0, 32), row.getAs(BlobTableSchema.SOURCE_PATH_SHA));
      assertEquals(version.modificationTime, row.<Long>getAs(BlobTableSchema.SOURCE_MODIFICATION_TIME));
      assertEquals(path, row.getAs(BlobTableSchema.SOURCE_FILE_PATH));
      assertEquals(version.objectKey.substring(version.objectKey.lastIndexOf('/') + 1), row.getAs(BlobTableSchema.SOURCE_FILE_NAME));
      assertEquals("pdf", row.getAs(BlobTableSchema.SOURCE_FILE_EXTENSION));
      assertEquals("application/pdf", row.getAs(BlobTableSchema.CONTENT_TYPE));
      assertEquals((long) version.bytes.length, row.<Long>getAs(BlobTableSchema.SOURCE_FILE_SIZE));
      Row blob = row.getAs(BlobTableSchema.BLOB);
      assertEquals(version.bytes.length <= THRESHOLD ? HoodieSchema.Blob.INLINE : HoodieSchema.Blob.OUT_OF_LINE, blob.getString(0));
      assertArrayEquals(expectedRead(version), storedBytes(table(), row), entry.getKey());
    }
    sparkSession.read().format("hudi").load(table()).createOrReplaceTempView("versioned_blob_snapshot");
    List<Row> resolved = sparkSession.sql("select _hoodie_record_key, read_blob(blob) as data from versioned_blob_snapshot").collectAsList();
    assertEquals(expected.keySet(), resolved.stream().map(row -> row.getString(0)).collect(Collectors.toSet()));
    for (Row row : resolved) {
      assertArrayEquals(expectedRead(expected.get(row.getString(0))), row.getAs("data"), row.getString(0));
    }
    return actual;
  }

  /** What a read of {@code version} returns: its own bytes when inline, else the object's full current bytes. */
  private byte[] expectedRead(ExpectedBlobVersion version) throws IOException {
    if (version.bytes.length <= THRESHOLD) {
      return version.bytes;
    }
    return Files.readAllBytes(harness().file(version.objectKey));
  }

  private static String referencePath(Row row) {
    Row blob = row.getAs(BlobTableSchema.BLOB);
    return blob.getStruct(2).getString(0);
  }

  /** Decodes the current round's physical data blocks: inline bytes and the exact reference returned by the live snapshot. */
  private void assertLogPayloads(String instant, Map<String, ExpectedBlobVersion> expected, Map<String, Row> snapshot) throws Exception {
    HoodieTableMetaClient meta = metaClient(table());
    HoodieSchema schema = new TableSchemaResolver(meta).getTableSchema(true);
    List<HoodieWriteStat> writes = meta.getActiveTimeline().readCommitMetadata(commits(table()).get(2)).getWriteStats();
    assertEquals(2L, writes.stream().mapToLong(HoodieWriteStat::getNumWrites).sum());
    Set<String> seen = new HashSet<>();
    for (String path : writes.stream().map(HoodieWriteStat::getPath).collect(Collectors.toSet())) {
      assertTrue(FSUtils.isLogFile(path), path);
      assertTrue(Files.size(Paths.get(java.net.URI.create(table() + "/" + path))) > 0);
      HoodieLogFile file = new HoodieLogFile(new StoragePath(table(), path));
      try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(meta, file, schema)) {
        while (reader.hasNext()) {
          HoodieLogBlock block = reader.next();
          if (!(block instanceof HoodieDataBlock) || !instant.equals(block.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME))) {
            continue;
          }
          assertLogBlock((HoodieDataBlock) block, instant, expected, snapshot, seen);
        }
      }
    }
    assertEquals(expected.keySet(), seen, "both inline data and the unmanaged reference must be present in actual log blocks");
  }

  private static void assertLogBlock(HoodieDataBlock block, String instant, Map<String, ExpectedBlobVersion> expected,
      Map<String, Row> snapshot, Set<String> seen) {
    try (ClosableIterator<HoodieRecord<IndexedRecord>> records = block.getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
      while (records.hasNext()) {
        GenericRecord logRecord = (GenericRecord) records.next().getData();
        String key = logRecord.get("_hoodie_record_key").toString();
        assertTrue(seen.add(key), "one log record per current version: " + key);
        ExpectedBlobVersion version = expected.get(key);
        assertNotNull(version, key);
        assertLogVersion(logRecord, version, snapshot.get(key), instant);
      }
    }
  }

  private static void assertLogVersion(GenericRecord logRecord, ExpectedBlobVersion version, Row snapshot, String instant) {
    assertEquals(instant, logRecord.get("_hoodie_commit_time").toString());
    assertEquals(version.modificationTime, logRecord.get(BlobTableSchema.SOURCE_MODIFICATION_TIME));
    assertEquals((long) version.bytes.length, logRecord.get(BlobTableSchema.SOURCE_FILE_SIZE));
    GenericRecord blob = (GenericRecord) logRecord.get(BlobTableSchema.BLOB);
    if (version.bytes.length <= THRESHOLD) {
      assertEquals(HoodieSchema.Blob.INLINE, blob.get("type").toString());
      assertNull(blob.get("reference"));
      ByteBuffer data = ((ByteBuffer) blob.get("data")).duplicate();
      byte[] bytes = new byte[data.remaining()];
      data.get(bytes);
      assertArrayEquals(version.bytes, bytes, version.recordKey());
    } else {
      assertEquals(HoodieSchema.Blob.OUT_OF_LINE, blob.get("type").toString());
      assertNull(blob.get("data"));
      GenericRecord reference = (GenericRecord) blob.get("reference");
      assertEquals(referencePath(snapshot), reference.get("external_path").toString());
      assertNull(reference.get("offset"));
      assertNull(reference.get("length"));
      assertEquals(Boolean.FALSE, reference.get("managed"));
    }
  }

  private static class ExpectedBlobVersion {
    private final String objectKey;
    private final long modificationTime;
    private final byte[] bytes;

    private ExpectedBlobVersion(String objectKey, long modificationTime, byte[] bytes) {
      this.objectKey = objectKey;
      this.modificationTime = modificationTime;
      this.bytes = bytes;
    }

    private String recordKey() {
      return BlobJobHarness.versionKey(BlobJobHarness.sourcePath(objectKey), modificationTime, bytes.length);
    }
  }

  /**
   * An overwrite is a new version of the object: its row is added and the earlier version's row stays. Both reference the object
   * where it is, so both rows read the whole replacement: an overwrite is not frozen.
   */
  @Test
  void overwriteAddsAVersionRow() throws Exception {
    BlobJobHarness harness = harness();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    harness.putAndAnnounce(sparkSession, Collections.singletonMap("docs/a.pdf", 128));
    runStreamer(props, table(), Long.MAX_VALUE);
    String firstKey = harness.versionKey("docs/a.pdf");
    byte[] replacement = BlobJobHarness.fileBytes("replacement", 257);
    harness.announce(sparkSession, Collections.singletonList(harness.put("docs/a.pdf", replacement)));
    runStreamer(props, table(), Long.MAX_VALUE);
    String secondKey = harness.versionKey("docs/a.pdf");
    assertNotEquals(firstKey, secondKey, "the overwrite is another version");
    Map<String, byte[]> versions = new TreeMap<>();
    versions.put(firstKey, replacement);
    versions.put(secondKey, replacement);
    assertVersions(table(), versions);
    assertEquals(2, commits(table()).size());
    assertFalse(blobFolderExists(table()), "no version is copied under .hoodie/.blob");
  }

  /** The same object announced again in a later batch has the same version tag: the upsert replaces the row, nothing is added. */
  @Test
  void unchangedObjectInALaterBatchIsIdempotent() throws Exception {
    BlobJobHarness harness = harness();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    byte[] bytes = harness.putAndAnnounce(sparkSession, Collections.singletonMap("a.pdf", 32)).get("a.pdf");
    runStreamer(props, table(), Long.MAX_VALUE);
    String key = harness.versionKey("a.pdf");
    // announced again without touching the object
    harness.announce(sparkSession, Collections.singletonList(S3EventsTableFixture.created(BlobJobHarness.BUCKET, "a.pdf", 32, BlobJobHarness.EVENT_TIME)));
    runStreamer(props, table(), Long.MAX_VALUE);
    assertEquals(2, commits(table()).size(), "the second batch is consumed");
    assertVersions(table(), Collections.singletonMap(key, bytes));
  }

  /**
   * Stock selection keeps one entry per (path, announced size): two same-size notifications collapse, a differently announced size
   * stays a second entry. Every entry reads the object as it is now, so one version is written: one row.
   */
  @Test
  void sameBatchRepeatedEventsOfOnePathEndAsOneRow() throws Exception {
    BlobJobHarness harness = harness();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    byte[] bytes = BlobJobHarness.fileBytes("a.pdf", 32);
    harness.put("a.pdf", bytes);
    harness.announce(sparkSession, Arrays.asList(
        S3EventsTableFixture.created(BlobJobHarness.BUCKET, "a.pdf", 32, BlobJobHarness.EVENT_TIME).withSequencer("01"),
        S3EventsTableFixture.created(BlobJobHarness.BUCKET, "a.pdf", 32, BlobJobHarness.EVENT_TIME).withSequencer("02"),
        S3EventsTableFixture.created(BlobJobHarness.BUCKET, "a.pdf", 48, BlobJobHarness.EVENT_TIME).withSequencer("03")));
    runStreamer(props, table(), Long.MAX_VALUE);
    assertEquals(1, commits(table()).size());
    assertVersions(table(), Collections.singletonMap(harness.versionKey("a.pdf"), bytes));
  }

  /**
   * The first version's event, processed after the second version replaced the object, reads the second version (a no-op upsert
   * of its key); the first version is never ingested, since it is gone.
   */
  @Test
  void outOfOrderEventsIngestOnlyTheCurrentVersion() throws Exception {
    BlobJobHarness harness = harness();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    byte[] v1 = BlobJobHarness.fileBytes("v1", 32);
    S3EventsTableFixture.Event firstEvent = harness.put("a.pdf", v1);
    harness.touch("a.pdf", Instant.parse("2026-01-02T10:00:00Z"));
    byte[] v2 = BlobJobHarness.fileBytes("v2", 48);
    S3EventsTableFixture.Event secondEvent = harness.put("a.pdf", v2);
    harness.touch("a.pdf", Instant.parse("2026-01-02T10:00:01Z"));
    harness.announce(sparkSession, Collections.singletonList(secondEvent));
    runStreamer(props, table(), Long.MAX_VALUE);
    harness.announce(sparkSession, Collections.singletonList(firstEvent));
    runStreamer(props, table(), Long.MAX_VALUE);
    assertEquals(2, commits(table()).size());
    assertVersions(table(), Collections.singletonMap(
        BlobJobHarness.versionKey(BlobJobHarness.sourcePath("a.pdf"), Instant.parse("2026-01-02T10:00:01Z").toEpochMilli(), 48L), v2));
  }

  /** Two uploads of the same length are two versions when their modification times differ. */
  @Test
  void sameSizeOverwriteWithALaterModificationTimeAddsAVersionRow() throws Exception {
    BlobJobHarness harness = harness();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    byte[] first = BlobJobHarness.fileBytes("first", 32);
    harness.announce(sparkSession, Collections.singletonList(harness.put("a.pdf", first)));
    harness.touch("a.pdf", Instant.parse("2026-01-02T10:00:00Z"));
    runStreamer(props, table(), Long.MAX_VALUE);
    byte[] second = BlobJobHarness.fileBytes("second", 32);
    harness.announce(sparkSession, Collections.singletonList(harness.put("a.pdf", second)));
    harness.touch("a.pdf", Instant.parse("2026-01-02T10:00:01Z"));
    runStreamer(props, table(), Long.MAX_VALUE);
    Map<String, byte[]> versions = new TreeMap<>();
    versions.put(BlobJobHarness.versionKey(BlobJobHarness.sourcePath("a.pdf"), Instant.parse("2026-01-02T10:00:00Z").toEpochMilli(), 32L), first);
    versions.put(BlobJobHarness.versionKey(BlobJobHarness.sourcePath("a.pdf"), Instant.parse("2026-01-02T10:00:01Z").toEpochMilli(), 32L), second);
    assertVersions(table(), versions);
  }

  /** A4: an inline object that shrank between the status call and the read fails the round; the next round reads the new version. */
  @Test
  void shrunkBetweenTheStatusCallAndTheInlineReadFailsTheRoundThenSelfHeals() throws Exception {
    BlobJobHarness harness = harness();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    byte[] bytes = harness.putAndAnnounce(sparkSession, Collections.singletonMap("a.pdf", 32)).get("a.pdf");
    byte[] shorter = Arrays.copyOf(bytes, 16);
    Path file = harness.file("a.pdf");
    // once: the object is overwritten by a shorter version after the status call and before the read
    SourceOpenHooks.beforeOpen(file.toAbsolutePath().toString(), () -> {
      SourceOpenHooks.clear();
      Files.write(file, shorter);
    });
    try {
      Exception failure = assertThrows(Exception.class, () -> runStreamer(props, table(), Long.MAX_VALUE));
      StringBuilder causes = new StringBuilder();
      for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
        causes.append(cause.getClass().getName()).append(": ").append(cause.getMessage()).append("\n");
      }
      assertTrue(causes.toString().contains("EOFException"), causes.toString());
    } finally {
      SourceOpenHooks.clear();
    }
    assertEquals(0, commits(table()).size(), "the round failed before any commit");
    runStreamer(props, table(), Long.MAX_VALUE);
    assertEquals(1, commits(table()).size());
    assertVersions(table(), Collections.singletonMap(harness.versionKey("a.pdf"), shorter));
  }

  /** Exactly one row per expected version: the rows by record key, each with its version's bytes. */
  private static void assertVersions(String table, Map<String, byte[]> expectedByKey) throws Exception {
    List<Row> rows = rows(table);
    assertEquals(expectedByKey.size(), rows.size(), "one row per version");
    assertEquals(expectedByKey.keySet(), rows.stream().map(row -> row.<String>getAs("_hoodie_record_key")).collect(Collectors.toSet()));
    for (Row row : rows) {
      String key = row.getAs("_hoodie_record_key");
      // The expected version key identifies the observed size, which can differ from the current file's bytes.
      assertEquals(key, BlobJobHarness.versionKey(row.getAs(BlobTableSchema.SOURCE_FILE_PATH),
          row.getAs(BlobTableSchema.SOURCE_MODIFICATION_TIME), row.getAs(BlobTableSchema.SOURCE_FILE_SIZE)));
      assertArrayEquals(expectedByKey.get(key), storedBytes(table, row), key);
    }
  }

  @Test
  void failedCommitKeepsCheckpointAndRetryReferencesResolve() throws Exception {
    BlobJobHarness harness = harness();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    Map<String, byte[]> expected = new TreeMap<>(harness.putAndAnnounce(sparkSession, Collections.singletonMap("a.pdf", 128)));
    runStreamer(props, table(), Long.MAX_VALUE);
    String checkpoint = checkpoint(table());
    expected.putAll(harness.putAndAnnounce(sparkSession, Collections.singletonMap("b.pdf", 256)));
    TypedProperties failing = TypedProperties.copy(props);
    failing.setProperty("hoodie.precommit.validators", "org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator");
    failing.setProperty("hoodie.precommit.validators.single.value.sql.queries", "select count(*) from <TABLE_NAME>#999");
    Exception failure = assertThrows(Exception.class, () -> runStreamer(failing, table(), Long.MAX_VALUE));
    StringBuilder causes = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      causes.append(cause.getMessage()).append("\n");
    }
    assertTrue(causes.toString().contains("validation"), causes.toString());
    assertEquals(checkpoint, checkpoint(table()));
    assertEquals(1, commits(table()).size());
    assertFalse(blobFolderExists(table()), "the failed round copied nothing");
    runStreamer(props, table(), Long.MAX_VALUE);
    assertEquals(2, commits(table()).size());
    assertContent(table(), expected);
    assertFalse(blobFolderExists(table()), "nothing is copied under .hoodie/.blob");
  }

  /**
   * The quarantine transformer runs in every round of the blob source, whatever --transformer-class names: a job naming no
   * transformer, only its own, or the quarantine transformer more than once still has one large object referenced in place and
   * every byte stored.
   * The job's own transformer runs at its configured position (the SQL transformer's column is in the table).
   */
  @ParameterizedTest
  @ValueSource(strings = {"", "org.apache.hudi.utilities.transform.SqlQueryBasedTransformer",
      "org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer,org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer",
      "org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer,org.apache.hudi.utilities.transform.SqlQueryBasedTransformer,"
          + "org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer"})
  void quarantineTransformerRunsWhetherOrNotTheJobNamesIt(String transformers) throws Exception {
    BlobJobHarness harness = harness();
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("small.pdf", 32);
    sizes.put("large.pdf", 1024);
    Map<String, byte[]> expected = harness.putAndAnnounce(sparkSession, sizes);
    HoodieStreamer.Config cfg = BlobJobHarness.streamerConfig(table());
    cfg.transformerClassNames = transformers.isEmpty() ? null : Arrays.asList(transformers.split(","));
    TypedProperties props = harness.jobProperties(THRESHOLD);
    props.setProperty(SqlTransformerConfig.TRANSFORMER_SQL.key(), "SELECT *, 'applied' AS job_marker FROM <SRC>");
    HoodieStreamer streamer = new HoodieStreamer(cfg, jsc, Option.of(props));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
    }
    assertEquals(1, commits(table()).size());
    assertContent(table(), expected);
    assertFalse(blobFolderExists(table()), "the large object is referenced where it is");
    List<String> columns = Arrays.asList(sparkSession.read().format("hudi").load(table()).columns());
    assertEquals(transformers.contains("SqlQueryBasedTransformer"), columns.contains("job_marker"), columns.toString());
  }

  /**
   * The embedding transformer belongs to the embedding source and is refused by the blob source at start, before any object
   * is read or any instant starts.
   */
  @ParameterizedTest
  @ValueSource(strings = {"org.apache.hudi.utilities.unstructured.embedding.streamer.EmbeddingTableTransformer"})
  void chainTheBlobSourceCannotRunIsRefusedAtStart(String transformers) throws Exception {
    BlobJobHarness harness = harness();
    harness.putAndAnnounce(sparkSession, Collections.singletonMap("small.pdf", 32));
    HoodieStreamer.Config cfg = BlobJobHarness.streamerConfig(table());
    cfg.transformerClassNames = Arrays.asList(transformers.split(","));
    String refusal = refusedAtStart(cfg, harness.jobProperties(THRESHOLD), HoodieTransformPlanException.class);
    assertTrue(refusal.contains("UnstructuredDataSource does not run with"), refusal);
    assertTrue(refusal.contains(EmbeddingTableTransformer.class.getName()), refusal);
  }

  /** The refusal of a job at start: its message, and the table has no instant. */
  private String refusedAtStart(HoodieStreamer.Config cfg, TypedProperties props, Class<? extends Exception> type) {
    Exception failure = assertThrows(Exception.class, () -> syncNewStreamer(cfg, props));
    // the streamer keeps each failed source constructor's exception as a suppressed one
    String refusal = EmbeddingStreamers.causeOf(failure, type)
        .orElseThrow(() -> new AssertionError(EmbeddingStreamers.causes(failure))).getMessage();
    // the streamer initializes the table before it builds the source; the refusal leaves it with no instant in any state
    assertTrue(Files.exists(Paths.get(java.net.URI.create(cfg.targetBasePath + "/.hoodie/hoodie.properties"))), cfg.targetBasePath);
    assertEquals(Collections.emptyList(), metaClient(cfg.targetBasePath).getActiveTimeline().getInstants(), "no instant was started");
    return refusal;
  }

  /**
   * The blob table's Parquet settings are the job's when it sets them (blob-table.properties: uncompressed, no record-key bloom
   * filter, the blob column injector); a job naming its own injector there instead of under the delegate key is refused at start,
   * before its table is initialized.
   */
  @Test
  void parquetSettingsAreTheJobsAndAnotherInjectorIsRefused() throws Exception {
    BlobJobHarness harness = harness();
    harness.putAndAnnounce(sparkSession, Collections.singletonMap("small.pdf", 32));
    runStreamer(harness.jobProperties(THRESHOLD), table(), Long.MAX_VALUE);
    ParquetFooters.assertBaseFiles(jsc.hadoopConfiguration(), table(), CompressionCodecName.UNCOMPRESSED, false);

    String otherInjector = tempDir.resolve("other-injector").toUri().toString();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    props.setProperty(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key(), "com.example.JobInjector");
    HoodieStreamer.Config otherInjectorCfg = BlobJobHarness.streamerConfig(otherInjector);
    Exception failure = assertThrows(Exception.class, () -> syncNewStreamer(otherInjectorCfg, props));
    String refusal = EmbeddingStreamers.causeOf(failure, IllegalArgumentException.class)
        .orElseThrow(() -> new AssertionError(EmbeddingStreamers.causes(failure))).getMessage();
    assertTrue(refusal.startsWith("UnstructuredDataSource (mode blob) requires " + HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key() + "="
        + BlobParquetConfigInjector.class.getName()), refusal);
    assertFalse(Files.exists(Paths.get(java.net.URI.create(otherInjector + "/.hoodie/hoodie.properties"))),
        "the job is refused before its table is initialized");
  }

  /**
   * A table keeps its job: the embedding job on a blob table is refused at start, naming the record key, whether it sets the blob
   * table's key itself or leaves it unset; the blob job on an embedding table likewise. Nothing is written.
   */
  @Test
  void modeSwitchOnExistingTableRefused() throws Exception {
    BlobJobHarness harness = harness();
    harness.putAndAnnounce(sparkSession, Collections.singletonMap("a.pdf", 32));
    runStreamer(harness.jobProperties(THRESHOLD), table(), Long.MAX_VALUE);
    assertEquals(1, commits(table()).size());
    HoodieStreamer.Config embeddingCfg = BlobJobHarness.streamerConfig(table());
    embeddingCfg.transformerClassNames = Collections.singletonList(EmbeddingTableTransformer.class.getName());

    TypedProperties keySet = harness.jobProperties(THRESHOLD);
    keySet.setProperty("hoodie.unstructured.source.mode", "embedding");
    keySet.setProperty("hoodie.unstructured.embedding.mode", "prep_only");
    keySet.setProperty(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key(),
        "org.apache.hudi.utilities.unstructured.embedding.EmbeddingParquetConfigInjector");
    String setRefusal = modeSwitchRefusal(embeddingCfg, keySet);
    assertTrue(setRefusal.startsWith("UnstructuredDataSource (mode embedding) requires hoodie.datasource.write.recordkey.field="),
        setRefusal);

    TypedProperties keyUnset = TypedProperties.copy(keySet);
    keyUnset.remove("hoodie.datasource.write.recordkey.field");
    String unsetRefusal = modeSwitchRefusal(embeddingCfg, keyUnset);
    assertTrue(unsetRefusal.startsWith("UnstructuredDataSource (mode embedding) requires the table's hoodie.table.recordkey.fields="),
        unsetRefusal);
    assertEquals(1, commits(table()).size(), "nothing was written to the blob table");

    String embeddingTable = tempDir.resolve("embedding-table").toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("blob_table")
        .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS))
        .setPartitionFields(BlobTableSchema.SOURCE_FILE_EXTENSION)
        .setKeyGeneratorClassProp("org.apache.hudi.keygen.ComplexKeyGenerator")
        .initTable(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()), embeddingTable);
    TypedProperties blob = harness.jobProperties(THRESHOLD);
    blob.remove("hoodie.datasource.write.recordkey.field");
    String blobRefusal = modeSwitchRefusal(BlobJobHarness.streamerConfig(embeddingTable), blob);
    assertTrue(blobRefusal.startsWith("UnstructuredDataSource (mode blob) requires the table's hoodie.table.recordkey.fields="),
        blobRefusal);
    assertTrue(commits(embeddingTable).isEmpty());
  }

  private static String modeSwitchRefusal(HoodieStreamer.Config cfg, TypedProperties props) {
    Exception failure = assertThrows(Exception.class, () -> syncNewStreamer(cfg, props));
    return EmbeddingStreamers.causeOf(failure, IllegalArgumentException.class)
        .orElseThrow(() -> new AssertionError(EmbeddingStreamers.causes(failure))).getMessage();
  }

  private static void syncNewStreamer(HoodieStreamer.Config cfg, TypedProperties props) throws Exception {
    new HoodieStreamer(cfg, jsc, Option.of(props)).sync();
  }

  /** A preceding user transformer can mark one row invalid without altering source IO policy. */
  public static class NullOneBlobTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext context, SparkSession session, Dataset<Row> rows, TypedProperties props) {
      return rows.withColumn(BlobTableSchema.BLOB, functions.when(functions.col(BlobTableSchema.SOURCE_FILE_NAME).equalTo("bad.pdf"),
          functions.lit(null).cast(rows.schema().apply(BlobTableSchema.BLOB).dataType())).otherwise(functions.col(BlobTableSchema.BLOB)));
    }
  }

  /** The job's own transformers run only after the quarantine transformer: one placed before it is refused at start. */
  @Test
  void transformerBeforeTheQuarantineTransformerIsRefusedAtStart() throws Exception {
    BlobJobHarness harness = harness();
    harness.putAndAnnounce(sparkSession, Collections.singletonMap("good.pdf", 128));
    TypedProperties props = BlobJobHarness.withErrorTable(harness.jobProperties(THRESHOLD), tempDir.resolve("errors").toString());
    HoodieStreamer.Config cfg = BlobJobHarness.streamerConfig(table());
    cfg.transformerClassNames = Arrays.asList(NullOneBlobTransformer.class.getName(), BlobQuarantineTransformer.class.getName());
    Exception failure = assertThrows(Exception.class, () -> syncNewStreamer(cfg, props));
    String causes = EmbeddingStreamers.causes(failure);
    assertTrue(causes.contains("runs the job's own transformers only after " + BlobQuarantineTransformer.class.getName()), causes);
    assertTrue(commits(table()).isEmpty(), "nothing was written");
  }

  /** Fails any open of {@code large.pdf}: the round must never read the bytes of an object above the inline threshold. */
  public static class RefuseLargeOpenFileSystem extends BucketFolderFileSystem {
    @Override
    public org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path path, int bufferSize) throws java.io.IOException {
      if (path.getName().equals("large.pdf")) {
        throw new java.io.IOException("injected: the round opened an object above the inline threshold");
      }
      return super.open(path, bufferSize);
    }
  }

  /**
   * A file above the inline threshold stays where it is: the round reads only its status, never its bytes, and commits an
   * unmanaged reference to it beside the inline row; nothing is quarantined and nothing is written under .hoodie/.blob.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void largeObjectIsReferencedInPlaceWithoutBeingRead(HoodieTableType tableType) throws Exception {
    BlobJobHarness harness = harness();
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("small.pdf", 32);
    sizes.put("large.pdf", 256);
    Map<String, byte[]> expected = new TreeMap<>(harness.putAndAnnounce(sparkSession, sizes));
    String errors = tempDir.resolve("in-place-errors").toString();
    TypedProperties props = BlobJobHarness.withErrorTable(harness.jobProperties(THRESHOLD), errors);
    jsc.hadoopConfiguration().set("fs.bucketfs.impl", RefuseLargeOpenFileSystem.class.getName());
    try {
      runStreamer(props, table(), Long.MAX_VALUE, tableType);
    } finally {
      harness.install(jsc.hadoopConfiguration());
    }
    assertEquals(1, commits(table()).size());
    assertEquals(Collections.emptyList(), BlobJobHarness.quarantinedRecordKeys(errors));
    assertContent(table(), expected);
    assertFalse(blobFolderExists(table()), "nothing is copied under .hoodie/.blob");
  }

  /** A1: an inline object deleted between its event and the round is quarantined as gone since announced; the round commits. */
  @Test
  void goneBeforeTheStatusCallIsQuarantinedAndTheRoundCommits() throws Exception {
    BlobJobHarness harness = harness();
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("good.pdf", 32);
    sizes.put("zz-gone.pdf", 32);
    Map<String, byte[]> expected = new TreeMap<>(harness.putAndAnnounce(sparkSession, sizes));
    expected.remove("zz-gone.pdf");
    Files.delete(harness.file("zz-gone.pdf"));
    String errors = tempDir.resolve("gone-errors").toString();
    runStreamer(BlobJobHarness.withErrorTable(harness.jobProperties(THRESHOLD), errors), table(), Long.MAX_VALUE);
    assertContent(table(), expected);
    assertEquals(Collections.singletonList(BlobJobHarness.versionKey(BlobJobHarness.sourcePath("zz-gone.pdf"), 0L, 32L)),
        BlobJobHarness.quarantinedRecordKeys(errors), "nothing was observed: no modification time and the announced size");
    assertTrue(BlobJobHarness.quarantinedErrors(errors).values().iterator().next().contains("gone since announced"),
        BlobJobHarness.quarantinedErrors(errors).toString());
    assertEquals(1, commits(table()).size());
    // the stock checkpoint names the batch's greatest key: the gone object itself, which sorts after good.pdf
    assertEquals(commits(harness.eventsPath()).get(0).requestedTime() + "#zz-gone.pdf", checkpoint(table()), "the checkpoint advances past the gone object");
  }

  /**
   * Objects gone before the round, inline-size and out-of-line-size, are quarantined; the round commits only the objects it read,
   * and no committed row has a null blob.
   */
  @Test
  void goneObjectsOfEitherSizeAreQuarantinedAndNoCommittedRowHasANullBlob() throws Exception {
    BlobJobHarness harness = harness();
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("good-small.pdf", 32);
    sizes.put("good-large.pdf", (int) THRESHOLD * 2);
    sizes.put("gone-small.pdf", 32);
    sizes.put("gone-large.pdf", (int) THRESHOLD * 2);
    Map<String, byte[]> expected = new TreeMap<>(harness.putAndAnnounce(sparkSession, sizes));
    expected.remove("gone-small.pdf");
    expected.remove("gone-large.pdf");
    Files.delete(harness.file("gone-small.pdf"));
    Files.delete(harness.file("gone-large.pdf"));
    String errors = tempDir.resolve("gone-either-errors").toString();
    runStreamer(BlobJobHarness.withErrorTable(harness.jobProperties(THRESHOLD), errors), table(), Long.MAX_VALUE);
    assertContent(table(), expected);
    assertEquals(Stream.of(BlobJobHarness.versionKey(BlobJobHarness.sourcePath("gone-large.pdf"), 0L, THRESHOLD * 2),
        BlobJobHarness.versionKey(BlobJobHarness.sourcePath("gone-small.pdf"), 0L, 32L)).sorted().collect(Collectors.toList()),
        BlobJobHarness.quarantinedRecordKeys(errors).stream().sorted().collect(Collectors.toList()),
        "nothing was observed: no modification time and the announced size");
    assertTrue(BlobJobHarness.quarantinedErrors(errors).values().stream().allMatch(error -> error.contains("gone since announced")),
        BlobJobHarness.quarantinedErrors(errors).toString());
    assertEquals(1, commits(table()).size());
    assertFalse(blobFolderExists(table()), "nothing is copied under .hoodie/.blob");
  }

  /** A2: an inline object deleted after the status call and before the read is quarantined under the version the status observed. */
  @Test
  void goneBetweenTheStatusCallAndTheInlineReadIsQuarantined() throws Exception {
    BlobJobHarness harness = harness();
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("good.pdf", 32);
    sizes.put("zz-gone.pdf", 32);
    Map<String, byte[]> expected = new TreeMap<>(harness.putAndAnnounce(sparkSession, sizes));
    expected.remove("zz-gone.pdf");
    String goneKey = harness.versionKey("zz-gone.pdf");
    Path gone = harness.file("zz-gone.pdf");
    SourceOpenHooks.beforeOpen(gone.toAbsolutePath().toString(), () -> Files.deleteIfExists(gone));
    String errors = tempDir.resolve("gone-errors").toString();
    try {
      runStreamer(BlobJobHarness.withErrorTable(harness.jobProperties(THRESHOLD), errors), table(), Long.MAX_VALUE);
    } finally {
      SourceOpenHooks.clear();
    }
    assertContent(table(), expected);
    assertEquals(Collections.singletonList(goneKey), BlobJobHarness.quarantinedRecordKeys(errors), "the version the status call observed");
    assertTrue(BlobJobHarness.quarantinedErrors(errors).get(goneKey).contains("gone since announced"), BlobJobHarness.quarantinedErrors(errors).toString());
    assertEquals(1, commits(table()).size());
    assertEquals(commits(harness.eventsPath()).get(0).requestedTime() + "#zz-gone.pdf", checkpoint(table()), "the checkpoint advances past the gone object");
  }

  /** A denial wrapped as missing holds the last checkpoint; fixing access retries that same event instead of skipping it. */
  @ParameterizedTest
  @CsvSource({"status,false", "status,true", "open,false", "open,true"})
  public void nestedSourceDenialHoldsCheckpointUntilItCanBeRead(String stage, boolean errorTable) throws Exception {
    BlobJobHarness harness = harness();
    String errors = tempDir.resolve("denial-errors").toString();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    if (errorTable) {
      BlobJobHarness.withErrorTable(props, errors);
    }
    Map<String, byte[]> before = new TreeMap<>(harness.putAndAnnounce(sparkSession, Collections.singletonMap("seed.pdf", 32)));
    runStreamer(props, table(), Long.MAX_VALUE);
    String previousCheckpoint = checkpoint(table());
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("good.pdf", 32);
    sizes.put(NestedDenialFileSystem.DENIED_FILE, 32);
    Map<String, byte[]> expected = new TreeMap<>(before);
    expected.putAll(harness.putAndAnnounce(sparkSession, sizes));
    String nextEvent = commits(harness.eventsPath()).get(1).requestedTime();
    NestedDenialFileSystem.install(jsc.hadoopConfiguration(), harness.objectsRoot(), stage);
    try {
      Exception failure = assertThrows(Exception.class, () -> runStreamer(props, table(), Long.MAX_VALUE));
      assertTrue(EmbeddingStreamers.causeOf(failure, AccessDeniedException.class).isPresent(), EmbeddingStreamers.causes(failure));
    } finally {
      harness.install(jsc.hadoopConfiguration());
    }
    assertEquals(previousCheckpoint, checkpoint(table()), "an access denial cannot consume the event");
    assertEquals(1, commits(table()).size());
    assertContent(table(), before);
    assertEquals(Collections.emptyList(), BlobJobHarness.quarantinedRecordKeys(errors), "a denial is never a content quarantine");

    runStreamer(props, table(), Long.MAX_VALUE);

    assertEquals(2, commits(table()).size());
    assertEquals(nextEvent + "#" + NestedDenialFileSystem.DENIED_FILE, checkpoint(table()));
    assertContent(table(), expected);
    assertEquals(Collections.emptyList(), BlobJobHarness.quarantinedRecordKeys(errors));
  }

  /** With the error table off nothing can record the gone object, so the round fails as every content fault does. */
  @Test
  void goneObjectWithoutAnErrorTableFailsTheRoundAndHoldsTheCheckpoint() throws Exception {
    BlobJobHarness harness = harness();
    TypedProperties props = harness.jobProperties(THRESHOLD);
    Map<String, byte[]> expected = new TreeMap<>(harness.putAndAnnounce(sparkSession, Collections.singletonMap("seed.pdf", 32)));
    runStreamer(props, table(), Long.MAX_VALUE);
    String previousCheckpoint = checkpoint(table());
    Map<String, Integer> sizes = new TreeMap<>();
    sizes.put("good.pdf", 32);
    sizes.put("zz-gone.pdf", 32);
    harness.putAndAnnounce(sparkSession, sizes);
    Files.delete(harness.file("zz-gone.pdf"));
    Exception failure = assertThrows(Exception.class, () -> runStreamer(props, table(), Long.MAX_VALUE));
    StringBuilder causes = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      causes.append(cause.getMessage()).append("\n");
    }
    assertTrue(causes.toString().contains("without an error table"), causes.toString());
    assertTrue(causes.toString().contains("gone since announced"), causes.toString());
    assertEquals(previousCheckpoint, checkpoint(table()));
    assertEquals(1, commits(table()).size());
    assertContent(table(), expected);
  }
}
