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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.sources.blob.BlobParquetConfigInjector;
import org.apache.hudi.utilities.sources.blob.BlobTableConfig;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.sources.blob.BucketFolderFileSystem;
import org.apache.hudi.utilities.sources.blob.S3EventsTableFixture;
import org.apache.hudi.utilities.sources.blob.S3EventsTableFixture.Event;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredCloud;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredMode;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceConfig;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceContract;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.utilities.sources.blob.S3EventsTableFixture.created;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * {@link UnstructuredDataSource}: its job and cloud keys, the settings and transformers each job runs with, and one blob round on S3,
 * step by step,
 * over an S3 events Hudi table on the local disk whose objects are read through {@link BucketFolderFileSystem}
 * ({@code hoodie.streamer.source.s3incr.fs.prefix=bucketfs}): every phase is {@link S3EventsHoodieIncrSource}'s,
 * and only what becomes of the selected objects is the blob table's.
 */
@Tag("functional")
public class TestUnstructuredDataSource extends UtilitiesTestBase {

  private static final String BUCKET = "landing";
  private static final Instant T0 = Instant.parse("2026-01-02T10:00:00.000Z");
  private static final int INLINE_MAX = 64;

  @TempDir
  Path tempDir;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @BeforeEach
  public void mapBuckets() {
    BucketFolderFileSystem.install(jsc.hadoopConfiguration(), objects());
  }

  private Path objects() {
    return tempDir.resolve("objects");
  }

  private String eventsPath() {
    return tempDir.resolve("events").toUri().toString();
  }

  /** Writes the object's bytes (each byte = the size mod 251) and returns its ObjectCreated event. */
  private Event put(String bucket, String key, int size) {
    write(bucket, key, size);
    return created(bucket, key, size, T0);
  }

  private void write(String bucket, String key, int size) {
    Path file = BucketFolderFileSystem.file(objects(), bucket, key);
    try {
      Files.createDirectories(file.getParent());
      Files.write(file, bytes(size));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static byte[] bytes(int size) {
    byte[] bytes = new byte[size];
    Arrays.fill(bytes, (byte) (size % 251));
    return bytes;
  }

  private void commit(Event... events) {
    S3EventsTableFixture.appendEvents(sparkSession, eventsPath(), Arrays.asList(events));
  }

  /** Requested times of the events table's completed commits, oldest first. */
  private List<String> commits() {
    return HoodieTableMetaClient.builder().setBasePath(eventsPath())
        .setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).build()
        .getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants().stream()
        .map(HoodieInstant::requestedTime).collect(Collectors.toList());
  }

  /** The blob job's source properties. */
  private TypedProperties props() {
    TypedProperties props = new TypedProperties();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), "blob");
    props.setProperty(UnstructuredSourceConfig.CLOUD.key(), "s3");
    props.setProperty(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key(), BlobParquetConfigInjector.class.getName());
    props.setProperty(HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key(), eventsPath());
    props.setProperty(S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX.key(), BucketFolderFileSystem.SCHEME);
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), String.valueOf(INLINE_MAX));
    props.setProperty("hoodie.datasource.write.recordkey.field", BlobTableSchema.SOURCE_PATH_SHA + "," + BlobTableSchema.SOURCE_MODIFICATION_TIME + "," + BlobTableSchema.SOURCE_FILE_SIZE);
    props.setProperty("hoodie.datasource.write.partitionpath.field", BlobTableSchema.SOURCE_FILE_EXTENSION);
    props.setProperty("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator");
    return props;
  }

  private static UnstructuredDataSource source(TypedProperties props) {
    return new UnstructuredDataSource(props, jsc, sparkSession, mock(HoodieIngestionMetrics.class),
        new DefaultStreamContext(null, Option.empty()));
  }

  /** One round's rows by source key ({@code <bucket>/<key>}), and its checkpoint. */
  private static final class Round {
    final Map<String, Row> rows = new TreeMap<>();
    final Checkpoint checkpoint;

    Round(Pair<Option<Dataset<Row>>, Checkpoint> batch) {
      this.checkpoint = batch.getRight();
      batch.getLeft().ifPresent(rowSet -> rowSet.collectAsList().forEach(row -> {
        String sourcePath = row.getAs(BlobTableSchema.SOURCE_FILE_PATH);
        String key = sourcePath.substring((BucketFolderFileSystem.SCHEME + "://").length());
        rows.put(key, row);
      }));
    }

    String checkpointKey() {
      return checkpoint.getCheckpointKey();
    }
  }

  private static Round round(UnstructuredDataSource source, Option<Checkpoint> last, long sourceLimit) {
    return new Round(source.fetchNextBatch(last, sourceLimit));
  }

  private static Round round(UnstructuredDataSource source, Round previous, long sourceLimit) {
    return round(source, Option.of(previous.checkpoint), sourceLimit);
  }

  private static List<String> keys(String... keys) {
    return Stream.of(keys).map(key -> BUCKET + "/" + key).sorted().collect(Collectors.toList());
  }

  private static List<String> keys(Round round) {
    return round.rows.keySet().stream().sorted().collect(Collectors.toList());
  }

  @Test
  public void numInstantsCommitsFromTheCheckpointsCommitInRequestedTimeOrder() {
    commit(put(BUCKET, "a.pdf", 10));
    commit(put(BUCKET, "b.pdf", 20));
    commit(put(BUCKET, "c.pdf", 30));
    commit(put(BUCKET, "d.pdf", 40));
    List<String> commits = commits();
    TypedProperties props = props();
    props.setProperty(HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH.key(), "3");
    UnstructuredDataSource source = source(props);
    // a keyed checkpoint reads num_instants commits from its own one: commits 1-3, of which what follows a.pdf
    Round afterFirst = round(source, Option.of(new StreamerCheckpointV1(commits.get(0) + "#a.pdf")), Long.MAX_VALUE);
    assertEquals(keys("b.pdf", "c.pdf"), keys(afterFirst));
    assertEquals(commits.get(2) + "#c.pdf", afterFirst.checkpointKey());
    Round next = round(source, afterFirst, Long.MAX_VALUE);
    assertEquals(keys("d.pdf"), keys(next));
    assertEquals(commits.get(3) + "#d.pdf", next.checkpointKey());
    // a keyless checkpoint names a commit not read past: num_instants commits after it, and it, are read (as in S3EventsHoodieIncrSource)
    Round keyless = round(source(props), Option.of(new StreamerCheckpointV1(commits.get(0))), Long.MAX_VALUE);
    assertEquals(keys("a.pdf", "b.pdf", "c.pdf", "d.pdf"), keys(keyless));
  }

  /**
   * S3EventsHoodieIncrSource's behaviour: with {@code num_instants=1} a keyed checkpoint at the last object of its commit
   * never advances, since a keyed round reads from that commit on ({@code findInstantsAfterOrEquals(commit, 1)}) and
   * nothing in it follows the key.
   */
  @Test
  public void numInstantsOfOneStallsAtTheLastKeyOfACommit() {
    commit(put(BUCKET, "a.pdf", 10));
    commit(put(BUCKET, "b.pdf", 20));
    List<String> commits = commits();
    TypedProperties props = props();
    props.setProperty(HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH.key(), "1");
    UnstructuredDataSource source = source(props);
    Round stalled = round(source, Option.of(new StreamerCheckpointV1(commits.get(0) + "#a.pdf")), Long.MAX_VALUE);
    assertTrue(stalled.rows.isEmpty(), "b.pdf of the next commit is not read: " + stalled.rows.keySet());
    assertEquals(commits.get(0) + "#a.pdf", stalled.checkpointKey());
  }

  @Test
  public void firstRoundWithoutACheckpointUnsetOrUptoLatestSnapshotsTheWholeEventsTable() {
    commit(put(BUCKET, "a.pdf", 10));
    commit(put(BUCKET, "b.pdf", 20));
    commit(put(BUCKET, "c.pdf", 30));
    for (String strategy : Arrays.asList(null, "READ_UPTO_LATEST_COMMIT")) {
      TypedProperties props = props();
      props.setProperty(HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH.key(), "1");
      if (strategy != null) {
        props.setProperty(HoodieIncrSourceConfig.MISSING_CHECKPOINT_STRATEGY.key(), strategy);
      }
      Round first = round(source(props), Option.empty(), Long.MAX_VALUE);
      assertEquals(keys("a.pdf", "b.pdf", "c.pdf"), keys(first), "strategy " + strategy);
      assertEquals(commits().get(2) + "#c.pdf", first.checkpointKey());
    }
  }

  @Test
  public void firstRoundWithoutACheckpointReadLatestReadsTheFirstCommitsFromTheStart() {
    commit(put(BUCKET, "a.pdf", 10));
    commit(put(BUCKET, "b.pdf", 20));
    commit(put(BUCKET, "c.pdf", 30));
    TypedProperties strategy = props();
    strategy.setProperty(HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH.key(), "1");
    strategy.setProperty(HoodieIncrSourceConfig.MISSING_CHECKPOINT_STRATEGY.key(), "READ_LATEST");
    TypedProperties flag = props();
    flag.setProperty(HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH.key(), "1");
    flag.setProperty(HoodieIncrSourceConfig.READ_LATEST_INSTANT_ON_MISSING_CKPT.key(), "true");
    for (TypedProperties props : Arrays.asList(strategy, flag)) {
      Round first = round(source(props), Option.empty(), Long.MAX_VALUE);
      // stock behaviour: the begin is 00000000000000, so READ_LATEST reads the first commit, not the latest one
      assertEquals(keys("a.pdf"), keys(first));
      assertEquals(commits().get(0) + "#a.pdf", first.checkpointKey());
    }
  }

  @Test
  public void anEarlierEventsCommitStillInFlightBlocksTheRoundUnderTheDefaultPolicy() throws IOException {
    commit(put(BUCKET, "a.pdf", 10));
    commit(put(BUCKET, "b.pdf", 20));
    List<String> commits = commits();
    String first = commits.get(0);
    // turn the first commit back into an in-flight one: drop its completed instant file from the timeline
    Path timeline = tempDir.resolve("events").resolve(".hoodie").resolve("timeline");
    List<Path> completed;
    try (Stream<Path> files = Files.list(timeline)) {
      completed = files.filter(f -> f.getFileName().toString().startsWith(first + "_")
          && f.getFileName().toString().endsWith(".commit")).collect(Collectors.toList());
    }
    String listing;
    try (Stream<Path> files = Files.list(timeline)) {
      listing = files.map(f -> f.getFileName().toString()).sorted().collect(Collectors.joining(", "));
    }
    assertEquals(1, completed.size(), "the completed instant file of " + first + " in " + listing);
    byte[] completion = Files.readAllBytes(completed.get(0));
    Files.delete(completed.get(0));
    assertEquals(Collections.singletonList(commits.get(1)), commits(), "the first commit is in flight again");
    // the default policy is S3EventsHoodieIncrSource's BLOCK: nothing at or after the hole is read, and the checkpoint stays before it
    Round blocked = round(source(props()), Option.empty(), Long.MAX_VALUE);
    assertTrue(blocked.rows.isEmpty(), "BLOCK stops before the hole: " + blocked.rows.keySet());
    assertEquals("00000000000000", blocked.checkpointKey(), "the checkpoint does not pass the hole");
    TypedProperties fail = props();
    fail.setProperty(HoodieCommonConfig.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key(), "FAIL");
    Exception failure = assertThrows(Exception.class, () -> round(source(fail), Option.empty(), Long.MAX_VALUE));
    assertTrue(chain(failure).contains("hollow commit"), chain(failure));

    // the earlier-requested commit completes after the later one: the next round reads both, in requested-time order
    Files.write(completed.get(0), completion);
    Round resumed = round(source(props()), Option.of(blocked.checkpoint), Long.MAX_VALUE);
    assertEquals(keys("a.pdf", "b.pdf"), keys(resumed));
    assertEquals(commits.get(1) + "#b.pdf", resumed.checkpointKey());
  }

  @Test
  public void sameKeyInTwoBucketsIsTwoObjectsUnlessASplitFallsBetweenThem() {
    commit(put("bucket-a", "same.pdf", 10), put("bucket-b", "same.pdf", 10), put(BUCKET, "z.pdf", 10));
    Round whole = round(source(props()), Option.empty(), Long.MAX_VALUE);
    assertEquals(Arrays.asList("bucket-a/same.pdf", "bucket-b/same.pdf", BUCKET + "/z.pdf"), keys(whole));

    // the checkpoint key s3.object.key carries no bucket, so a split between the two skips one
    TypedProperties props = props();
    props.setProperty(CloudSourceConfig.SOURCE_MAX_FILES_PER_SYNC.key(), "1");
    UnstructuredDataSource source = source(props);
    Round first = round(source, Option.empty(), Long.MAX_VALUE);
    assertEquals(1, first.rows.size());
    assertTrue(keys(first).get(0).endsWith("/same.pdf"), keys(first).toString());
    assertEquals(commits().get(0) + "#same.pdf", first.checkpointKey());
    Round second = round(source, first, Long.MAX_VALUE);
    assertEquals(keys("z.pdf"), keys(second), "the other bucket's same.pdf is skipped");
  }

  @Test
  public void zeroByteObjectsAndThePathFiltersAreApplied() {
    commit(put(BUCKET, "docs/a.pdf", 10), put(BUCKET, "docs/empty.pdf", 0), put(BUCKET, "img/b.png", 20),
        put(BUCKET, "docs/tmp/c.pdf", 30));
    TypedProperties props = props();
    props.setProperty(CloudSourceConfig.SELECT_RELATIVE_PATH_PREFIX.key(), "docs/");
    props.setProperty(CloudSourceConfig.IGNORE_RELATIVE_PATH_SUBSTR.key(), "tmp");
    assertEquals(keys("docs/a.pdf"), keys(round(source(props), Option.empty(), Long.MAX_VALUE)));
    assertEquals(keys("docs/a.pdf", "docs/tmp/c.pdf", "img/b.png"), keys(round(source(props()), Option.empty(), Long.MAX_VALUE)),
        "a zero-byte object is never ingested");
  }

  @Test
  public void theRegexIgnorePrefixAndLegacyKeyPrefixPathKeysAreApplied() {
    commit(put(BUCKET, "in/a.pdf", 10), put(BUCKET, "in/skip/b.pdf", 20), put(BUCKET, "in/c.png", 30), put(BUCKET, "out/d.pdf", 40));
    TypedProperties regex = props();
    regex.setProperty(CloudSourceConfig.SELECT_RELATIVE_PATH_PREFIX.key(), "in/");
    regex.setProperty(CloudSourceConfig.SELECT_RELATIVE_PATH_REGEX.key(), ".*\\.pdf");
    assertEquals(keys("in/a.pdf", "in/skip/b.pdf"), keys(round(source(regex), Option.empty(), Long.MAX_VALUE)));
    TypedProperties ignore = props();
    ignore.setProperty(CloudSourceConfig.IGNORE_RELATIVE_PATH_PREFIX.key(), "in/skip");
    assertEquals(keys("in/a.pdf", "in/c.png", "out/d.pdf"), keys(round(source(ignore), Option.empty(), Long.MAX_VALUE)));
    TypedProperties legacy = props();
    legacy.setProperty(S3EventsHoodieIncrSourceConfig.S3_KEY_PREFIX.key(), "out/");
    assertEquals(keys("out/d.pdf"), keys(round(source(legacy), Option.empty(), Long.MAX_VALUE)));
  }

  @Test
  public void extensionsAreMatchedCaseSensitivelyWithTheirDot() {
    commit(put(BUCKET, "a.pdf", 10), put(BUCKET, "IMG.JPG", 20), put(BUCKET, "c.jpg", 30), put(BUCKET, "report_pdf", 40),
        put(BUCKET, "notes.txt", 50));
    assertEquals(keys("a.pdf", "c.jpg"), keys(round(source(props()), Option.empty(), Long.MAX_VALUE)));
    TypedProperties props = props();
    props.setProperty(BlobTableConfig.FILE_EXTENSIONS.key(), "jpg");
    assertEquals(keys("c.jpg"), keys(round(source(props), Option.empty(), Long.MAX_VALUE)));
  }

  /** S3EventsHoodieIncrSource's behaviour: a key holding a bare {@code %} cannot be decoded again, so the round fails. */
  @Test
  public void keyHoldingABarePercentFailsTheRound() {
    commit(put(BUCKET, "docs/q3 report 100%.pdf", 10));
    Exception failure = assertThrows(Exception.class, () -> round(source(props()), Option.empty(), Long.MAX_VALUE));
    assertTrue(chain(failure).contains("Failed to generate path to cloud file"), chain(failure));
  }

  private static String chain(Throwable failure) {
    StringBuilder chain = new StringBuilder();
    for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause()) {
      chain.append(t).append(" <- ");
    }
    return chain.toString();
  }

  @Test
  public void anFsPrefixWithoutAFileSystemFailsTheRound() {
    commit(put(BUCKET, "a.pdf", 10));
    TypedProperties props = props();
    props.setProperty(S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX.key(), "nosuchfs");
    assertThrows(Exception.class, () -> source(props).fetchNextBatch(Option.empty(), Long.MAX_VALUE).getLeft().get().collectAsList());
  }

  @Test
  public void defaultExtensionsReadRawBytesAndExcludeStructuredEvents() {
    String[] names = Arrays.stream(BlobTableConfig.FILE_EXTENSIONS.defaultValue().split(","))
        .map(extension -> "raw/file." + extension).toArray(String[]::new);
    for (String name : names) {
      commit(put(BUCKET, name, 10));
    }
    commit(put(BUCKET, "raw/a.parquet", 10), put(BUCKET, "raw/b.json", 10));
    Round result = round(source(props()), Option.empty(), Long.MAX_VALUE);
    assertEquals(keys(names), keys(result));
    for (Row row : result.rows.values()) {
      String path = row.getAs(BlobTableSchema.SOURCE_FILE_PATH);
      assertEquals(org.apache.hudi.utilities.sources.blob.BlobFileNames.sourcePathSha(path), row.getAs(BlobTableSchema.SOURCE_PATH_SHA));
      Row blob = row.getAs(BlobTableSchema.BLOB);
      org.junit.jupiter.api.Assertions.assertArrayEquals(bytes(10), (byte[]) blob.get(1));
    }
  }

  @Test
  public void fileCapKeepsTheStockDefaultAndExplicitBudgetSplits() {
    TypedProperties defaults = props();
    source(defaults);
    assertFalse(defaults.containsKey(CloudSourceConfig.SOURCE_MAX_FILES_PER_SYNC.key()));
    assertEquals(10000L, (long) CloudSourceConfig.SOURCE_MAX_FILES_PER_SYNC.defaultValue());
    commit(put(BUCKET, "a.pdf", 10), put(BUCKET, "b.pdf", 20), put(BUCKET, "c.pdf", 30));
    TypedProperties limited = props();
    limited.setProperty(CloudSourceConfig.SOURCE_MAX_FILES_PER_SYNC.key(), "1");
    UnstructuredDataSource source = source(limited);
    Round first = round(source, Option.empty(), 1);
    assertEquals(keys("a.pdf"), keys(first));
    assertEquals(commits().get(0) + "#a.pdf", first.checkpointKey());
    Round second = round(source(props()), first, 20);
    assertEquals(keys("b.pdf"), keys(second));
    assertEquals(commits().get(0) + "#b.pdf", second.checkpointKey());
  }

  @Test
  public void repeatedEventsOfOnePathBuildOneRowWhateverTheirSizes() {
    Event same = put(BUCKET, "same.pdf", 10);
    commit(same.withSequencer("01"), same.withSequencer("02"),
        put(BUCKET, "changed.pdf", 5).withSequencer("01"),
        put(BUCKET, "changed.pdf", 10).withSequencer("02"));
    Pair<Option<Dataset<Row>>, Checkpoint> result = source(props()).fetchNextBatch(Option.empty(), Long.MAX_VALUE);
    List<Row> rows = result.getLeft().get().collectAsList();
    assertEquals(Arrays.asList("changed.pdf", "same.pdf"), rows.stream()
        .map(row -> (String) row.getAs(BlobTableSchema.SOURCE_FILE_NAME)).sorted().collect(Collectors.toList()));
    assertEquals(Arrays.asList(10L, 10L), rows.stream().map(row -> (Long) row.getAs(BlobTableSchema.SOURCE_FILE_SIZE))
        .sorted().collect(Collectors.toList()), "each row carries the object's current size");
    assertEquals(commits().get(0) + "#same.pdf", result.getRight().getCheckpointKey());
  }

  @Test
  public void existenceFilteringIsRefusedAndMissingObjectsStillReachTheSource() throws Exception {
    commit(put(BUCKET, "exists.pdf", 10), created(BUCKET, "missing.pdf", 10, T0));
    TypedProperties checked = props();
    checked.setProperty(CloudSourceConfig.ENABLE_EXISTS_CHECK.key(), "true");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> source(checked));
    assertTrue(refused.getMessage().contains(CloudSourceConfig.ENABLE_EXISTS_CHECK.key()), refused.getMessage());
    TypedProperties unchecked = props();
    unchecked.setProperty(CloudSourceConfig.ENABLE_EXISTS_CHECK.key(), "false");
    Round attempted = round(source(unchecked), Option.empty(), Long.MAX_VALUE);
    assertEquals(keys("exists.pdf", "missing.pdf"), keys(attempted));
    assertEquals(commits().get(0) + "#missing.pdf", attempted.checkpointKey());
    Row existing = attempted.rows.get(BUCKET + "/exists.pdf");
    Row missing = attempted.rows.get(BUCKET + "/missing.pdf");
    assertEquals("bucketfs://landing/exists.pdf", existing.getAs(BlobTableSchema.SOURCE_FILE_PATH));
    assertEquals("b22f795efdfc08eb787b39c97adc3f70", existing.getAs(BlobTableSchema.SOURCE_PATH_SHA));
    assertEquals("bucketfs://landing/missing.pdf", missing.getAs(BlobTableSchema.SOURCE_FILE_PATH));
    assertEquals("7f66659272b5cb966e094a7d819825f5", missing.getAs(BlobTableSchema.SOURCE_PATH_SHA));
    assertEquals(10L, missing.<Long>getAs(BlobTableSchema.SOURCE_FILE_SIZE), "a gone object keeps its announced size");
    assertEquals(0L, missing.<Long>getAs(BlobTableSchema.SOURCE_MODIFICATION_TIME), "a gone object has no observed modification time");
    org.junit.jupiter.api.Assertions.assertNull(missing.getAs(BlobTableSchema.BLOB));
    assertEquals(java.nio.file.Files.getLastModifiedTime(BucketFolderFileSystem.file(objects(), BUCKET, "exists.pdf")).toMillis(),
        existing.<Long>getAs(BlobTableSchema.SOURCE_MODIFICATION_TIME));
    Row blob = existing.getAs(BlobTableSchema.BLOB);
    org.junit.jupiter.api.Assertions.assertArrayEquals(bytes(10), (byte[]) blob.get(1));
  }

  @Test
  public void configuredPathPartitionFieldsPreserveMetadata() {
    commit(put(BUCKET, "country=US/source_file_name=ignored/a.pdf", 10));
    TypedProperties props = props();
    props.setProperty(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key(), "country,missing,source_file_name");
    Round result = round(source(props), Option.empty(), Long.MAX_VALUE);
    Row row = result.rows.values().iterator().next();
    assertEquals("US", row.getAs("country"));
    org.junit.jupiter.api.Assertions.assertNull(row.getAs("missing"));
    assertEquals("a.pdf", row.getAs(BlobTableSchema.SOURCE_FILE_NAME));
    assertEquals(org.apache.spark.sql.types.DataTypes.StringType, row.schema().apply("country").dataType());
  }

  @Test
  public void caseVariantPathPartitionsPreserveFixedMetadata() {
    assertFalse(Boolean.parseBoolean(sparkSession.conf().get("spark.sql.caseSensitive", "false")));
    String key = "SOURCE_PATH_SHA=directory-key/SOURCE_FILE_NAME=directory-name/a.pdf";
    commit(put(BUCKET, key, 10));
    TypedProperties props = props();
    props.setProperty(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key(), "SOURCE_PATH_SHA,SOURCE_FILE_NAME");
    Row row = round(source(props), Option.empty(), Long.MAX_VALUE).rows.get(BUCKET + "/" + key);
    assertEquals(BlobTableSchema.schema(), row.schema());
    String path = BucketFolderFileSystem.SCHEME + "://" + BUCKET + "/" + key;
    String expectedId = org.apache.hudi.utilities.sources.blob.BlobFileNames.sourcePathSha(path);
    assertEquals(expectedId, row.getAs(BlobTableSchema.SOURCE_PATH_SHA));
    assertEquals("a.pdf", row.getAs(BlobTableSchema.SOURCE_FILE_NAME));
    assertEquals(path, row.getAs(BlobTableSchema.SOURCE_FILE_PATH));
    Row blob = row.getAs(BlobTableSchema.BLOB);
    org.junit.jupiter.api.Assertions.assertArrayEquals(bytes(10), (byte[]) blob.get(1));
  }

  @Test
  public void disjointExtensionConstraintsSelectNothing() {
    commit(put(BUCKET, "a.pdf", 10), put(BUCKET, "b.png", 10));
    TypedProperties props = props();
    props.setProperty(BlobTableConfig.FILE_EXTENSIONS.key(), "pdf");
    props.setProperty(CloudSourceConfig.CLOUD_DATAFILE_EXTENSION.key(), "png");
    assertTrue(round(source(props), Option.empty(), Long.MAX_VALUE).rows.isEmpty());
  }

  private static final String CODEC = HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key();
  private static final String BLOOM = HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED.key();
  private static final String RECORD_KEY = KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key();
  private static final String KEY_GENERATOR = HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key();
  private static final String BLOB_KEY = "source_path_sha,source_modification_time,source_file_size";
  private static final String EMBEDDING_KEY = BLOB_KEY + ",modality,chunk_id";

  /** The embedding job's source properties: the blob job's, under {@code mode=embedding} with {@code prep_only}. */
  private TypedProperties embeddingProps() {
    TypedProperties props = props();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), "embedding");
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    props.setProperty(RECORD_KEY, EMBEDDING_KEY);
    return props;
  }

  /** The source reads its job and cloud; the settings and transformers are its contract's (TestUnstructuredSourceContract). */
  @Test
  public void modeAndCloud() {
    UnstructuredDataSource blob = source(props());
    assertEquals(UnstructuredMode.BLOB, blob.mode());
    assertEquals(UnstructuredCloud.S3, blob.cloud());
    assertEquals(UnstructuredMode.EMBEDDING, source(embeddingProps()).mode());
  }

  /** The source refuses, as it is built, a job property it cannot run with, naming itself and its mode. */
  @Test
  public void constructionRefusalsNameUnstructuredDataSourceAndMode() {
    TypedProperties embeddingModeOnBlob = props();
    embeddingModeOnBlob.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    assertTrue(assertThrows(IllegalArgumentException.class, () -> source(embeddingModeOnBlob)).getMessage()
        .startsWith("UnstructuredDataSource (mode blob) refuses hoodie.unstructured.embedding.mode=prep_only"));
    TypedProperties noEmbeddingMode = embeddingProps();
    noEmbeddingMode.remove(EmbeddingTableConfig.MODE.key());
    assertTrue(assertThrows(IllegalArgumentException.class, () -> source(noEmbeddingMode)).getMessage()
        .startsWith("UnstructuredDataSource (mode embedding) requires hoodie.unstructured.embedding.mode"));
    TypedProperties gcsWithS3Prefix = props();
    gcsWithS3Prefix.setProperty(UnstructuredSourceConfig.CLOUD.key(), "gcs");
    assertTrue(assertThrows(IllegalArgumentException.class, () -> source(gcsWithS3Prefix)).getMessage()
        .startsWith("UnstructuredDataSource (mode blob) refuses hoodie.streamer.source.s3incr.fs.prefix"));
    for (String key : new String[] {UnstructuredSourceConfig.MODE.key(), UnstructuredSourceConfig.CLOUD.key()}) {
      TypedProperties unset = props();
      unset.remove(key);
      assertTrue(assertThrows(IllegalArgumentException.class, () -> source(unset)).getMessage()
          .startsWith("UnstructuredDataSource requires " + key + ", one of: "), key);
    }
  }

  @Test
  public void datafileFormatStillRefused() {
    TypedProperties props = props();
    props.setProperty(CloudSourceConfig.DATAFILE_FORMAT.key(), "parquet");
    assertEquals(CloudSourceConfig.DATAFILE_FORMAT.key() + " must be empty for UnstructuredDataSource (mode blob); the source reads raw "
        + "unstructured bytes", assertThrows(IllegalArgumentException.class, () -> source(props)).getMessage());
  }

  /**
   * The source reads none of the settings its contract declares, neither as it is built nor in a round: the streamer gives it the
   * merged properties, but the source does not depend on them.
   */
  @Test
  public void invariantRequiredKeysNotReadThroughSourceProps() {
    commit(put(BUCKET, "a.pdf", 10), put(BUCKET, "b.pdf", INLINE_MAX + 1));
    for (TypedProperties jobProps : Arrays.asList(props(), embeddingProps())) {
      // the keys the merge adds to a job that sets only its mode and cloud: every key the contract declares
      TypedProperties modeAndCloud = new TypedProperties();
      for (String key : new String[] {UnstructuredSourceConfig.MODE.key(), UnstructuredSourceConfig.CLOUD.key(), EmbeddingTableConfig.MODE.key()}) {
        if (jobProps.containsKey(key)) {
          modeAndCloud.setProperty(key, jobProps.getString(key));
        }
      }
      Set<String> declared = new TreeSet<>(new UnstructuredSourceContract(jobProps).prepareJob(modeAndCloud)
          .stringPropertyNames());
      declared.removeAll(modeAndCloud.stringPropertyNames());
      declared.addAll(Arrays.asList(HoodieWriteConfig.MARKERS_TYPE.key(), HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key()));
      GuardedProperties guarded = new GuardedProperties(declared);
      for (String key : jobProps.stringPropertyNames()) {
        if (!declared.contains(key)) {
          guarded.setProperty(key, jobProps.getString(key));
        }
      }
      guarded.arm();
      UnstructuredDataSource source = source(guarded);
      Round round = round(source, Option.empty(), Long.MAX_VALUE);
      assertEquals(keys("a.pdf", "b.pdf"), keys(round), "mode " + source.mode());
    }
  }

  /** Job properties that fail a read of any of the given keys once armed. */
  private static final class GuardedProperties extends TypedProperties {
    private final Set<String> guarded;
    private boolean armed;

    GuardedProperties(Set<String> guarded) {
      this.guarded = guarded;
    }

    void arm() {
      armed = true;
    }

    private void check(Object key) {
      if (armed && key != null && guarded.contains(key.toString())) {
        throw new AssertionError("the source read " + key + ", a setting it declares");
      }
    }

    @Override
    public String getProperty(String key) {
      check(key);
      return super.getProperty(key);
    }

    @Override
    public String getProperty(String key, String defaultValue) {
      check(key);
      return super.getProperty(key, defaultValue);
    }

    @Override
    public synchronized Object get(Object key) {
      check(key);
      return super.get(key);
    }

    @Override
    public synchronized boolean containsKey(Object key) {
      check(key);
      return super.containsKey(key);
    }

    @Override
    public synchronized boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public synchronized int hashCode() {
      return super.hashCode();
    }
  }
}
