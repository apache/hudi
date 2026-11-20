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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.transaction.ConcurrentOperation;
import org.apache.hudi.client.validator.SparkPreCommitValidator;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.utilities.exception.HoodieSourceTimeoutException;
import org.apache.hudi.utilities.exception.HoodieTransformException;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.sources.TestDataSource;
import org.apache.hudi.utilities.transform.InstantAwareTransformer;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A transformer chain with an {@link InstantAwareTransformer}: once the source returned data StreamSync starts the commit
 * (REQUESTED), moves it to INFLIGHT right before the first {@link InstantAwareTransformer} of the chain, which receives the
 * INFLIGHT instant and writes marked files for it (marker first, then the file, the normal write protocol); the write of the
 * round takes the instant that is already INFLIGHT and overwrites its inflight file with the workload profile; a transformer
 * failure rolls the instant back (and a failed rollback does not hide the failure), a crash leaves an INFLIGHT instant that the
 * standard marker-based rollback removes with its files; and the heartbeat of the instant survives the re-creation of the write
 * client. A chain without one runs as stock: the transformer runs before any instant. There is no option selecting either.
 */
@Tag("functional")
public class TestStreamSyncInstantAwareTransformer extends HoodieDeltaStreamerTestBase {

  /** Property naming the target base path, so a test transformer can look at the table. */
  static final String TARGET_BASE_PATH = "test.instant.aware.target.base.path";
  /** Where the test transformer writes its files: under the table's meta folder, which marker reconciliation leaves alone. */
  static final String PAYLOAD_FOLDER = HoodieTableMetaClient.METAFOLDER_NAME + "/.instant-aware-payloads";
  /** The extension of the files the test transformers write for an instant; not a data file of the table. */
  private static final String PAYLOAD_EXTENSION = ".payload";

  /** The file a {@link PayloadWritingTransformer} wrote for an instant, by table and instant. */
  private static final Map<String, Path> PAYLOADS = new ConcurrentHashMap<>();
  /** The modification time of each file a {@link PayloadWritingTransformer} wrote, read right after it wrote it. */
  private static final Map<Path, Long> PAYLOAD_WRITTEN_AT = new ConcurrentHashMap<>();
  /** One entry per call: the instant passed, or an empty string for a call through the plain {@code apply}. */
  private static final List<String> SEEN_INSTANTS = new CopyOnWriteArrayList<>();
  /** One entry per call: the pending (requested or inflight) instants of the target table at the time of the call. */
  private static final List<List<HoodieInstant>> PENDING_AT_APPLY = new CopyOnWriteArrayList<>();
  private static final List<String> COMMIT_EVENTS = new CopyOnWriteArrayList<>();
  private static final List<String> TRANSFORMER_COMMITS = new CopyOnWriteArrayList<>();
  private static final List<List<String>> COMPLETED_AT_CALLBACK = new CopyOnWriteArrayList<>();
  /** One entry per pre-commit call: the writer schema of the write config the round commits with. */
  private static final List<String> WRITER_SCHEMA_AT_PRE_COMMIT = new CopyOnWriteArrayList<>();
  private static final AtomicLong RECORDS_AT_FAILED_COMMIT = new AtomicLong(-1);

  /** Records the source's existing commit callback so the transformer hook must follow it. */
  public static class CommitRecordingSource extends TestDataSource {
    public CommitRecordingSource(TypedProperties props, JavaSparkContext jsc, SparkSession spark, SchemaProvider schemaProvider) {
      super(props, jsc, spark, schemaProvider);
    }

    @Override
    public void onCommit(String checkpoint) {
      super.onCommit(checkpoint);
      COMMIT_EVENTS.add("source");
    }
  }

  /** When armed, returns the rows of the next batch with the checkpoint it resumed from: data, but no new checkpoint. */
  public static class UnchangedCheckpointSource extends TestDataSource {
    static final AtomicBoolean ARMED = new AtomicBoolean();

    public UnchangedCheckpointSource(TypedProperties props, JavaSparkContext jsc, SparkSession spark, SchemaProvider schemaProvider) {
      super(props, jsc, spark, schemaProvider);
    }

    @Override
    protected InputBatch<JavaRDD<GenericRecord>> readFromCheckpoint(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
      InputBatch<JavaRDD<GenericRecord>> batch = super.readFromCheckpoint(lastCheckpoint, sourceLimit);
      return ARMED.get() ? new InputBatch<>(batch.getBatch(), lastCheckpoint.orElse(null)) : batch;
    }
  }

  /** When armed, returns no rows but a new checkpoint. */
  public static class EmptyBatchNewCheckpointSource extends TestDataSource {
    static final AtomicBoolean ARMED = new AtomicBoolean();

    public EmptyBatchNewCheckpointSource(TypedProperties props, JavaSparkContext jsc, SparkSession spark, SchemaProvider schemaProvider) {
      super(props, jsc, spark, schemaProvider);
    }

    @Override
    protected InputBatch<JavaRDD<GenericRecord>> readFromCheckpoint(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
      InputBatch<JavaRDD<GenericRecord>> batch = super.readFromCheckpoint(lastCheckpoint, sourceLimit);
      return ARMED.get() ? new InputBatch<>(Option.empty(), batch.getCheckpointForNextBatch()) : batch;
    }
  }

  /** Records each call; given an instant, writes a fresh marked file for it. */
  public static class PayloadWritingTransformer implements InstantAwareTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      record(properties, null);
      return rowDataset;
    }

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      record(properties, instantTime);
      String base = properties.getString(TARGET_BASE_PATH);
      PAYLOADS.put(base + "#" + instantTime, writeMarkedFile(jsc, properties, instantTime));
      return rowDataset;
    }

    static void record(TypedProperties properties, String instantTime) {
      SEEN_INSTANTS.add(instantTime == null ? "" : instantTime);
      String basePath = properties.getString(TARGET_BASE_PATH);
      try {
        PENDING_AT_APPLY.add(fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
            ? HoodieTestUtils.createMetaClient(storage, basePath).getActiveTimeline().filterInflightsAndRequested().getInstants()
            : Collections.emptyList());
      } catch (IOException e) {
        throw new HoodieTransformException("Cannot read the timeline of " + basePath, e);
      }
    }
  }

  /**
   * Writes a file for {@code instantTime} under {@link #PAYLOAD_FOLDER} as any writer of the instant does: its CREATE marker first,
   * through the write config's markers (the streamer passes its timeline server), then the file. A rollback of the instant
   * removes the file by its marker.
   */
  static Path writeMarkedFile(JavaSparkContext jsc, TypedProperties properties, String instantTime) {
    String base = properties.getString(TARGET_BASE_PATH);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(properties).withPath(base).build();
    HoodieSparkTable<?> table = HoodieSparkTable.create(config, new HoodieSparkEngineContext(jsc));
    String fileName = UUID.randomUUID() + "_" + instantTime + PAYLOAD_EXTENSION;
    WriteMarkersFactory.get(config.getMarkersType(), table, instantTime).create(PAYLOAD_FOLDER, fileName, IOType.CREATE);
    StoragePath file = new StoragePath(new StoragePath(base, PAYLOAD_FOLDER), fileName);
    try {
      try (OutputStream out = storage.create(file, false)) {
        out.write(instantTime.getBytes(StandardCharsets.UTF_8));
      }
      Path path = new Path(file.toUri());
      PAYLOAD_WRITTEN_AT.put(path, fs.getFileStatus(path).getModificationTime());
      return path;
    } catch (IOException e) {
      throw new HoodieTransformException("Cannot write the payload of " + instantTime, e);
    }
  }

  /** Records the completed timeline at callback time, before any later test-side observation. */
  public static class CommitRecordingTransformer extends PayloadWritingTransformer {
    @Override
    public void onCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties props, String instant) {
      TRANSFORMER_COMMITS.add(instant);
      COMPLETED_AT_CALLBACK.add(completedCommits(props.getString(TARGET_BASE_PATH)));
      COMMIT_EVENTS.add("transformer");
    }
  }

  /** {@link CommitRecordingTransformer} without an instant: a plain transformer, so its chain runs as stock. */
  public static class PlainCommitRecordingTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      PayloadWritingTransformer.record(properties, null);
      return rowDataset;
    }

    @Override
    public void onCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties props, String instant) {
      TRANSFORMER_COMMITS.add(instant);
      COMPLETED_AT_CALLBACK.add(completedCommits(props.getString(TARGET_BASE_PATH)));
      COMMIT_EVENTS.add("transformer");
    }
  }

  /** A plain transformer that records each call and the pending instants at that moment, and writes nothing. */
  public static class PlainRecordingTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      PayloadWritingTransformer.record(properties, null);
      return rowDataset;
    }
  }

  /** An {@link InstantAwareTransformer} that records each call and writes nothing. */
  public static class PassThroughInstantAwareTransformer extends PlainRecordingTransformer implements InstantAwareTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      PayloadWritingTransformer.record(properties, instantTime);
      return rowDataset;
    }
  }

  /** {@link PassThroughInstantAwareTransformer} that records, at pre-commit, the writer schema the round commits with. */
  public static class WriterSchemaRecordingTransformer extends PassThroughInstantAwareTransformer {
    @Override
    public void onPreCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties properties, String instantTime,
                            HoodieTableMetaClient metaClient) {
      WRITER_SCHEMA_AT_PRE_COMMIT.add(properties.getString(HoodieWriteConfig.AVRO_SCHEMA_STRING.key(), ""));
    }
  }

  /** A plain transformer that records its call and the pending instants, then fails: before any instant-aware transformer. */
  public static class FailingPlainTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      PayloadWritingTransformer.record(properties, null);
      throw new HoodieTransformException("injected plain transformer failure");
    }
  }

  public static class ThrowingCommitTransformer extends CommitRecordingTransformer {
    @Override
    public void onCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties props, String instant) {
      super.onCommit(jsc, spark, props, instant);
      throw new IllegalStateException("injected transformer post-commit failure at " + instant);
    }
  }

  /** Produces an empty write with a changed checkpoint, so the commit client can skip it. */
  public static class EmptyCommitTransformer extends CommitRecordingTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession spark, Dataset<Row> rows, TypedProperties props, String instant) {
      return super.apply(jsc, spark, rows, props, instant).limit(0);
    }
  }

  /** Fails inside the actual commit call, after the round has written its records. */
  public static class RejectingCommitValidator<T, I, K, O extends HoodieData<WriteStatus>>
      extends SparkPreCommitValidator<T, I, K, O> {
    public RejectingCommitValidator(HoodieSparkTable<T> table, HoodieEngineContext context, HoodieWriteConfig config) {
      super(table, context, config);
    }

    @Override
    public void validate(String instant, HoodieWriteMetadata<O> result, Dataset<Row> before, Dataset<Row> after) {
      RECORDS_AT_FAILED_COMMIT.set(result.getWriteStats().get().stream().mapToLong(HoodieWriteStat::getNumWrites).sum());
      throw new HoodieValidationException("injected commit failure after records were written");
    }

    @Override
    protected void validateRecordsBeforeAndAfter(Dataset<Row> before, Dataset<Row> after, Set<String> partitions) {
      throw new HoodieValidationException("injected commit failure after records were written");
    }
  }

  @BeforeEach
  void clearCalls() {
    SEEN_INSTANTS.clear();
    PAYLOADS.clear();
    PAYLOAD_WRITTEN_AT.clear();
    PENDING_AT_APPLY.clear();
    COMMIT_EVENTS.clear();
    TRANSFORMER_COMMITS.clear();
    COMPLETED_AT_CALLBACK.clear();
    WRITER_SCHEMA_AT_PRE_COMMIT.clear();
    RECORDS_AT_FAILED_COMMIT.set(-1);
    UnchangedCheckpointSource.ARMED.set(false);
    EmptyBatchNewCheckpointSource.ARMED.set(false);
  }

  /** Writes its payload like {@link PayloadWritingTransformer}, then dies with an error the streamer does not catch: a crash. */
  public static class CrashingAfterWriteTransformer extends PayloadWritingTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      super.apply(jsc, sparkSession, rowDataset, properties, instantTime);
      throw new SimulatedCrash("simulated crash after the transformer wrote at " + instantTime);
    }
  }

  /** An {@link Error}, so no rollback in the streamer runs: the round's instant stays as the crash left it. */
  static class SimulatedCrash extends Error {
    SimulatedCrash(String message) {
      super(message);
    }
  }

  /** Writes its payload like {@link PayloadWritingTransformer}, then fails. */
  public static class FailingAfterWriteTransformer extends PayloadWritingTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      super.apply(jsc, sparkSession, rowDataset, properties, instantTime);
      throw new HoodieTransformException("injected transformer failure at " + instantTime);
    }
  }

  /**
   * Writes its payload like {@link PayloadWritingTransformer}, then breaks the instant's marker type file, so the rollback of the
   * instant cannot read its markers and fails, and then fails itself.
   */
  public static class FailingWithUnreadableMarkersTransformer extends PayloadWritingTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      super.apply(jsc, sparkSession, rowDataset, properties, instantTime);
      String base = properties.getString(TARGET_BASE_PATH);
      StoragePath markerType = new StoragePath(HoodieTestUtils.createMetaClient(storage, base).getMarkerFolderPath(instantTime),
          MarkerUtils.MARKER_TYPE_FILENAME);
      try (OutputStream out = storage.create(markerType, true)) {
        out.write("NOT_A_MARKER_TYPE".getBytes(StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new HoodieTransformException("Cannot overwrite " + markerType, e);
      }
      throw new HoodieTransformException("injected transformer failure at " + instantTime);
    }
  }

  /** A plain transformer whose rows sleep when they are evaluated, so the write runs longer than the heartbeat tolerance. */
  public static class SlowRowsTransformer extends TestHoodieDeltaStreamer.TripsWithDistanceTransformer {
    static final long SLEEP_MS = 5000;

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      Dataset<Row> withDistance = super.apply(jsc, sparkSession, rowDataset, properties);
      return sparkSession.createDataFrame(withDistance.javaRDD().mapPartitions(rows -> {
        Thread.sleep(SLEEP_MS);
        return rows;
      }), withDistance.schema());
    }
  }

  /** Armed by {@link TimeoutArmingTransformer}: the next target-schema read after it throws a source timeout, once. */
  private static final AtomicBoolean TIMEOUT_ARMED = new AtomicBoolean();

  /** Writes its payload like {@link PayloadWritingTransformer}; on its first call it arms {@link TimeoutOnceSchemaProvider}. */
  public static class TimeoutArmingTransformer extends PayloadWritingTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      Dataset<Row> rows = super.apply(jsc, sparkSession, rowDataset, properties, instantTime);
      if (SEEN_INSTANTS.size() == 1) {
        TIMEOUT_ARMED.set(true);
      }
      return rows;
    }
  }

  /** A file-based schema provider whose target-schema read throws a source timeout once, when armed. */
  public static class TimeoutOnceSchemaProvider extends FilebasedSchemaProvider {
    public TimeoutOnceSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    public HoodieSchema getTargetHoodieSchema() {
      if (TIMEOUT_ARMED.compareAndSet(true, false)) {
        throw new HoodieSourceTimeoutException("injected source timeout after the transformer");
      }
      return super.getTargetHoodieSchema();
    }
  }

  static Path payloadPath(String basePath, String instantTime) {
    return PAYLOADS.get(basePath + "#" + instantTime);
  }

  private static HoodieDeltaStreamer.Config config(String tableBasePath, List<String> transformers) {
    return config(tableBasePath, transformers, WriteOperationType.INSERT);
  }

  private static HoodieDeltaStreamer.Config config(String tableBasePath, List<String> transformers, WriteOperationType operation) {
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, operation, transformers);
    cfg.sourceClassName = TestDataSource.class.getName();
    cfg.configs.add(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() + "=LAZY");
    cfg.configs.add(HoodieWriteConfig.MARKERS_TYPE.key() + "=TIMELINE_SERVER_BASED");
    cfg.configs.add(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key() + "=true");
    cfg.configs.add(TARGET_BASE_PATH + "=" + tableBasePath);
    return cfg;
  }

  private static List<String> transformers(Class<?>... classes) {
    return Arrays.stream(classes).map(Class::getName).collect(Collectors.toList());
  }

  private static HoodieDeltaStreamer.Config commitHookConfig(String tableBasePath, Class<?> transformer) {
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, transformers(transformer, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    cfg.sourceClassName = CommitRecordingSource.class.getName();
    return cfg;
  }

  private static HoodieTableMetaClient metaClient(String tableBasePath) {
    return HoodieTestUtils.createMetaClient(storage, tableBasePath);
  }

  private static List<String> completedCommits(String tableBasePath) {
    return metaClient(tableBasePath).getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstantsAsStream()
        .map(HoodieInstant::requestedTime).collect(Collectors.toList());
  }

  private static String causes(Throwable e) {
    StringBuilder messages = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
      messages.append(t).append(" | ");
    }
    return messages.toString();
  }

  private static List<Throwable> causeChain(Throwable e) {
    List<Throwable> chain = new ArrayList<>();
    for (Throwable t = e; t != null && !chain.contains(t); t = t.getCause()) {
      chain.add(t);
    }
    return chain;
  }

  /** An instant-aware round calls its transformer back once, after the source and the durable commit; a stock round does not. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void transformerCommitCallbackRunsOnceAfterSourceAndDurableCommitOfAnInstantAwareRound(boolean earlyCommit) throws Exception {
    String tableBasePath = basePath + "/transformer_commit_callback_" + earlyCommit;
    HoodieDeltaStreamer.Config cfg = commitHookConfig(tableBasePath,
        earlyCommit ? CommitRecordingTransformer.class : PlainCommitRecordingTransformer.class);
    syncOnce(new HoodieDeltaStreamer(cfg, jsc));

    List<String> commits = completedCommits(tableBasePath);
    assertEquals(1, commits.size(), "the round durably committed");
    if (earlyCommit) {
      assertEquals(commits, TRANSFORMER_COMMITS, "the transformer is notified exactly once of that commit");
      assertEquals(Collections.singletonList(commits), COMPLETED_AT_CALLBACK, "the instant was already completed at callback time");
      assertEquals(Arrays.asList("source", "transformer"), COMMIT_EVENTS, "Source.onCommit runs before the transformer callback");
    } else {
      // a stock round (no instant-aware transformer) runs as upstream: only the source is called back
      assertEquals(Collections.emptyList(), TRANSFORMER_COMMITS, "a stock round's transformer is not called back");
      assertEquals(Collections.singletonList("source"), COMMIT_EVENTS, "Source.onCommit runs as upstream");
    }
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  @Test
  void transformerCommitCallbackDoesNotRunWhenActualCommitFails() throws Exception {
    String tableBasePath = basePath + "/transformer_callback_failed_commit";
    HoodieDeltaStreamer.Config cfg = commitHookConfig(tableBasePath, CommitRecordingTransformer.class);
    cfg.configs.add(HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.key() + "=" + RejectingCommitValidator.class.getName());
    Exception failure = assertThrows(Exception.class, () -> syncOnce(new HoodieDeltaStreamer(cfg, jsc)));

    assertTrue(causes(failure).contains("pre-commit validation failed"), causes(failure));
    assertTrue(RECORDS_AT_FAILED_COMMIT.get() > 0, "the validator rejected a commit after the actual write");
    assertEquals(1, SEEN_INSTANTS.size(), "the transformer returned before the commit failed");
    assertTrue(completedCommits(tableBasePath).isEmpty());
    assertTrue(TRANSFORMER_COMMITS.isEmpty(), "a failed commit cannot publish transformer state");
    assertTrue(COMMIT_EVENTS.isEmpty(), "neither source nor transformer receives a successful commit callback");
  }

  @Test
  void transformerCommitCallbackDoesNotRunForAnUnchangedSourceCheckpoint() throws Exception {
    String tableBasePath = basePath + "/transformer_callback_no_new_data";
    HoodieDeltaStreamer.Config cfg = commitHookConfig(tableBasePath, CommitRecordingTransformer.class);
    syncOnce(new HoodieDeltaStreamer(cfg, jsc));
    List<String> before = completedCommits(tableBasePath);
    TRANSFORMER_COMMITS.clear();
    COMMIT_EVENTS.clear();
    SEEN_INSTANTS.clear();
    cfg.sourceLimit = 0;
    syncOnce(new HoodieDeltaStreamer(cfg, jsc));

    assertEquals(before, completedCommits(tableBasePath), "the empty source round adds no completed instant");
    assertTrue(SEEN_INSTANTS.isEmpty(), "the unchanged checkpoint skips transformation");
    assertTrue(TRANSFORMER_COMMITS.isEmpty());
    assertTrue(COMMIT_EVENTS.isEmpty());
  }

  @Test
  void transformerCommitCallbackDoesNotRunWhenAnEmptyWriteSkipsCommit() throws Exception {
    String tableBasePath = basePath + "/transformer_callback_skipped_empty_commit";
    HoodieDeltaStreamer.Config cfg = commitHookConfig(tableBasePath, EmptyCommitTransformer.class);
    cfg.configs.add(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key() + "=false");
    syncOnce(new HoodieDeltaStreamer(cfg, jsc));

    assertEquals(1, SEEN_INSTANTS.size(), "the changed checkpoint reached the transformer and write path");
    assertTrue(completedCommits(tableBasePath).isEmpty(), "empty commit suppression leaves no durable instant");
    assertTrue(TRANSFORMER_COMMITS.isEmpty(), "a successful return for a skipped commit cannot publish transformer state");
    // CompletedWrite relies on this: a skipped empty commit stays pending in the active timeline, so it never reads the archive
    assertTrue(metaClient(tableBasePath).getActiveTimeline().filterInflightsAndRequested().containsInstant(SEEN_INSTANTS.get(0)),
        "the skipped empty commit is still pending");
  }

  @Test
  void throwingTransformerCommitCallbackFailsTheRoundWithoutRollingBackCompletedData() throws Exception {
    String tableBasePath = basePath + "/transformer_callback_throws_after_commit";
    HoodieDeltaStreamer.Config cfg = commitHookConfig(tableBasePath, ThrowingCommitTransformer.class);
    Exception failure = assertThrows(Exception.class, () -> syncOnce(new HoodieDeltaStreamer(cfg, jsc)));

    assertTrue(causes(failure).contains("injected transformer post-commit failure"), causes(failure));
    List<String> commits = completedCommits(tableBasePath);
    assertEquals(1, commits.size(), "the durable commit survives a callback failure");
    assertEquals(commits, TRANSFORMER_COMMITS);
    assertEquals(Collections.singletonList(commits), COMPLETED_AT_CALLBACK);
    assertEquals(Arrays.asList("source", "transformer"), COMMIT_EVENTS);
    assertEquals(0, metaClient(tableBasePath).getActiveTimeline().getRollbackTimeline().countInstants(),
        "completed data is not rolled back");
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  /**
   * A chain with no {@link InstantAwareTransformer} runs as stock: the transformer runs through the plain apply before any instant
   * exists.
   */
  @Test
  void withoutInstantAwareTransformerTheChainRunsAsStock() throws Exception {
    String tableBasePath = basePath + "/early_commit_plain_chain";
    syncOnce(config(tableBasePath, transformers(PlainRecordingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class)));

    assertEquals(Collections.singletonList(""), SEEN_INSTANTS, "called through the plain apply, without an instant");
    assertEquals(Collections.singletonList(Collections.emptyList()), PENDING_AT_APPLY, "no instant was pending at apply");
    assertEquals(1, completedCommits(tableBasePath).size());
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  /**
   * The instant is REQUESTED once the source returned data, and INFLIGHT right before the first {@link InstantAwareTransformer}
   * of the chain: a plain transformer before it sees the instant REQUESTED, the instant-aware one sees it INFLIGHT before it
   * writes its marked file, and the data files of the write come after that file.
   */
  @Test
  void requestedOnceTheSourceHasDataAndInflightRightBeforeTheFirstInstantAwareTransformer() throws Exception {
    String tableBasePath = basePath + "/early_commit_requested_then_inflight";
    syncOnce(config(tableBasePath, transformers(PlainRecordingTransformer.class, PayloadWritingTransformer.class,
        TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class), WriteOperationType.UPSERT));

    assertEquals(2, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    assertEquals("", SEEN_INSTANTS.get(0), "the plain transformer is called through the plain apply");
    String instant = SEEN_INSTANTS.get(1);
    assertEquals(1, PENDING_AT_APPLY.get(0).size(), PENDING_AT_APPLY.toString());
    assertEquals(instant, PENDING_AT_APPLY.get(0).get(0).requestedTime());
    assertEquals(HoodieInstant.State.REQUESTED, PENDING_AT_APPLY.get(0).get(0).getState(),
        "the instant is REQUESTED before the first instant-aware transformer");
    assertEquals(1, PENDING_AT_APPLY.get(1).size(), PENDING_AT_APPLY.toString());
    assertEquals(HoodieInstant.State.INFLIGHT, PENDING_AT_APPLY.get(1).get(0).getState(),
        "the instant is INFLIGHT when the instant-aware transformer writes");
    assertEquals(Collections.singletonList(instant), completedCommits(tableBasePath));

    // the payload was written while the instant was INFLIGHT (above); the data files of the write come after the payload
    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    long payloadWritten = PAYLOAD_WRITTEN_AT.get(payloadPath(tableBasePath, instant));
    List<HoodieWriteStat> stats = metaClient.getActiveTimeline().readCommitMetadata(
        metaClient.getActiveTimeline().getCommitsTimeline().lastInstant().get()).getWriteStats();
    assertFalse(stats.isEmpty());
    for (HoodieWriteStat stat : stats) {
      assertTrue(fs.getFileStatus(new Path(tableBasePath, stat.getPath())).getModificationTime() >= payloadWritten,
          stat.getPath() + " is written after the payload");
    }
  }

  /**
   * A failure while the instant is still REQUESTED (a plain transformer before the first instant-aware one) rolls the REQUESTED
   * instant back with the stock rollback: no file was written for it, nothing stays pending, and its heartbeat is stopped.
   */
  @Test
  void instantAwareFailureWhileRequestedRollsBackTheRequestedInstant() throws Exception {
    String tableBasePath = basePath + "/early_commit_failure_requested";
    HoodieDeltaStreamer.Config cfg = withCleaningPolicy(config(tableBasePath, transformers(FailingPlainTransformer.class,
        PayloadWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class)), "LAZY");
    Exception e = assertThrows(Exception.class, () -> syncOnce(cfg));
    assertTrue(causes(e).contains("injected plain transformer failure"), causes(e));

    assertEquals(Collections.singletonList(""), SEEN_INSTANTS, "the instant-aware transformer never ran");
    assertEquals(1, PENDING_AT_APPLY.get(0).size(), PENDING_AT_APPLY.toString());
    assertEquals(HoodieInstant.State.REQUESTED, PENDING_AT_APPLY.get(0).get(0).getState());
    String instant = PENDING_AT_APPLY.get(0).get(0).requestedTime();
    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    assertEquals(0, metaClient.getActiveTimeline().getCommitsTimeline().countInstants(), "no pending or completed commit");
    List<HoodieInstant> rollbacks = metaClient.getActiveTimeline().getRollbackTimeline().filterCompletedInstants().getInstants();
    assertEquals(1, rollbacks.size(), rollbacks.toString());
    assertEquals(instant, metaClient.getActiveTimeline().readRollbackMetadata(rollbacks.get(0)).getCommitsRollback().get(0));
    assertTrue(PAYLOADS.isEmpty(), "nothing was written for the instant");
    assertFalse(fs.exists(new Path(HoodieTableMetaClient.getHeartbeatFolderPath(tableBasePath), instant)), "heartbeat stopped");
  }

  /**
   * With {@code --retry-last-pending-inline-clustering-job}, the streamer retries the table's pending clustering before it starts the
   * round's instant, so the clustering does not run against the round's own pending instant.
   */
  @Test
  void instantAwareRetriesThePendingClusteringBeforeStartingItsInstant() throws Exception {
    String tableBasePath = basePath + "/early_commit_retry_clustering";
    HoodieDeltaStreamer.Config cfg = withCleaningPolicy(config(tableBasePath, transformers(PayloadWritingTransformer.class,
        TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class), WriteOperationType.BULK_INSERT), "LAZY");
    syncOnce(cfg);
    cfg.sourceLimit = 2000;
    syncOnce(cfg);
    String clustering;
    try (SparkRDDWriteClient<?> client = writeClient(tableBasePath)) {
      clustering = client.scheduleClustering(Option.empty()).get();
    }

    cfg.retryLastPendingInlineClusteringJob = true;
    cfg.configs.add("hoodie.clustering.inline=true");
    cfg.sourceLimit = 3000;
    syncOnce(cfg);

    HoodieTimeline timeline = metaClient(tableBasePath).getActiveTimeline();
    assertTrue(timeline.filterCompletedInstants().containsInstant(clustering), "the pending clustering was retried and completed");
    assertEquals(0, timeline.filterInflightsAndRequested().countInstants(), "nothing left pending");
    assertEquals(3, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    assertTrue(timeline.filterCompletedInstants().containsInstant(SEEN_INSTANTS.get(2)), "the round committed");
    String clusteringCompleted = timeline.filterCompletedInstants().getInstantsAsStream()
        .filter(i -> i.requestedTime().equals(clustering)).findFirst().get().getCompletionTime();
    assertTrue(clusteringCompleted.compareTo(SEEN_INSTANTS.get(2)) < 0,
        "the clustering completed (" + clusteringCompleted + ") before the round's instant was started (" + SEEN_INSTANTS.get(2) + ")");
  }

  /**
   * Two rounds (an empty table, then one with a schema): each round's transformer receives the instant, already INFLIGHT, that
   * the round then commits and writes a marked file for it; the commit removes the instant's markers; the plain transformer of
   * the chain still runs.
   */
  @Test
  void instantAwarePassesTheInflightInstantToTheTransformer() throws Exception {
    String tableBasePath = basePath + "/early_commit_flag_on";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, transformers(PayloadWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    syncOnce(cfg);
    cfg.sourceLimit = 2000;
    syncOnce(cfg);

    assertEquals(2, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    assertNotEquals(SEEN_INSTANTS.get(0), SEEN_INSTANTS.get(1));
    assertEquals(SEEN_INSTANTS, completedCommits(tableBasePath), "each round committed the instant its transformer saw");
    for (int i = 0; i < 2; i++) {
      String instant = SEEN_INSTANTS.get(i);
      List<HoodieInstant> pending = PENDING_AT_APPLY.get(i);
      assertEquals(1, pending.size(), pending.toString());
      assertEquals(instant, pending.get(0).requestedTime());
      assertEquals(HoodieInstant.State.INFLIGHT, pending.get(0).getState(), "the instant is INFLIGHT at apply");
      assertTrue(PAYLOADS.containsKey(tableBasePath + "#" + instant), "the transformer wrote for " + instant);
      assertFalse(fs.exists(new Path(metaClient(tableBasePath).getMarkerFolderPath(instant))), "the commit removed its markers");
      assertTrue(fs.exists(payloadPath(tableBasePath, instant)), "a committed round keeps the file its transformer wrote for " + instant);
    }
    Dataset<Row> table = sqlContext.read().format("hudi").load(tableBasePath);
    long rows = table.count();
    assertTrue(rows > SQL_SOURCE_NUM_RECORDS, "both rounds wrote rows: " + rows);
    assertEquals(rows, table.where("haversine_distance is not null").count(), "the plain transformer of the chain ran");
  }

  /**
   * The write path of each operation (the commit action executor for UPSERT, the bulk-insert helper for BULK_INSERT) takes the
   * instant that is already INFLIGHT, re-writing its inflight file instead of failing on it, and commits it.
   */
  @ParameterizedTest
  @EnumSource(value = WriteOperationType.class, names = {"UPSERT", "BULK_INSERT"})
  void instantAwareWriteTakesTheInflightInstant(WriteOperationType operation) throws Exception {
    String tableBasePath = basePath + "/early_commit_" + operation.name().toLowerCase();
    syncOnce(config(tableBasePath, transformers(PayloadWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class), operation));

    assertEquals(1, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    String instant = SEEN_INSTANTS.get(0);
    assertEquals(HoodieInstant.State.INFLIGHT, PENDING_AT_APPLY.get(0).get(0).getState(), "the instant is INFLIGHT at apply");
    assertEquals(Collections.singletonList(instant), completedCommits(tableBasePath), "the write committed that instant");
    HoodieTimeline timeline = metaClient(tableBasePath).getActiveTimeline();
    assertEquals(operation, timeline.readCommitMetadata(timeline.getCommitsTimeline().lastInstant().get()).getOperationType());
    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    assertEquals(2, metaClient.getTimelineLayoutVersion().getVersion(), "timeline layout v2: the inflight file is write-once");
    HoodieCommitMetadata inflight = metaClient.getActiveTimeline().readCommitMetadata(
        metaClient.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instant));
    assertFalse(inflight.getExtraMetadata().containsKey(ConcurrentOperation.WORKLOAD_PROFILE_PENDING_KEY),
        "no pending-workload-profile marker stays on the inflight of a committed round");
    if (operation == WriteOperationType.UPSERT) {
      // the write overwrote the inflight file the streamer created with its workload profile
      assertEquals(WriteOperationType.UPSERT, inflight.getOperationType(), "the inflight file holds the workload profile");
      assertFalse(inflight.getPartitionToWriteStats().isEmpty(), "with the file groups the write touches");
    }
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  /**
   * BULK_INSERT through the row writer (a ROW source, the streamer's row-writer switch on) builds its own write config; its
   * executor must also take the instant that is already INFLIGHT: otherwise its requested-to-inflight transition fails on the
   * existing inflight file and every round fails.
   */
  @Test
  void instantAwareRowWriterBulkInsertTakesTheInflightInstant() throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesEarlyCommit" + testNum;
    int records = 100;
    prepareParquetDFSFiles(records, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null);
    prepareParquetDFSSource(true, true, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET, PARQUET_SOURCE_ROOT, false,
        "partition_path", "", false, true);
    String tableBasePath = basePath + "/early_commit_row_writer" + testNum;
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT, ParquetDFSSource.class.getName(),
        transformers(PassThroughInstantAwareTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class),
        PROPS_FILENAME_TEST_PARQUET, false, true, 100000, false, null, null, "timestamp", null, false);
    cfg.configs.add(TARGET_BASE_PATH + "=" + tableBasePath);
    syncOnce(cfg);

    assertEquals(1, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    assertEquals(HoodieInstant.State.INFLIGHT, PENDING_AT_APPLY.get(0).get(0).getState(), "the instant is INFLIGHT at apply");

    List<String> commits = completedCommits(tableBasePath);
    assertEquals(1, commits.size(), commits.toString());
    assertEquals(0, metaClient(tableBasePath).getActiveTimeline().filterInflightsAndRequested().countInstants(), "nothing left pending");
    assertRecordCount(records, tableBasePath, sqlContext);
  }

  private static SparkRDDWriteClient<?> writeClient(String tableBasePath) {
    return new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc),
        HoodieWriteConfig.newBuilder().withPath(tableBasePath).withEmbeddedTimelineServerEnabled(false).build());
  }

  /** A round with no new data starts no instant and does not call the transformer. */
  @Test
  void instantAwareNoNewDataStartsNoInstant() throws Exception {
    String tableBasePath = basePath + "/early_commit_no_new_data";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, transformers(PayloadWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    syncOnce(cfg);
    List<HoodieInstant> before = metaClient(tableBasePath).getActiveTimeline().getInstants();
    // the test source returns no data and its previous checkpoint for a source limit of 0
    cfg.sourceLimit = 0;
    syncOnce(cfg);

    assertEquals(1, SEEN_INSTANTS.size(), "the transformer ran only in the round with data");
    assertEquals(SEEN_INSTANTS, completedCommits(tableBasePath));
    assertEquals(before, metaClient(tableBasePath).getActiveTimeline().getInstants(), "the round with no new data wrote no instant");
  }

  /**
   * A source that returns rows but its previous checkpoint has no new data: the round ends on the unchanged checkpoint before it
   * starts an instant or transforms the rows.
   */
  @Test
  void instantAwareUnchangedCheckpointWithRowsStartsNoInstant() throws Exception {
    String tableBasePath = basePath + "/early_commit_unchanged_checkpoint";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, transformers(PayloadWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    cfg.sourceClassName = UnchangedCheckpointSource.class.getName();
    syncOnce(cfg);
    List<HoodieInstant> before = metaClient(tableBasePath).getActiveTimeline().getInstants();
    UnchangedCheckpointSource.ARMED.set(true);
    syncOnce(cfg);

    assertEquals(1, SEEN_INSTANTS.size(), "the transformer ran only in the round with a new checkpoint");
    assertEquals(SEEN_INSTANTS, completedCommits(tableBasePath));
    assertEquals(before, metaClient(tableBasePath).getActiveTimeline().getInstants(), "the round with no new checkpoint wrote no instant");
  }

  /**
   * A source that returns no rows but a new checkpoint gives the instant-aware transformer nothing to write for: the round starts
   * no instant before the transformer and commits as stock.
   */
  @Test
  void instantAwareEmptyBatchWithNewCheckpointStartsNoInstantBeforeTheTransformer() throws Exception {
    String tableBasePath = basePath + "/early_commit_empty_batch";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, transformers(PayloadWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    cfg.sourceClassName = EmptyBatchNewCheckpointSource.class.getName();
    syncOnce(cfg);
    List<String> committedWithRows = completedCommits(tableBasePath);
    EmptyBatchNewCheckpointSource.ARMED.set(true);
    syncOnce(cfg);

    assertEquals(1, SEEN_INSTANTS.size(), "the transformer ran only in the round with rows");
    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    assertEquals(0, metaClient.getActiveTimeline().filterInflightsAndRequested().countInstants(), "nothing left pending");
    List<String> commits = completedCommits(tableBasePath);
    assertEquals(committedWithRows, commits.subList(0, committedWithRows.size()), commits.toString());
  }

  /**
   * A transformer failure rolls back the INFLIGHT instant, and the standard marker-based rollback removes its marked files, with
   * eager or lazy OCC cleaning. The marker directory and heartbeat are removed after rollback.
   */
  @ParameterizedTest
  @ValueSource(strings = {"EAGER", "LAZY"})
  void instantAwareTransformerFailureRollsBackTheInflightInstant(String cleaningPolicy) throws Exception {
    String tableBasePath = basePath + "/early_commit_failure_" + cleaningPolicy.toLowerCase();
    HoodieDeltaStreamer.Config cfg = withCleaningPolicy(config(tableBasePath,
        transformers(FailingAfterWriteTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class)), cleaningPolicy);
    Exception e = assertThrows(Exception.class, () -> syncOnce(cfg));
    assertTrue(causes(e).contains("injected transformer failure"), causes(e));

    assertEquals(1, SEEN_INSTANTS.size());
    String instant = SEEN_INSTANTS.get(0);
    assertEquals(HoodieInstant.State.INFLIGHT, PENDING_AT_APPLY.get(0).get(0).getState());
    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    assertEquals(0, metaClient.getActiveTimeline().getCommitsTimeline().countInstants(), "no pending or completed commit");
    List<HoodieInstant> rollbacks = metaClient.getActiveTimeline().getRollbackTimeline().filterCompletedInstants().getInstants();
    assertEquals(1, rollbacks.size(), "the instant was rolled back explicitly");
    assertEquals(instant, metaClient.getActiveTimeline().readRollbackMetadata(rollbacks.get(0)).getCommitsRollback().get(0));
    assertFalse(fs.exists(payloadPath(tableBasePath, instant)), "the marked file is removed by rollback");
    assertTrue(fs.exists(payloadPath(tableBasePath, instant).getParent()), "its folder remains");
    assertFalse(fs.exists(new Path(metaClient.getMarkerFolderPath(instant))), "rollback removed its marker directory");
    assertFalse(fs.exists(new Path(HoodieTableMetaClient.getHeartbeatFolderPath(tableBasePath), instant)), "heartbeat stopped");
  }

  /**
   * When the rollback of the failed transformer's instant fails too, the round still fails with the transformer's failure, which
   * carries the rollback failure as suppressed, and the instant's heartbeat is stopped.
   */
  @Test
  void instantAwareTransformerFailureKeepsAFailedRollbackAsSuppressed() throws Exception {
    String tableBasePath = basePath + "/early_commit_failure_rollback_fails";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath,
        transformers(FailingWithUnreadableMarkersTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    Exception e = assertThrows(Exception.class, () -> syncOnce(cfg));

    Throwable transformerFailure = causeChain(e).stream()
        .filter(t -> t instanceof HoodieTransformException && String.valueOf(t.getMessage()).contains("injected transformer failure"))
        .findFirst().orElse(null);
    assertTrue(transformerFailure != null, "the round fails with the transformer's failure: " + causes(e));
    assertEquals(1, transformerFailure.getSuppressed().length, "the failed rollback is kept as suppressed: " + causes(e));
    assertTrue(causes(transformerFailure.getSuppressed()[0]).contains("NOT_A_MARKER_TYPE"), causes(transformerFailure.getSuppressed()[0]));

    String instant = SEEN_INSTANTS.get(0);
    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    assertEquals(0, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants(), "nothing committed");
    assertFalse(fs.exists(new Path(HoodieTableMetaClient.getHeartbeatFolderPath(tableBasePath), instant)), "heartbeat stopped");
  }

  /**
   * A crash after the transformer wrote leaves the instant INFLIGHT with its marked files. The next streamer (a restart) rolls it
   * back with the standard rollback of failed writes, which removes those files, and its round then commits cleanly.
   */
  @ParameterizedTest
  @ValueSource(strings = {"EAGER", "LAZY"})
  void instantAwareCrashLeavesAnInflightInstantThatTheRestartRollsBack(String cleaningPolicy) throws Exception {
    String tableBasePath = basePath + "/early_commit_crash_" + cleaningPolicy.toLowerCase();
    // a first round, so the crashed instant is not the first instant of the table
    HoodieDeltaStreamer.Config first = config(tableBasePath, transformers(PayloadWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    syncOnce(withCleaningPolicy(first, cleaningPolicy));
    String committedFirst = SEEN_INSTANTS.get(0);

    HoodieDeltaStreamer.Config crashing = config(tableBasePath, transformers(CrashingAfterWriteTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    crashing.sourceLimit = 2000;
    Throwable crash = assertThrows(Throwable.class, () -> syncOnce(withCleaningPolicy(crashing, cleaningPolicy)));
    assertTrue(causes(crash).contains("simulated crash"), causes(crash));
    String crashed = SEEN_INSTANTS.get(1);
    List<HoodieInstant> pending = metaClient(tableBasePath).getActiveTimeline().filterInflightsAndRequested().getInstants();
    assertEquals(1, pending.size(), pending.toString());
    assertEquals(crashed, pending.get(0).requestedTime());
    assertEquals(HoodieInstant.State.INFLIGHT, pending.get(0).getState(), "the crash left the instant INFLIGHT");
    assertTrue(fs.exists(payloadPath(tableBasePath, crashed)), "the crash left its marked file");
    assertTrue(fs.exists(new Path(metaClient(tableBasePath).getMarkerFolderPath(crashed))), "and its markers");

    HoodieDeltaStreamer.Config restart = config(tableBasePath, transformers(PayloadWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    restart.sourceLimit = 2000;
    syncOnce(withCleaningPolicy(restart, cleaningPolicy));
    String committedAfter = SEEN_INSTANTS.get(2);

    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    assertEquals(Arrays.asList(committedFirst, committedAfter), completedCommits(tableBasePath));
    assertEquals(0, metaClient.getActiveTimeline().filterInflightsAndRequested().countInstants(), "nothing left pending");
    List<HoodieInstant> rollbacks = metaClient.getActiveTimeline().getRollbackTimeline().filterCompletedInstants().getInstants();
    assertEquals(1, rollbacks.size(), rollbacks.toString());
    assertEquals(crashed, metaClient.getActiveTimeline().readRollbackMetadata(rollbacks.get(0)).getCommitsRollback().get(0));
    assertFalse(fs.exists(payloadPath(tableBasePath, crashed)), "the standard rollback removed the crashed instant's marked file");
    assertFalse(fs.exists(new Path(metaClient.getMarkerFolderPath(crashed))), "and its markers");
  }

  private static HoodieDeltaStreamer.Config withCleaningPolicy(HoodieDeltaStreamer.Config cfg, String cleaningPolicy) {
    cfg.configs.removeIf(value -> value.startsWith(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() + "="));
    cfg.configs.add(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() + "=" + cleaningPolicy);
    if ("LAZY".equals(cleaningPolicy)) {
      cfg.configs.add("hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL");
      cfg.configs.add("hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider");
    }
    return cfg;
  }

  /**
   * A source timeout after the transformer returned, in the same attempt, is retried: the retry releases the instant the first
   * attempt started and held (its heartbeat stops), starts a new one, and the round commits that one; the round's LAZY cleaning
   * then rolls the released instant back, removing its marked files.
   */
  @Test
  void instantAwareSourceTimeoutRetryReleasesTheHeldInstant() throws Exception {
    String tableBasePath = basePath + "/early_commit_timeout_retry";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, transformers(TimeoutArmingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    cfg.schemaProviderClassName = TimeoutOnceSchemaProvider.class.getName();
    cfg.retryOnSourceFailures = true;
    cfg.maxRetryCount = 2;
    cfg.retryIntervalSecs = 0;
    TIMEOUT_ARMED.set(false);
    syncOnce(cfg);

    assertEquals(2, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    String held = SEEN_INSTANTS.get(0);
    String committed = SEEN_INSTANTS.get(1);
    assertEquals(Collections.singletonList(committed), completedCommits(tableBasePath));
    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    assertEquals(0, metaClient.getActiveTimeline().filterInflightsAndRequested().countInstants(), "nothing left pending");
    List<HoodieInstant> rollbacks = metaClient.getActiveTimeline().getRollbackTimeline().filterCompletedInstants().getInstants();
    assertEquals(1, rollbacks.size(), "the released instant, its heartbeat gone, was rolled back by LAZY cleaning");
    assertEquals(held, metaClient.getActiveTimeline().readRollbackMetadata(rollbacks.get(0)).getCommitsRollback().get(0));
    assertFalse(fs.exists(payloadPath(tableBasePath, held)), "the rolled-back instant's marked file is removed");
  }

  /**
   * With a 1 s heartbeat interval (the minimum) and two tolerable misses, a write that runs 5 s after the write client was
   * re-created commits only if the new client keeps the heartbeat of the pending instant.
   */
  @Test
  void instantAwareHeartbeatSurvivesWriteClientReinitialisation() throws Exception {
    String tableBasePath = basePath + "/early_commit_heartbeat";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, transformers(PayloadWritingTransformer.class, SlowRowsTransformer.class));
    cfg.configs.add(HoodieWriteConfig.CLIENT_HEARTBEAT_INTERVAL_IN_MS.key() + "=1000");
    cfg.configs.add(HoodieWriteConfig.CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES.key() + "=2");
    syncOnce(cfg);

    assertEquals(SEEN_INSTANTS, completedCommits(tableBasePath), "the round committed the instant started before the transformer");
    assertNull(metaClient(tableBasePath).getActiveTimeline().filterInflightsAndRequested().firstInstant().orElse(null));
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  /**
   * On an empty table the write client the round starts its instant with is built from the table schema, which there is none of;
   * the round re-creates it from the batch schema (the new-schema branch of initializeWriteClientAndRetryTableServices), so it
   * commits with the batch's writer schema.
   */
  @Test
  void instantAwareRoundOnAnEmptyTableCommitsWithTheBatchWriterSchema() throws Exception {
    String tableBasePath = basePath + "/early_commit_writer_schema";
    syncOnce(config(tableBasePath, transformers(WriterSchemaRecordingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class)));

    assertEquals(1, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    assertEquals(Collections.singletonList(SEEN_INSTANTS.get(0)), completedCommits(tableBasePath));
    assertEquals(1, WRITER_SCHEMA_AT_PRE_COMMIT.size(), WRITER_SCHEMA_AT_PRE_COMMIT.toString());
    HoodieSchema writerSchema = HoodieSchema.parse(WRITER_SCHEMA_AT_PRE_COMMIT.get(0));
    assertTrue(writerSchema.getField("haversine_distance").isPresent(),
        "the write client was re-created from the transformed batch's schema: " + writerSchema);
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  /**
   * An instant-aware stream's round with no rows but a new checkpoint commits as stock (no instant is handed to the transformer),
   * so its transformer is not called back for it; the round with rows before it is.
   */
  @Test
  void transformerCommitCallbackDoesNotRunForARoundWithoutRows() throws Exception {
    String tableBasePath = basePath + "/transformer_callback_empty_batch";
    HoodieDeltaStreamer.Config cfg = commitHookConfig(tableBasePath, CommitRecordingTransformer.class);
    cfg.sourceClassName = EmptyBatchNewCheckpointSource.class.getName();
    syncOnce(new HoodieDeltaStreamer(cfg, jsc));
    List<String> committedWithRows = completedCommits(tableBasePath);
    assertEquals(committedWithRows, TRANSFORMER_COMMITS, "the round with rows is called back");
    EmptyBatchNewCheckpointSource.ARMED.set(true);
    syncOnce(new HoodieDeltaStreamer(cfg, jsc));

    assertEquals(1, SEEN_INSTANTS.size(), "the transformer ran only in the round with rows");
    assertEquals(2, completedCommits(tableBasePath).size(), "the round without rows committed its new checkpoint");
    assertEquals(committedWithRows, TRANSFORMER_COMMITS, "the round without rows, committed as stock, is not called back");
  }
}
