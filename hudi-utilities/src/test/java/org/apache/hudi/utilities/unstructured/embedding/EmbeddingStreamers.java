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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.blob.BlobTableConfig;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceConfig;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.streamer.JsonLinesErrorTableWriter;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.streamer.EmbeddingTableTransformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Runs the embedding job in tests: ONE streamer per table, {@link UnstructuredDataSource} (mode embedding) over an S3 events table and
 * {@link EmbeddingTableTransformer}, one commit per round with the stock S3 events checkpoint ({@code <events commit>#<key>}).
 */
public final class EmbeddingStreamers {

  /** The suffix of the job's error table path, beside the embedding table. */
  public static final String ERRORS_SUFFIX = "_error_table";
  /** The model the harness declares under prep_and_compute: the dummy provider, 8 dimensions, column {@code emb_dummy}. */
  public static final String MODEL = "dummy";
  public static final String MODEL_COLUMN = "emb_" + MODEL;
  public static final int MODEL_DIMENSIONS = 8;

  private EmbeddingStreamers() {
  }

  /**
   * The embedding job's properties for the embedding table {@code table} over the S3 events table {@code eventsTable}
   * ({@link UnstructuredDataSource} in mode embedding on cloud s3, objects read under {@code fsPrefix}, inline up to {@code inlineMaxBytes}): a
   * non-partitioned table, the streamer settings the job needs, the dummy model under prep_and_compute (none under prep_only),
   * the error table, then {@code overrides}.
   */
  public static TypedProperties jobProps(String table, String eventsTable, String fsPrefix, long inlineMaxBytes, String mode,
                                         TypedProperties overrides) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.partitionpath.field", "");
    props.setProperty("hoodie.datasource.write.keygenerator.class", NonpartitionedKeyGenerator.class.getName());
    props.setProperty(HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key(), eventsTable);
    props.setProperty(S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX.key(), fsPrefix);
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), String.valueOf(inlineMaxBytes));
    props.putAll(overrides);
    TypedProperties job = commonProps(table, mode);
    job.putAll(props);
    return job;
  }

  /** The table, writer, streamer, model (prep_and_compute only) and error-table settings every embedding job here shares. */
  private static TypedProperties commonProps(String table, String mode) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), table);
    props.setProperty(UnstructuredSourceConfig.MODE.key(), "embedding");
    props.setProperty(UnstructuredSourceConfig.CLOUD.key(), "s3");
    props.setProperty("hoodie.datasource.write.recordkey.field", String.join(",", EmbeddingTableSchema.KEY_COLUMNS));
    props.setProperty("hoodie.datasource.write.hive_style_partitioning", "false");
    props.setProperty("hoodie.datasource.write.partitionpath.urlencode", "false");
    props.setProperty("hoodie.write.record.merge.mode", RecordMergeMode.COMMIT_TIME_ORDERING.name());
    props.setProperty("hoodie.datasource.write.reconcile.schema", "false");
    props.setProperty("hoodie.metadata.enable", "false");
    props.setProperty("hoodie.compact.inline", "false");
    props.setProperty("hoodie.clean.automatic", "false");
    props.setProperty("hoodie.upsert.shuffle.parallelism", "2");
    props.setProperty("hoodie.insert.shuffle.parallelism", "2");
    props.setProperty(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key(), EmbeddingParquetConfigInjector.class.getName());
    props.setProperty(EmbeddingTableConfig.MODE.key(), mode);
    if (EmbeddingTableConfig.MODE_PREP_AND_COMPUTE.equals(mode)) {
      // a prep_only job declares no model: its table has no model column
      props.setProperty(EmbeddingTableConfig.MODELS.key(), MODEL);
      props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX,
          String.valueOf(MODEL_DIMENSIONS));
      props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX,
          DummyEmbeddingClient.PROVIDER);
      props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX,
          EmbeddingTableSchema.MODALITY_TEXT);
    }
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "true");
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_WRITE_CLASS.key(), JsonLinesErrorTableWriter.class.getName());
    props.setProperty(HoodieErrorTableConfig.ERROR_TARGET_TABLE.key(), "embedding_error_table");
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_BASE_PATH.key(), withoutTrailingSlash(table) + ERRORS_SUFFIX);
    return props;
  }

  /**
   * The streamer configuration of the embedding job on {@code table}: the unstructured data source, MERGE_ON_READ, UPSERT,
   * commit-time ordering.
   */
  public static HoodieStreamer.Config config(String table) {
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetBasePath = table;
    cfg.targetTableName = "embedding_table";
    cfg.tableType = HoodieTableType.MERGE_ON_READ.name();
    cfg.sourceClassName = UnstructuredDataSource.class.getName();
    cfg.transformerClassNames = Collections.singletonList(EmbeddingTableTransformer.class.getName());
    cfg.operation = WriteOperationType.UPSERT;
    cfg.sourceOrderingFields = null;
    cfg.recordMergeMode = RecordMergeMode.COMMIT_TIME_ORDERING;
    Triple<RecordMergeMode, String, String> merge = HoodieTableConfig.inferMergingConfigsForWrites(
        cfg.recordMergeMode, cfg.payloadClassName, cfg.recordMergeStrategyId, cfg.sourceOrderingFields,
        HoodieTableVersion.current());
    cfg.recordMergeMode = merge.getLeft();
    cfg.payloadClassName = merge.getMiddle();
    cfg.recordMergeStrategyId = merge.getRight();
    cfg.sourceLimit = Long.MAX_VALUE;
    cfg.forceDisableCompaction = true;
    return cfg;
  }

  /** One round of the embedding job with {@code props} ({@link #jobProps}). */
  public static Round round(SparkSession spark, TypedProperties props) {
    return round(spark, props, cfg -> { });
  }

  /** One round, with {@code tune} applied to the streamer configuration first. */
  public static Round round(SparkSession spark, TypedProperties props, Consumer<HoodieStreamer.Config> tune) {
    return round(spark, props, tune, true);
  }

  /**
   * One round, with {@code tune} applied to the streamer configuration first; with {@code clearCache}, the Spark catalog cache is
   * cleared after it (a caller running another job on the same session at the same time clears it once both are done).
   */
  public static Round round(SparkSession spark, TypedProperties props, Consumer<HoodieStreamer.Config> tune, boolean clearCache) {
    String table = props.getString(HoodieWriteConfig.BASE_PATH.key());
    String errorTable = errorTablePath(props);
    StorageConfiguration<?> storageConf = storageConf(spark);
    List<HoodieInstant> before = commits(table, storageConf);
    int errorsBefore = events(errorTable).size();
    HoodieStreamer.Config cfg = config(table);
    tune.accept(cfg);
    // the job names its table by --target-base-path alone, as a rendered job does
    TypedProperties job = TypedProperties.copy(props);
    job.remove(HoodieWriteConfig.BASE_PATH.key());
    try {
      HoodieStreamer streamer = new HoodieStreamer(cfg, JavaSparkContext.fromSparkContext(spark.sparkContext()), Option.of(job));
      try {
        streamer.sync();
      } finally {
        streamer.shutdownGracefully();
      }
    } catch (RuntimeException e) {
      // a refusal or a failed round propagates as the streamer threw it, so a test can assert its type
      throw e;
    } catch (Exception e) {
      throw new HoodieException("The embedding round on " + table + " failed", e);
    } finally {
      // every round here is a new streamer; the next one releases nothing of this one's, so the catalog is cleared
      if (clearCache) {
        spark.catalog().clearCache();
      }
    }
    List<HoodieInstant> after = commits(table, storageConf);
    List<HoodieInstant> added = after.stream().filter(instant -> !before.contains(instant)).collect(Collectors.toList());
    List<Map<String, Object>> errors = payloads(errorTable);
    return new Round(table, storageConf, added, errors.subList(errorsBefore, errors.size()));
  }

  // ------------------------------------------------------------------------------------------------------------ reading back

  /** The error table path of the job's properties. */
  public static String errorTablePath(TypedProperties props) {
    return props.getString(HoodieErrorTableConfig.ERROR_TABLE_BASE_PATH.key());
  }

  /** The payloads of the error events under {@code errorTable}; every event is a transformer failure. */
  @SuppressWarnings("unchecked")
  public static List<Map<String, Object>> payloads(String errorTable) {
    List<Map<String, Object>> payloads = new ArrayList<>();
    for (Map<String, Object> event : events(errorTable)) {
      if (!"CUSTOM_TRANSFORMER_FAILURE".equals(event.get("reason"))) {
        throw new AssertionError("an error event that is not a transformer failure: " + event);
      }
      payloads.add((Map<String, Object>) event.get("payload"));
    }
    return payloads;
  }

  private static List<Map<String, Object>> events(String errorTable) {
    try {
      return JsonLinesErrorTableWriter.read(errorTable);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** The completed commits of {@code table} in completion order (none when there is no table). */
  public static List<HoodieInstant> commits(String table, StorageConfiguration<?> storageConf) {
    if (!EmbeddingTableChecks.tableExists(table, storageConf)) {
      return Collections.emptyList();
    }
    return metaClient(table, storageConf).getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants();
  }

  /** The commit metadata of {@code commit} of {@code table}. */
  public static HoodieCommitMetadata metadata(String table, StorageConfiguration<?> storageConf, HoodieInstant commit) {
    try {
      return metaClient(table, storageConf).getActiveTimeline().readCommitMetadata(commit);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** The stock S3 events checkpoint ({@code <events commit>#<key>}, the V1 key) {@code commit} carries, or null. */
  public static String checkpoint(String table, StorageConfiguration<?> storageConf, HoodieInstant commit) {
    return metadata(table, storageConf, commit).getMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1);
  }

  /** The keys of {@code commit}'s extra metadata that name a streamer checkpoint (V1 or V2). */
  public static List<String> checkpointKeys(String table, StorageConfiguration<?> storageConf, HoodieInstant commit) {
    return metadata(table, storageConf, commit).getExtraMetadata().keySet().stream()
        .filter(key -> key.equals(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1)
            || key.equals(StreamerCheckpointV2.STREAMER_CHECKPOINT_KEY_V2) || key.toLowerCase().contains("checkpoint"))
        .sorted().collect(Collectors.toList());
  }

  public static HoodieTableMetaClient metaClient(String table, StorageConfiguration<?> storageConf) {
    return HoodieTableMetaClient.builder().setConf(storageConf.newInstance()).setBasePath(table).build();
  }

  public static StorageConfiguration<?> storageConf(SparkSession spark) {
    return HadoopFSUtils.getStorageConf(spark.sparkContext().hadoopConfiguration());
  }

  /** The causes of {@code e}, outermost first, as one string. */
  public static String causes(Throwable e) {
    StringBuilder text = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
      text.append(t.getClass().getName()).append(": ").append(t.getMessage()).append('\n');
    }
    return text.toString();
  }

  /**
   * The first cause of {@code e} of {@code type}, searching each cause's suppressed exceptions too: the streamer reports a source
   * whose construction failed as suppressed exceptions of its own failure.
   */
  public static <T extends Throwable> Option<T> causeOf(Throwable e, Class<T> type) {
    for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
      if (type.isInstance(t)) {
        return Option.of(type.cast(t));
      }
      for (Throwable suppressed : t.getSuppressed()) {
        Option<T> found = causeOf(suppressed, type);
        if (found.isPresent()) {
          return found;
        }
      }
    }
    return Option.empty();
  }

  private static String withoutTrailingSlash(String path) {
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }

  /** What one round committed. */
  public static final class Round {
    private final String table;
    private final StorageConfiguration<?> storageConf;
    private final List<HoodieInstant> added;
    private final List<Map<String, Object>> errors;

    Round(String table, StorageConfiguration<?> storageConf, List<HoodieInstant> added, List<Map<String, Object>> errors) {
      this.table = table;
      this.storageConf = storageConf;
      this.added = added;
      this.errors = errors;
    }

    /** The commit the round completed; empty when it committed nothing. */
    public Option<HoodieInstant> getCommit() {
      if (added.size() > 1) {
        throw new AssertionError("one round completed several commits: " + added);
      }
      return added.isEmpty() ? Option.empty() : Option.of(added.get(0));
    }

    /** Every instant the round completed, in completion order. */
    public List<HoodieInstant> getCompleted() {
      return added;
    }

    /** The incoming records committed (inserts plus updates). */
    public long getRowsWritten() {
      Option<HoodieInstant> commit = getCommit();
      if (!commit.isPresent()) {
        return 0L;
      }
      return metadata(table, storageConf, commit.get()).getWriteStats().stream()
          .mapToLong(stat -> stat.getNumInserts() + stat.getNumUpdateWrites()).sum();
    }

    /** The payloads of the error events the round committed. */
    public List<Map<String, Object>> getErrors() {
      return errors;
    }

    @Override
    public String toString() {
      return "completed=" + added + " errors=" + errors;
    }
  }
}
