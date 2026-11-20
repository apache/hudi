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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.exception.HoodiePreCommitRefusalException;
import org.apache.hudi.utilities.transform.InstantAwareTransformer;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.balance.BalancedRows;
import org.apache.hudi.utilities.unstructured.embedding.compute.ComputeCounters;
import org.apache.hudi.utilities.unstructured.embedding.compute.ComputeRound;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientFactory;
import org.apache.hudi.utilities.unstructured.embedding.input.EmbeddingInputConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry;
import org.apache.hudi.utilities.unstructured.embedding.prep.VideoFrameHeapCheck;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * The transformer of the embedding job: one stream (the unstructured data source, which emits each announced object's
 * bytes, inline or as a reference to the object), one table, one commit per
 * round holding every row the round prepares, with its embedding under {@code prep_and_compute}. As an
 * {@link InstantAwareTransformer} it makes the streamer start the commit, move it to INFLIGHT right before this transformer and
 * hand it the instant ({@link #apply(JavaSparkContext, SparkSession, Dataset, TypedProperties, String)}), so every out-of-line file the
 * round writes (input-store frames and images, prepared payloads) is written after its marker under an inflight instant; a throw
 * rolls the instant back with the standard marker-based rollback, which removes those files, and the checkpoint does not move.
 *
 * <p>One round:
 * <ol>
 *   <li>releases the previous round's caches for this table (a transformer has no hook after the commit);</li>
 *   <li>checks, at the start of the round (after the streamer fetched the batch and started the instant, before any prep
 *   work; a refusal rolls the instant back): the table's version; under
 *   {@code prep_only} no model and no compute setting; under {@code prep_and_compute} exactly one active model, whose column and
 *   dimensions name and size the VECTOR column (an existing table's column of that name has the model's dimensions), with a
 *   loadable client; no Spark speculation; the prep extension allow-list; the preprocessors' configuration; when the job preps
 *   video, that the executor heap holds its frame decodes ({@link VideoFrameHeapCheck});</li>
 *   <li>takes the output schema: an existing table's latest writer schema without meta fields, its columns beyond the base
 *   ones nullable, with the model's column added
 *   when missing; else {@link EmbeddingTableSchema#build}, which has no VECTOR column under {@code prep_only};</li>
 *   <li>prepares the batch ({@link PrepRound}), persisted and counted;</li>
 *   <li>{@code prep_only}: returns the prepared rows;</li>
 *   <li>{@code prep_and_compute}: spreads the prepared rows without an error over the compute tasks by a hash of their keys
 *   ({@link ComputeRound#hashPartitionByKey}, {@link EmbeddingTableConfig#COMPUTE_PARALLELISM}), embeds them, persisted and counted, and
 *   returns the data rows, one error-only row per PERMANENT input, and prep's error rows. With no row left to embed it skips
 *   compute and returns prep's error rows.</li>
 * </ol>
 * One model per embedding table: a round fills the active model's VECTOR column only. Switching the active model adds the new
 * model's column, and every row the new model writes carries NULL in the old model's column.
 * A row carrying {@code _corrupt_record} goes to the job's error table ({@code CUSTOM_TRANSFORMER_FAILURE}); with the error
 * table off a failed file, chunk or PERMANENT input fails the round. Every cache of a round is at
 * {@code hoodie.write.tagged.record.storage.level}; the source batch is released once prep has run, and the prep output, the
 * balancer's assignment and the compute output stay until the table's next round or this transformer closes.
 */
public class EmbeddingTableTransformer implements InstantAwareTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddingTableTransformer.class);
  /** The prefix of every compute setting. */
  private static final String COMPUTE_PREFIX = EmbeddingTableConfig.PREFIX + "compute.";

  /** The caches of each table's last round, released at the table's next round. */
  private static final ConcurrentMap<String, RoundCaches> ROUND_CACHES = new ConcurrentHashMap<>();
  /** Driver-only cache ownership, guarded by this transformer's apply/close monitor. */
  private final Map<String, RoundCaches> ownedRoundCaches = new LinkedHashMap<>();

  private final ConcurrentMap<String, ComputeCheck> pendingCompute = new ConcurrentHashMap<>();

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
    throw new HoodieException(getClass().getSimpleName() + " writes every row of a round under the round's instant: it runs only "
        + "through the streamer, which hands it the instant. Nothing was written");
  }

  /**
   * The schema of the rows a round writes ({@link #roundSchema} on the table as it is), with {@code _corrupt_record} while the
   * error table is on. Reads the table's latest writer schema only: no Spark job and no model call.
   */
  @Override
  public StructType transformedSchema(JavaSparkContext jsc, SparkSession sparkSession, StructType incomingStruct, TypedProperties properties) {
    EmbeddingTableConfig config = EmbeddingTableConfig.from(properties);
    Option<StructType> table = EmbeddingTableSchema.writerReadSchema(config.targetTablePath(),
        HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()));
    StructType schema = roundSchema(config, table);
    return PrepRound.errorTableEnabled(properties)
        ? schema.add(DataTypes.createStructField(PrepRound.CORRUPT_RECORD, DataTypes.StringType, true)) : schema;
  }

  @Override
  public synchronized Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                            String instantTime) {
    EmbeddingTableConfig config = EmbeddingTableConfig.from(properties);
    String target = config.targetTablePath();
    releaseRound(target);
    ownedRoundCaches.remove(target);
    pendingCompute.remove(target);
    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration());
    // the table first: its minimum version
    PrepRound.checkTable(config, storageConf);
    Option<StructType> table = EmbeddingTableSchema.writerReadSchema(target, storageConf);
    Option<EmbeddingModelSpec> model = checkRound(config, sparkSession, table);
    StructType tableSchema = outputSchema(config, table, model);
    try {
      return round(jsc, sparkSession, rowDataset, config, model, storageConf, tableSchema, instantTime);
    } catch (Exception e) {
      // every Spark job of the round may fail with a checked SparkException: wrapped, so the streamer's rollback of the instant runs
      throw e instanceof RuntimeException ? (RuntimeException) e
          : new HoodieException("Embedding round of " + target + " failed at instant " + instantTime, e);
    }
  }

  /** Releases the caches of {@code targetTablePath}'s last round, if any are still held. */
  public static void releaseRound(String targetTablePath) {
    RoundCaches caches = ROUND_CACHES.remove(targetTablePath);
    if (caches != null) {
      caches.release();
    }
  }

  /** The caches a round leaves: the prep output, the balancer's assignment and the compute output. */
  private static final class RoundCaches {
    private final List<Dataset<Row>> outputs = new ArrayList<>();
    private BalancedRows balance;

    synchronized void release() {
      List<Dataset<Row>> retained = new ArrayList<>(outputs);
      BalancedRows retainedBalance = balance;
      outputs.clear();
      balance = null;
      try {
        retained.forEach(output -> output.unpersist(false));
      } finally {
        if (retainedBalance != null) {
          retainedBalance.close();
        }
      }
    }
  }

  // ------------------------------------------------------------------------------------------------------------ checks

  /**
   * The round's checks after the table's version ({@link PrepRound#checkTable}), at the start of the round before any prep work.
   *
   * @param table the existing table's latest writer schema, empty for a new table
   * @return under {@code prep_and_compute} the active model, which names and sizes the VECTOR column; under {@code prep_only}
   *     empty
   */
  static Option<EmbeddingModelSpec> checkRound(EmbeddingTableConfig config, SparkSession spark, Option<StructType> table) {
    Option<EmbeddingModelSpec> model = checkJob(config);
    if (model.isPresent() && table.isPresent()) {
      checkModelColumnMatchesTable(model.get(), config.targetTablePath(), table.get());
    }
    PrepRound.checkNoSpeculation(spark.sparkContext().getConf());
    PreprocessorRegistry registry = new PreprocessorRegistry(config.getProps());
    try {
      if (registry.storesInputs(config)) {
        // the input store's inline threshold against the per-task heap ceiling, before any Spark work
        EmbeddingInputConfig.checkOnDriver(config.getProps(), spark.sparkContext().getConf());
        OutOfLineWriter.checkTimelineServerMarkers(HadoopFSUtils.getStorageConf(spark.sparkContext().hadoopConfiguration()),
            config.targetTablePath(), config.getProps());
      }
      if (VideoFrameHeapCheck.prepsVideo(config, registry)) {
        // the executor heap must hold the frame decodes of a video: refused before any Spark work
        VideoFrameHeapCheck.check(config, spark.sparkContext().getConf());
      }
    } finally {
      registry.close();
    }
    return model;
  }

  /** Checks property-only mode, model and preprocessor settings before source or table construction. */
  public static Option<EmbeddingModelSpec> checkJob(EmbeddingTableConfig config) {
    TypedProperties props = config.getProps();
    Option<EmbeddingModelSpec> model = Option.empty();
    if (config.computes()) {
      model = Option.of(activeModel(config));

      // the declared modalities: set, sendable by the model's client, within a known model's
      ModelModalityChecks.checkDeclaration(model.get());
      // the model's client must load on the driver, so a misconfigured model fails the round before any Spark work
      EmbeddingClientFactory.checkConfig(model.get(), props);
      config.computeBatchSize();
      config.computeMaxAttempts();
      config.computeRetryBackoffMs();
      config.computeParallelism(1);
      // the image settings and their decoder too, whether or not this model's client embeds images
      config.imagePreprocessing();
    } else {
      checkNoModelOrComputeSettings(config);
    }

    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    try {
      PrepRound.checkPrepFileExtensions(config, registry);
      registry.validateConfig(props, TokenizerFactory.create(props));
      if (model.isPresent()) {
        // content of a modality the model does not declare: refused with the error table off, a WARN with it on
        ModelModalityChecks.checkContent(config, registry, model.get());
      }
      EmbeddingInputConfig.inlineMaxBytes(props);
      EmbeddingInputConfig.inlineMaxHeapFraction(props);

    } finally {
      registry.close();
    }
    config.prepSourceRetry();
    config.prepPrefetchMaxBytes();
    return model;
  }

  /**
   * The one active model ({@link EmbeddingTableConfig#ACTIVE_MODELS}, default every declared model).
   *
   * @throws IllegalArgumentException when not exactly one model is active
   */
  static EmbeddingModelSpec activeModel(EmbeddingTableConfig config) {
    List<EmbeddingModelSpec> active = config.activeModels();
    if (active.size() != 1) {
      throw new IllegalArgumentException("The embedding job fills one model column, but "
          + active.stream().map(EmbeddingModelSpec::getName).collect(Collectors.toList()) + " are active; declare one model in "
          + EmbeddingTableConfig.MODELS.key() + " or set " + EmbeddingTableConfig.ACTIVE_MODELS.key() + " to one of them. Nothing was "
          + "written");
    }
    return active.get(0);
  }

  /**
   * Under {@code prep_only} no model and no compute setting is set: no {@link EmbeddingTableConfig#MODELS} or
   * {@link EmbeddingTableConfig#ACTIVE_MODELS}, no per-model {@code hoodie.unstructured.embedding.model.<alias>.*} key (its
   * column, dimensions, provider, name, client class or client settings) and no {@code hoodie.unstructured.embedding.compute.*}
   * key. A new table it creates has no VECTOR column; an existing table keeps its columns. The refusal offers
   * {@code prep_and_compute}, which a new or an existing table takes.
   *
   * @throws IllegalArgumentException naming every model and compute key set
   */
  static void checkNoModelOrComputeSettings(EmbeddingTableConfig config) {
    List<String> refused = config.getProps().stringPropertyNames().stream()
        .filter(EmbeddingTableTransformer::isModelOrComputeKey).sorted().collect(Collectors.toList());
    if (!refused.isEmpty()) {
      throw new IllegalArgumentException("Config " + EmbeddingTableConfig.MODE.key() + "=" + EmbeddingTableConfig.MODE_PREP_ONLY
          + " declares no model and computes no embedding, but model or compute settings are set: " + refused + ". Remove them"
          + ", or set " + EmbeddingTableConfig.MODE.key() + "=" + EmbeddingTableConfig.MODE_PREP_AND_COMPUTE
          + ". Nothing was written");
    }
  }

  private static boolean isModelOrComputeKey(String key) {
    return key.equals(EmbeddingTableConfig.MODELS.key()) || key.equals(EmbeddingTableConfig.ACTIVE_MODELS.key())
        || key.startsWith(EmbeddingTableConfig.MODEL_PREFIX) || key.startsWith(COMPUTE_PREFIX);
  }

  /**
   * The schema of the rows a round of {@code config} writes on the table whose latest writer schema is {@code table} (empty for a
   * new table): {@link #outputSchema} with the active model under {@code prep_and_compute}, none under {@code prep_only}.
   */
  public static StructType roundSchema(EmbeddingTableConfig config, Option<StructType> table) {
    return outputSchema(config, table, config.computes() ? Option.of(activeModel(config)) : Option.empty());
  }

  /**
   * The schema of the rows the round writes: the latest writer schema of an existing table ({@code table}) without its meta
   * fields, every column beyond the base ones nullable (prep writes null in a column it does not make), with the nullable
   * prepared-image BLOB column added when missing and the active model's VECTOR column added under
   * {@code prep_and_compute} when the table does not have it yet; else the configuration's
   * ({@link EmbeddingTableSchema#build}).
   */
  static StructType outputSchema(EmbeddingTableConfig config, Option<StructType> table, Option<EmbeddingModelSpec> model) {
    if (!table.isPresent()) {
      return EmbeddingTableSchema.build(config);
    }
    StructType existing = table.get();
    // Preserve nullable-image evolution for tables created before image preparation.
    if (Arrays.stream(existing.fields()).noneMatch(field -> field.name().equalsIgnoreCase(EmbeddingTableSchema.PREPARED_IMAGE))) {
      existing = existing.add(EmbeddingTableSchema.preparedImageField());
    }
    EmbeddingTableSchema.checkBaseColumns(existing);
    // every column of the table is in the round's rows, so the write keeps the table's schema; one beyond the base columns (a path
    // partition, a model's VECTOR, a column a customer's transformer added) is nullable, since prep writes null in a column it
    // does not make. A customer's transformer that adds its column again in the round wins over that null (UnstructuredSchemaGuard)
    List<StructField> fields = Arrays.stream(existing.fields())
        .filter(field -> !HoodieRecord.HOODIE_META_COLUMNS.contains(field.name()))
        .map(field -> EmbeddingTableSchema.BASE_COLUMNS.contains(field.name()) || field.nullable() ? field
            : new StructField(field.name(), field.dataType(), true, field.metadata()))
        .collect(Collectors.toList());
    if (model.isPresent()) {
      StructField vector = EmbeddingTableSchema.vectorField(model.get());
      if (fields.stream().noneMatch(field -> field.name().equals(vector.name()))) {
        fields.add(vector);
      }
    }
    return DataTypes.createStructType(fields);
  }

  /**
   * When {@code tableSchema} already has the model's column, that column is a VECTOR of the model's dimensions, the size
   * {@link EmbeddingTableSchema#vectorField} gives it.
   *
   * @throws IllegalArgumentException when the table's column of that name is not a VECTOR of the model's dimensions
   */
  static void checkModelColumnMatchesTable(EmbeddingModelSpec model, String tablePath, StructType tableSchema) {
    String expected = EmbeddingTableSchema.vectorField(model).metadata().getString(HoodieSchema.TYPE_METADATA_FIELD);
    for (StructField field : tableSchema.fields()) {
      if (!field.name().equals(model.getColumn())) {
        continue;
      }
      boolean vector = field.metadata().contains(HoodieSchema.TYPE_METADATA_FIELD)
          && field.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD).startsWith("VECTOR(");
      String stored = vector ? field.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD) : field.dataType().simpleString();
      if (!vector || ((HoodieSchema.Vector) HoodieSchema.parseTypeDescriptor(stored)).getDimension() != model.getDimension()) {
        throw new IllegalArgumentException("Model " + model.getName() + " writes its column " + model.getColumn() + " as " + expected
            + " (" + EmbeddingTableConfig.MODEL_PREFIX + model.getName() + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX + "="
            + model.getDimension() + "), but embedding table " + tablePath + " stores " + model.getColumn() + " as " + stored
            + ": set the model's dimensions to the table's, or write this job to a new table. Nothing was written");
      }
    }
  }

  // ------------------------------------------------------------------------------------------------------------ the round

  private Dataset<Row> round(JavaSparkContext jsc, SparkSession spark, Dataset<Row> blobRows, EmbeddingTableConfig config,
                             Option<EmbeddingModelSpec> computedModel, StorageConfiguration<?> storageConf, StructType tableSchema,
                             String instant) {
    TypedProperties props = config.getProps();
    String target = config.targetTablePath();
    StructType outputSchema = tableSchema.add(DataTypes.createStructField(PrepRound.CORRUPT_RECORD, DataTypes.StringType, true));
    StorageLevel level = EmbeddingTableConfig.persistLevel(props);
    RoundCaches caches = new RoundCaches();
    try {
      PrepRound.Prepared prepared = PrepRound.prepare(spark, blobRows, config, storageConf, tableSchema, outputSchema, instant, level);
      caches.outputs.add(prepared.output);
      caches.balance = prepared.balance;
      if (!computedModel.isPresent()) {
        retainRound(target, caches);
        return routed(prepared.output, props);
      }
      EmbeddingModelSpec model = computedModel.get();
      Dataset<Row> toEmbed = prepared.output.filter(functions.col(PrepRound.CORRUPT_RECORD).isNull()).drop(PrepRound.CORRUPT_RECORD);
      Dataset<Row> prepErrors = prepared.output.filter(functions.col(PrepRound.CORRUPT_RECORD).isNotNull());
      if (toEmbed.isEmpty()) {
        // nothing left to embed: no compute task runs and no client is built
        LOG.info("Embedding round of {} at instant {}: model {} has no row to embed", target, instant, model.getName());
        retainRound(target, caches);
        return routed(prepErrors, props);
      }
      int parallelism = config.computeParallelism(jsc.defaultParallelism());
      ComputeCounters counters = ComputeCounters.register(jsc.sc());
      Dataset<Row> embedded = ComputeRound.embed(spark, ComputeRound.hashPartitionByKey(toEmbed, parallelism), tableSchema, model, props,
          new BlobBytesSource(target, storageConf, config.prepSourceRetry()), counters, level);
      caches.outputs.add(embedded);
      pendingCompute.put(target, new ComputeCheck(instant, model, counters));
      LOG.info("Embedding round of {} at instant {}: model {} over {} task(s): {} row(s) embedded, {} failed for good", target,
          instant, model.getName(), parallelism, counters.getRowsEmbedded().value(), counters.getRowsPermanent().value());
      if (!PrepRound.errorTableEnabled(props)) {
        // counted in the persisted output, not the accumulators: a retried compute task adds to those again
        long permanent = embedded.filter(ComputeRound.errorColumn(embedded).isNotNull()).count();
        if (permanent > 0) {
          throw ComputeRound.permanentWithoutErrorTable(model, target, permanent);
        }
      }
      Dataset<Row> output = ComputeRound.dataRows(embedded, outputSchema)
          .union(ComputeRound.errorRows(embedded, outputSchema, model))
          .union(prepErrors);
      retainRound(target, caches);
      return routed(output, props);
    } catch (Exception e) {
      // a failed Spark job surfaces as a checked SparkException: the round's caches are released for it too
      pendingCompute.remove(target);
      caches.release();
      throw e instanceof RuntimeException ? (RuntimeException) e
          : new HoodieException("Embedding round of " + target + " failed at instant " + instant, e);
    }
  }

  /**
   * The output the streamer writes: with the error table on, as is (the streamer routes {@code _corrupt_record} rows to it);
   * with it off, without the error rows and their column.
   */
  private static Dataset<Row> routed(Dataset<Row> output, TypedProperties props) {
    if (PrepRound.errorTableEnabled(props)) {
      return output;
    }
    return output.filter(functions.col(PrepRound.CORRUPT_RECORD).isNull()).drop(PrepRound.CORRUPT_RECORD);
  }

  @Override
  public synchronized void close() {
    RuntimeException first = null;
    for (Map.Entry<String, RoundCaches> entry : ownedRoundCaches.entrySet()) {
      ROUND_CACHES.remove(entry.getKey(), entry.getValue());
      try {
        entry.getValue().release();
      } catch (RuntimeException failure) {
        if (first == null) {
          first = failure;
        } else {
          first.addSuppressed(failure);
        }
      }
    }
    ownedRoundCaches.clear();
    if (first != null) {
      throw first;
    }
  }

  private void retainRound(String target, RoundCaches caches) {
    ownedRoundCaches.put(target, caches);
    ROUND_CACHES.put(target, caches);
  }

  private static final class ComputeCheck {
    private final String instant;
    private final EmbeddingModelSpec model;
    private final ComputeCounters counters;

    private ComputeCheck(String instant, EmbeddingModelSpec model, ComputeCounters counters) {
      this.instant = instant;
      this.model = model;
      this.counters = counters;
    }
  }

  @Override
  public void onPreCommit(JavaSparkContext jsc, SparkSession sparkSession, TypedProperties properties, String instantTime,
                          HoodieTableMetaClient metaClient) {
    String target = EmbeddingTableConfig.from(properties).targetTablePath();
    ComputeCheck compute = pendingCompute.get(target);
    if (compute != null && compute.instant.equals(instantTime)) {
      try {
        ComputeRound.checkRefused(compute.model, compute.counters);
      } catch (EmbeddingClientConfigException refusal) {
        throw new HoodiePreCommitRefusalException(refusal.getMessage(), refusal);
      }
    }
  }

  @Override
  public void onCommit(JavaSparkContext jsc, SparkSession sparkSession, TypedProperties properties, String instantTime) {
    String target = EmbeddingTableConfig.from(properties).targetTablePath();
    pendingCompute.computeIfPresent(target, (path, compute) -> compute.instant.equals(instantTime) ? null : compute);
  }
}
