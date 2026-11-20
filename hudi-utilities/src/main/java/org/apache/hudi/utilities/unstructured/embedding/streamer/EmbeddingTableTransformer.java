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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.transform.InstantAwareTransformer;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.balance.BalancedRows;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;

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
 * round holding every row the round prepares. It runs {@code prep_only}; {@code prep_and_compute} is refused. As an
 * {@link InstantAwareTransformer} it makes the streamer start the commit, move it to INFLIGHT right before this transformer and
 * hand it the instant ({@link #apply(JavaSparkContext, SparkSession, Dataset, TypedProperties, String)}), so every out-of-line file the
 * round writes (input-store frames and images, prepared payloads) is written after its marker under an inflight instant; a throw
 * rolls the instant back with the standard marker-based rollback, which removes those files, and the checkpoint does not move.
 *
 * <p>One round:
 * <ol>
 *   <li>releases the previous round's caches for this table (a transformer has no hook after the commit);</li>
 *   <li>checks the table's version; no model and no compute setting; no Spark speculation; the prep extension allow-list; the
 *   preprocessors' configuration. The checks run here, at the start of the round: after the streamer fetched the batch and
 *   started the instant, before any prep work; a refusal rolls the instant back;</li>
 *   <li>takes the output schema: an existing table's latest writer schema without meta fields, its columns beyond the base ones
 *   nullable, else {@link EmbeddingTableSchema#build};</li>
 *   <li>prepares the batch ({@link PrepRound}), persisted and counted;</li>
 *   <li>returns the prepared rows.</li>
 * </ol>
 * A row carrying {@code _corrupt_record} goes to the job's error table ({@code CUSTOM_TRANSFORMER_FAILURE}); with the error
 * table off a failed file fails the round. Every cache of a round is at
 * {@code hoodie.write.tagged.record.storage.level}; the prep output and the balancer's assignment stay until the table's next
 * round.
 */
public class EmbeddingTableTransformer implements InstantAwareTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddingTableTransformer.class);
  /** The prefix of every compute setting. */
  private static final String COMPUTE_PREFIX = EmbeddingTableConfig.PREFIX + "compute.";

  /** The caches of each table's last round, released at the table's next round. */
  private static final ConcurrentMap<String, RoundCaches> ROUND_CACHES = new ConcurrentHashMap<>();
  /** Driver-only cache ownership, guarded by this transformer's apply/close monitor. */
  private final Map<String, RoundCaches> ownedRoundCaches = new LinkedHashMap<>();

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
    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration());
    // the table first: its minimum version
    PrepRound.checkTable(config, storageConf);
    Option<StructType> table = EmbeddingTableSchema.writerReadSchema(target, storageConf);
    checkRound(config, sparkSession);
    StructType tableSchema = outputSchema(config, table);
    try {
      return round(jsc, sparkSession, rowDataset, config, storageConf, tableSchema, instantTime);
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

  /** The caches a round leaves: the prep output and the balancer's assignment. */
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
   */
  static void checkRound(EmbeddingTableConfig config, SparkSession spark) {
    checkJob(config);
    PrepRound.checkNoSpeculation(spark.sparkContext().getConf());

  }

  /** Checks property-only mode, model and preprocessor settings before source or table construction. */
  public static Option<EmbeddingModelSpec> checkJob(EmbeddingTableConfig config) {
    TypedProperties props = config.getProps();
    if (config.computes()) {
      throw new IllegalArgumentException("Config " + EmbeddingTableConfig.MODE.key() + "=" + EmbeddingTableConfig.MODE_PREP_AND_COMPUTE
          + " is not supported; use " + EmbeddingTableConfig.MODE_PREP_ONLY + ". Nothing was written");
    }
    checkNoModelOrComputeSettings(config);

    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    try {
      PrepRound.checkPrepFileExtensions(config, registry);
      registry.validateConfig(props, TokenizerFactory.create(props));
    } finally {
      registry.close();
    }
    config.prepSourceRetry();

    return Option.empty();
  }

  /**
   * Under {@code prep_only} no model and no compute setting is set: no {@link EmbeddingTableConfig#MODELS} or
   * {@link EmbeddingTableConfig#ACTIVE_MODELS}, no per-model {@code hoodie.unstructured.embedding.model.<alias>.*} key (its
   * column, dimensions, provider, name, client class or client settings) and no {@code hoodie.unstructured.embedding.compute.*}
   * key. Its table has no VECTOR column.
   *
   * @throws IllegalArgumentException naming every model and compute key set
   */
  static void checkNoModelOrComputeSettings(EmbeddingTableConfig config) {
    List<String> refused = config.getProps().stringPropertyNames().stream()
        .filter(EmbeddingTableTransformer::isModelOrComputeKey).sorted().collect(Collectors.toList());
    if (!refused.isEmpty()) {
      throw new IllegalArgumentException("Config " + EmbeddingTableConfig.MODE.key() + "=" + EmbeddingTableConfig.MODE_PREP_ONLY
          + " declares no model and computes no embedding, but model or compute settings are set: " + refused + ". Remove them. "
          + "Nothing was written");
    }
  }

  private static boolean isModelOrComputeKey(String key) {
    return key.equals(EmbeddingTableConfig.MODELS.key()) || key.equals(EmbeddingTableConfig.ACTIVE_MODELS.key())
        || key.startsWith(EmbeddingTableConfig.MODEL_PREFIX) || key.startsWith(COMPUTE_PREFIX);
  }

  /**
   * The schema of the rows a round of {@code config} writes on the table whose latest writer schema is {@code table} (empty for a
   * new table): {@link #outputSchema}.
   */
  public static StructType roundSchema(EmbeddingTableConfig config, Option<StructType> table) {
    return outputSchema(config, table);
  }

  /**
   * The schema of the rows the round writes: the latest writer schema of an existing table ({@code table}) without its meta
   * fields, every column beyond the base ones nullable (prep writes null in a column it does not make); else
   * {@link EmbeddingTableSchema#build}.
   */
  static StructType outputSchema(EmbeddingTableConfig config, Option<StructType> table) {
    if (!table.isPresent()) {
      return EmbeddingTableSchema.build(config);
    }
    // every column of the table is in the round's rows, so the write keeps the table's schema; one beyond the base columns (a path
    // partition, a column a customer's transformer added) is nullable, since prep writes null in a column it does not make. A
    // customer's transformer that adds its column again in the round wins over that null (UnstructuredSchemaGuard)
    List<StructField> fields = Arrays.stream(table.get().fields())
        .filter(field -> !HoodieRecord.HOODIE_META_COLUMNS.contains(field.name()))
        .map(field -> EmbeddingTableSchema.BASE_COLUMNS.contains(field.name()) || field.nullable() ? field
            : new StructField(field.name(), field.dataType(), true, field.metadata()))
        .collect(Collectors.toList());
    return DataTypes.createStructType(fields);
  }

  // ------------------------------------------------------------------------------------------------------------ the round

  private Dataset<Row> round(JavaSparkContext jsc, SparkSession spark, Dataset<Row> blobRows, EmbeddingTableConfig config,
                             StorageConfiguration<?> storageConf, StructType tableSchema, String instant) {
    TypedProperties props = config.getProps();
    String target = config.targetTablePath();
    StructType outputSchema = tableSchema.add(DataTypes.createStructField(PrepRound.CORRUPT_RECORD, DataTypes.StringType, true));
    StorageLevel level = EmbeddingTableConfig.persistLevel(props);
    RoundCaches caches = new RoundCaches();
    try {
      PrepRound.Prepared prepared = PrepRound.prepare(spark, blobRows, config, storageConf, tableSchema, outputSchema, instant, level);
      caches.outputs.add(prepared.output);
      caches.balance = prepared.balance;
      retainRound(target, caches);
      return routed(prepared.output, props);
    } catch (Exception e) {
      // a failed Spark job surfaces as a checked SparkException: the round's caches are released for it too
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
}
