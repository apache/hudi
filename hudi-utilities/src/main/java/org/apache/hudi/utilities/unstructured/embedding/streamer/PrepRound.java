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
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.streamer.BaseErrorTableWriter;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingPrepFailedException;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableChecks;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.balance.BalancedRows;
import org.apache.hudi.utilities.unstructured.embedding.balance.RowBalancer;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepCounters;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepSideOutput;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessingIterator;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * The prep half of an embedding round ({@link EmbeddingTableTransformer}): turns the blob rows of the round's batch into
 * embedding-table rows, inside the round's instant. The source batch is read once, into a cache every later stage reads.
 * <ol>
 *   <li>under {@link EmbeddingTableConfig#PREP_FILE_EXTENSIONS}, drops the rows of other extensions;</li>
 *   <li>caches the batch, bytes included (the source builds its rows in an opaque task, so every other action on it would read
 *   each inline object again) until preprocessing has run, and counts its rows in one action;</li>
 *   <li>balances the rows over tasks ({@link RowBalancer}; the source builds one row per object path, so each row is its own
 *   source version) and preprocesses them ({@link PreprocessingIterator}: the default
 *   chunk-id scheme, a failed chunk 0 fails its file-modality and a failed later chunk only itself);</li>
 *   <li>persists the output at {@code hoodie.write.tagged.record.storage.level} and counts it, so preprocessing runs inside
 *   the round, where a failure rolls the instant back.</li>
 * </ol>
 * A failed file, chunk or empty file is a row whose {@value #CORRUPT_RECORD} column holds a JSON description
 * ({@code source_path_sha, source_modification_time, source_file_size, source_file_path, modality, chunk_id, stage, error,
 * instant}); the streamer routes it
 * to the job's error table as a {@code CUSTOM_TRANSFORMER_FAILURE}. With the error table off
 * ({@code hoodie.errortable.enable=false}) a failed file or chunk fails the round, and empty files are only logged.
 */
final class PrepRound {

  private static final Logger LOG = LoggerFactory.getLogger(PrepRound.class);
  /** The column the streamer routes to the error table. */
  static final String CORRUPT_RECORD = BaseErrorTableWriter.ERROR_TABLE_CURRUPT_RECORD_COL_NAME;

  private PrepRound() {
  }

  /** What prep leaves for the rest of the round: its persisted output (data and error rows) and the balancer's assignment. */
  static final class Prepared {
    final Dataset<Row> output;
    final BalancedRows balance;

    Prepared(Dataset<Row> output, BalancedRows balance) {
      this.output = output;
      this.balance = balance;
    }
  }

  /**
   * Embedding prep does not support speculative Spark execution.
   *
   * @throws IllegalArgumentException when {@code spark.speculation} is true
   */
  static void checkNoSpeculation(SparkConf conf) {
    if (conf.getBoolean("spark.speculation", false)) {
      throw new IllegalArgumentException("spark.speculation=true is not supported by embedding prep. Nothing was written");
    }
  }

  /**
   * The prep extension allow-list ({@link EmbeddingTableConfig#PREP_FILE_EXTENSIONS}) names only extensions a preprocessor is
   * mapped to.
   *
   * @throws IllegalArgumentException naming the key and the extensions without a preprocessor
   */
  static void checkPrepFileExtensions(EmbeddingTableConfig config, PreprocessorRegistry registry) {
    Option<List<String>> extensions = config.prepFileExtensions();
    if (!extensions.isPresent()) {
      return;
    }
    List<String> unmapped = extensions.get().stream().filter(extension -> !registry.forExtension(extension).isPresent())
        .collect(Collectors.toList());
    if (!unmapped.isEmpty()) {
      throw new IllegalArgumentException("Config " + EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key() + " lists " + unmapped
          + ", which no preprocessor is mapped to (map one with " + EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "<ext>"
          + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX + ", or remove it from the list). Nothing was written");
    }
  }

  /**
   * Prepares {@code blobRows} at {@code instant}. The returned output has {@code outputSchema} (the table schema plus
   * {@value #CORRUPT_RECORD}), is persisted at {@code level} and has been counted. A batch without rows returns an empty,
   * unpersisted output and no balance.
   */
  static Prepared prepare(SparkSession spark, Dataset<Row> blobRows, EmbeddingTableConfig config, StorageConfiguration<?> storageConf,
                          StructType tableSchema, StructType outputSchema, String instant, StorageLevel level) {
    TypedProperties props = config.getProps();
    String target = config.targetTablePath();
    // under the prep extension allow-list, the rows of other extensions are dropped (lazily): no row, no error record
    Option<List<String>> extensions = config.prepFileExtensions();
    if (extensions.isPresent()) {
      blobRows = blobRows.filter(isListed(extensions.get()));
    }
    // the batch, bytes included, cached; its count is the one read of the source, which fills the cache
    Dataset<Row> batch = blobRows.persist(level);
    BalancedRows balanced = null;
    Dataset<Row> output = null;
    try {
      long rows = batch.count();
      if (rows == 0L) {
        batch.unpersist(false);
        return new Prepared(spark.createDataFrame(Collections.emptyList(), outputSchema), null);
      }
      PrepCounters counters = PrepCounters.register(spark.sparkContext());
      // the strategy spreads the batch's rows; an assignment strategy plans on the batch without blob, and only its join reads the bytes
      balanced = RowBalancer.balance(batch, RowBalancer.strategyFrom(props), props);
      // preprocessing over the balanced rows, read from the cached batch
      JavaRDD<Row> prepared = balanced.getRows().mapPartitions(preprocessFunction(config, instant, storageConf, counters,
          layoutOf(outputSchema, EmbeddingTableSchema.build(config)), outputSchema));
      output = spark.createDataFrame(prepared, outputSchema).persist(level);
      // the action that runs preprocessing, inside the round, where a failure rolls the instant back
      long outputRows = output.count();
      // nothing reads the batch after preprocessing: the rest of the round reads the persisted output
      batch.unpersist(false);
      LOG.info("Embedding prep of {} at instant {}: {} output row(s); files ok {}, failed {}, no preprocessor {}, no text {}; "
              + "chunks {}", target, instant, outputRows, counters.getFilesOk().value(), counters.getFilesFailed().value(),
          counters.getFilesNoPreprocessor().value(), counters.getFilesNoText().value(), counters.getChunks().value());
      if (!errorTableEnabled(props)) {
        failOnFailedRows(output, config);
      }
      return new Prepared(output, balanced);
    } catch (Exception e) {
      // a failed Spark job surfaces as a checked SparkException: wrapped, so the streamer's rollback of the instant runs
      batch.unpersist(false);
      if (output != null) {
        output.unpersist(false);
      }
      if (balanced != null) {
        balanced.close();
      }
      throw e instanceof RuntimeException ? (RuntimeException) e
          : new HoodieException("Embedding prep of " + target + " failed at instant " + instant, e);
    }
  }

  /** A blob row whose lower-case extension is listed; a row without an extension is not. */
  private static Column isListed(List<String> extensions) {
    return functions.coalesce(functions.lower(functions.col(BlobColumns.SOURCE_FILE_EXTENSION)).isin(extensions.toArray()),
        functions.lit(false));
  }

  // ---------------------------------------------------------------------------------------------------------------- preprocessing

  /**
   * For each output column, the index of the same-named column in the iterator's row ({@code iteratorSchema}), or -1: a column
   * only the table has is NULL. The last output column is {@value #CORRUPT_RECORD}.
   */
  @VisibleForTesting
  static int[] layoutOf(StructType outputSchema, StructType iteratorSchema) {
    List<String> iteratorColumns = Arrays.asList(iteratorSchema.fieldNames());
    return Arrays.stream(outputSchema.fieldNames()).mapToInt(iteratorColumns::indexOf).toArray();
  }

  private static FlatMapFunction<Iterator<Row>, Row> preprocessFunction(
      EmbeddingTableConfig config, String instant, StorageConfiguration<?> storageConf,
      PrepCounters counters, int[] layout, StructType outputSchema) {
    return blobRows -> outputRows(blobRows, config,
        new BlobBytesSource(storageConf, config.prepSourceRetry()), new PreprocessorRegistry(config.getProps()),
        TokenizerFactory.create(config.getProps()), counters, instant, outputSchema, layout);
  }

  /**
   * The rows of one partition in the output layout, including failed or empty files as error rows. This method owns the
   * side-output queue and constructs the preprocessing iterator with that queue as its sink.
   */
  @VisibleForTesting
  static Iterator<Row> outputRows(Iterator<Row> blobRows, EmbeddingTableConfig config, BlobBytesSource bytesSource,
                                  PreprocessorRegistry registry, Tokenizer tokenizer, PrepCounters counters,
                                  String instant, StructType outputSchema, int[] layout) {
    Deque<PrepSideOutput> sideOutputs = new ArrayDeque<>();
    PreprocessingIterator rows = new PreprocessingIterator(blobRows, config, bytesSource, registry, tokenizer, counters,
        sideOutputs::add);
    return new Iterator<Row>() {
      private boolean ready() {
        return !sideOutputs.isEmpty() || rows.advanceToRowOrSideOutput() || !sideOutputs.isEmpty();
      }

      @Override
      public boolean hasNext() {
        return ready();
      }

      @Override
      public Row next() {
        if (!ready()) {
          throw new NoSuchElementException();
        }
        return !sideOutputs.isEmpty() ? errorRow(sideOutputs.poll(), instant, outputSchema) : outputRow(rows.next(), layout);
      }
    };
  }

  /** The iterator's row in the output layout, {@value #CORRUPT_RECORD} NULL. */
  private static Row outputRow(Row row, int[] layout) {
    Object[] values = new Object[layout.length];
    for (int i = 0; i < layout.length; i++) {
      values[i] = layout[i] < 0 ? null : row.get(layout[i]);
    }
    return RowFactory.create(values);
  }

  /**
   * The error row of a failed or empty file or chunk: {@value #CORRUPT_RECORD} holds its JSON; every other column NULL, or a
   * placeholder of its type where the column is not nullable (the streamer drops the row before the write).
   */
  static Row errorRow(PrepSideOutput sideOutput, String instant, StructType outputSchema) {
    Object[] values = new Object[outputSchema.size()];
    StructField[] fields = outputSchema.fields();
    for (int i = 0; i < fields.length - 1; i++) {
      values[i] = fields[i].nullable() ? null : placeholder(fields[i].dataType());
    }
    values[fields.length - 1] = errorJson(sideOutput, instant);
    return RowFactory.create(values);
  }

  /** The value an error-only row carries in a non-nullable column of {@code type}. */
  static Object placeholder(DataType type) {
    if (DataTypes.StringType.equals(type)) {
      return "";
    }
    if (DataTypes.LongType.equals(type)) {
      return 0L;
    }
    if (DataTypes.IntegerType.equals(type)) {
      return 0;
    }
    return null;
  }

  /** The error-table JSON of {@code sideOutput}. */
  static String errorJson(PrepSideOutput sideOutput, String instant) {
    Map<String, Object> json = new LinkedHashMap<>();
    json.put("source_path_sha", sideOutput.getSourcePathSha());
    json.put("source_modification_time", sideOutput.getSourceModificationTime());
    json.put("source_file_size", sideOutput.getSourceFileSize());
    json.put("source_file_path", sideOutput.getSourcePath());
    json.put("modality", sideOutput.getModality());
    json.put("chunk_id", sideOutput.getChunkId());
    json.put("stage", sideOutput.getStage());
    json.put("error", sideOutput.getError());
    json.put("instant", instant);
    try {
      return JsonUtils.getObjectMapper().writeValueAsString(json);
    } catch (JsonProcessingException e) {
      throw new HoodieException("Cannot write the error row of " + sideOutput, e);
    }
  }

  static boolean errorTableEnabled(TypedProperties props) {
    return ConfigUtils.getBooleanWithAltKeys(props, HoodieErrorTableConfig.ERROR_TABLE_ENABLED);
  }

  /**
   * With the error table off nothing records a failed file or chunk, so the round fails; an empty file is only logged.
   *
   * @throws EmbeddingPrepFailedException naming the first failures
   */
  private static void failOnFailedRows(Dataset<Row> output, EmbeddingTableConfig config) {
    Column failed = functions.col(CORRUPT_RECORD).isNotNull()
        .and(functions.get_json_object(functions.col(CORRUPT_RECORD), "$.stage").notEqual(PrepSideOutput.STAGE_EMPTY));
    Dataset<Row> failures = output.filter(failed).select(CORRUPT_RECORD);
    long count = failures.count();
    if (count > 0) {
      List<String> first = failures.limit(5).collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
      throw new EmbeddingPrepFailedException("Embedding prep of " + config.targetTablePath() + " failed: " + count + " failed "
          + "file(s) or chunk(s) and " + HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key() + "=false, so nothing records them; "
          + "the round is rolled back. First: " + first);
    }
  }

  /**
   * Checks the embedding table's minimum version. It runs in the transformer at the start of the round: after the streamer
   * fetched the batch and started the instant, before any prep work; a refusal rolls the instant back.
   */
  static void checkTable(EmbeddingTableConfig config, StorageConfiguration<?> storageConf) {
    String target = config.targetTablePath();
    EmbeddingTableChecks.checkTableVersion(target, storageConf, config);
  }
}
