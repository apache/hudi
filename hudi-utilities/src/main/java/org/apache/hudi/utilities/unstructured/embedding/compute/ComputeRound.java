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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.streamer.BaseErrorTableWriter;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.balance.HashSpread;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;

import org.apache.spark.api.java.JavaRDD;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * The compute half of an embedding round: spreads the round's prepared rows over the compute tasks, embeds them
 * ({@link MicroBatchEmbeddingIterator}, retries inside) and shapes the rows the round's commit writes.
 * <ul>
 *   <li>{@link #hashPartitionByKey}: the prepared rows hash partitioned by the five key columns over P tasks ({@link HashSpread}), one
 *   shuffle, so task row counts are balanced in expectation and a row's task depends on its own keys and P alone: a retried or
 *   recomputed compute task over the same rows reads the same rows.</li>
 *   <li>{@link #embed}: the model calls, persisted at {@code hoodie.write.tagged.record.storage.level} and counted, so every call
 *   runs inside the round, before the write. Every column of a prepared row is kept as read; the model's column is set.</li>
 *   <li>{@link #dataRows} and {@link #errorRows}: a PERMANENT input keeps its data row (prepared columns, vector NULL), and adds
 *   one error-only row whose {@value #CORRUPT_RECORD} holds the error record ({@code error}, the five key columns, {@code model}) the
 *   streamer routes to the error table. The streamer drops a row with {@value #CORRUPT_RECORD} from the data, so one row could not
 *   carry both.</li>
 * </ul>
 */
public final class ComputeRound {

  /** The column the streamer routes to the error table. */
  public static final String CORRUPT_RECORD = BaseErrorTableWriter.ERROR_TABLE_CURRUPT_RECORD_COL_NAME;
  /**
   * The working column {@link MicroBatchEmbeddingIterator} stores a PERMANENT outcome in: appended to the round's compute
   * schema and dropped before the write, never written to the table. A reserved name outside the model-column namespace.
   */
  public static final String ERROR_WORKING_COLUMN = "_embedding_compute_error";

  /** Fields of the error record of a PERMANENT input, with the five key columns, in key order. */
  static final String ERROR_RECORD_ERROR = "error";
  static final String ERROR_RECORD_MODEL = "model";

  private ComputeRound() {
  }

  /** The nullable string field of {@link #ERROR_WORKING_COLUMN}. */
  public static StructField errorWorkingField() {
    return DataTypes.createStructField(ERROR_WORKING_COLUMN, DataTypes.StringType, true);
  }

  /**
   * Spreads {@code rows} over exactly {@code parallelism} tasks by a hash of the five key columns, with one shuffle of the rows
   * ({@link HashSpread}): partition k holds the rows whose {@code pmod(murmur3(keys), parallelism)} is k. A row's task is a
   * function of its own key columns and {@code parallelism} alone: the same rows in any input partitioning or order, and a
   * recomputation after a lost block that reads the same rows, deal every row to the same task. Task row counts are
   * binomial(N, 1/parallelism), balanced in expectation rather than exactly; adaptive execution never coalesces the tasks.
   */
  public static JavaRDD<Row> hashPartitionByKey(Dataset<Row> rows, int parallelism) {
    if (parallelism <= 0) {
      throw new IllegalArgumentException("The compute parallelism must be positive, got " + parallelism);
    }
    return HashSpread.of(rows, parallelism, EmbeddingTableSchema.KEY_COLUMNS.stream().map(functions::col).toArray(Column[]::new))
        .javaRDD();
  }

  /**
   * Embeds {@code rows} (in {@code tableSchema}) with {@code model}, reading image rows' {@code prepared_image} through
   * {@code storedInputs}: the rows in {@code tableSchema} plus
   * {@link #ERROR_WORKING_COLUMN}, persisted at {@code level} and counted, so every model call has run when it returns.
   */
  public static Dataset<Row> embed(SparkSession spark, JavaRDD<Row> rows, StructType tableSchema, EmbeddingModelSpec model,
                                   TypedProperties props, BlobBytesSource storedInputs, ComputeCounters counters,
                                   StorageLevel level) {
    StructType schema = tableSchema.add(errorWorkingField(tableSchema));
    int batchSize = EmbeddingTableConfig.from(props).computeBatchSize();
    JavaRDD<Row> embedded = rows.mapPartitions(iterator -> new MicroBatchEmbeddingIterator(withErrorSlot(iterator), model, props,
        schema, batchSize, counters, storedInputs));
    Dataset<Row> persisted = spark.createDataFrame(embedded, schema).persist(level);
    try {
      // the action that runs every model call before the write
      persisted.count();
    } catch (Exception e) {
      // a failed Spark job surfaces as a checked SparkException: the output is released for it too
      persisted.unpersist(false);
      throw e instanceof RuntimeException ? (RuntimeException) e
          : new HoodieException("Embedding with model " + model.getName() + " failed", e);
    }
    return persisted;
  }

  /** {@code rows} with one more, NULL value: the slot of {@link #ERROR_WORKING_COLUMN}. */
  private static Iterator<Row> withErrorSlot(Iterator<Row> rows) {
    return new Iterator<Row>() {
      @Override
      public boolean hasNext() {
        return rows.hasNext();
      }

      @Override
      public Row next() {
        Row row = rows.next();
        Object[] values = new Object[row.length() + 1];
        for (int i = 0; i < row.length(); i++) {
          values[i] = row.get(i);
        }
        return RowFactory.create(values);
      }
    };
  }

  /** The embedded rows in {@code outputSchema} (the table schema, then {@value #CORRUPT_RECORD}), every one a data row. */
  public static Dataset<Row> dataRows(Dataset<Row> embedded, StructType outputSchema) {
    List<Column> columns = new ArrayList<>();
    for (StructField field : outputSchema.fields()) {
      Column column = CORRUPT_RECORD.equals(field.name()) ? functions.lit(null).cast(DataTypes.StringType)
          : functions.col(field.name());
      columns.add(column.as(field.name(), field.metadata()));
    }
    return embedded.select(columns.toArray(new Column[0]));
  }

  /**
   * One error-only row per PERMANENT input, in {@code outputSchema}: {@value #CORRUPT_RECORD} is the canonical JSON of
   * {@code error}, the five key columns and {@code model}; every other column NULL, or a placeholder of its type where the column is
   * not nullable (the streamer routes the row to the error table and drops it from the data).
   */
  public static Dataset<Row> errorRows(Dataset<Row> embedded, StructType outputSchema, EmbeddingModelSpec model) {
    Column error = errorColumn(embedded);
    // struct fields in key order: to_json writes them in that order with no whitespace, the canonical spelling
    Column record = functions.to_json(functions.struct(error.as(ERROR_RECORD_ERROR),
        functions.col(EmbeddingTableSchema.SOURCE_PATH_SHA),
        functions.col(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME),
        functions.col(EmbeddingTableSchema.SOURCE_FILE_SIZE),
        functions.col(EmbeddingTableSchema.MODALITY),
        functions.col(EmbeddingTableSchema.CHUNK_ID),
        functions.lit(model.getName()).as(ERROR_RECORD_MODEL)));
    List<Column> columns = new ArrayList<>();
    for (StructField field : outputSchema.fields()) {
      Column column = CORRUPT_RECORD.equals(field.name()) ? record
          : field.nullable() ? functions.lit(null).cast(field.dataType()) : placeholder(field.dataType());
      columns.add(column.as(field.name(), field.metadata()));
    }
    return embedded.filter(error.isNotNull()).select(columns.toArray(new Column[0]));
  }

  private static Column placeholder(DataType type) {
    if (DataTypes.LongType.equals(type)) {
      return functions.lit(0L);
    }
    if (DataTypes.IntegerType.equals(type)) {
      return functions.lit(0);
    }
    return functions.lit("").cast(type);
  }

  /**
   * The refusal of a round with PERMANENT inputs while the error table is off: nothing would record them.
   */
  public static EmbeddingComputePermanentFailureException permanentWithoutErrorTable(EmbeddingModelSpec model, String target,
                                                                                     long permanent) {
    return new EmbeddingComputePermanentFailureException("EMBED-COMPUTE-PERMANENT-FAILURE model=" + model.getName() + " on "
        + target + ": " + permanent + " input(s) failed for good and the error table is off ("
        + HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key() + "), so the round fails and its instant is rolled back");
  }

  private static StructField errorWorkingField(StructType tableSchema) {
    String name = ERROR_WORKING_COLUMN;
    int suffix = 0;
    List<String> columns = Arrays.asList(tableSchema.fieldNames());
    while (containsIgnoreCase(columns, name)) {
      name = ERROR_WORKING_COLUMN + "_" + ++suffix;
    }
    return DataTypes.createStructField(name, DataTypes.StringType, true);
  }

  private static boolean containsIgnoreCase(List<String> columns, String name) {
    return columns.stream().anyMatch(column -> column.equalsIgnoreCase(name));
  }

  public static Column errorColumn(Dataset<Row> embedded) {
    StructField[] fields = embedded.schema().fields();
    return functions.col(fields[fields.length - 1].name());
  }
}
