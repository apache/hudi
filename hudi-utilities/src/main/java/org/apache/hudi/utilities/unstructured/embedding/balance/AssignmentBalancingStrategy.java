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

package org.apache.hudi.utilities.unstructured.embedding.balance;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/**
 * A {@link RowBalancingStrategy} that computes a global plan from the batch's metadata and applies it exactly. A subclass
 * supplies {@link #assign}: it sees the batch WITHOUT its {@code blob} column, so it can use any metadata (file type, size,
 * partition) but never the bytes, and returns an explicit (source_path_sha, source_modification_time, source_file_size) →
 * partition assignment. {@link #balance} is fixed: it trusts the assignment to name every input version exactly once with a
 * non-null INT partition in [0, numPartitions), a contract each subclass's tests hold it to. The assignment is persisted
 * at the configured tagged-record level ({@link EmbeddingTableConfig#persistLevel}) and computed into that cache before the join
 * is planned, so the join is planned on the assignment's real size. Persistence saves computation cost; an executor loss
 * can recompute the assignment, so {@link RowBalancingStrategy}'s determinism contract keeps retries consistent. The assignment is joined back to the full rows, and the rows are shuffled once with an
 * {@link ExactPartitioner}, so Spark partition k holds exactly the rows assigned to k, the way
 * {@code HoodieSparkEngineContext#mapGroupsByKey} keys values by their target group and partitions on it. The join carries no
 * hint: the persisted assignment is broadcast automatically when it is small (under {@code spark.sql.autoBroadcastJoinThreshold}),
 * else joined by shuffle, so a large batch never forces it onto the driver. The assignment stays persisted until the returned
 * {@link BalancedRows} is closed.
 *
 * <p>Only the final join reads blob bytes, once: the metadata the strategy plans on is a projection of the blob rows without
 * {@code blob}, which the source prunes to metadata columns.
 */
public abstract class AssignmentBalancingStrategy implements RowBalancingStrategy {

  private static final long serialVersionUID = 1L;

  /**
   * The batch may be EMPTY, so an implementation must return an assignment for zero rows, never fail on one.
   *
   * @param rowsWithoutBlob the incremental batch with the {@code blob} column dropped, possibly empty
   * @param writerProps     the job's properties
   * @return the number of partitions and one (source_path_sha, source_modification_time, source_file_size, partition) row per input row
   */
  public abstract BalancingAssignment assign(Dataset<Row> rowsWithoutBlob, TypedProperties writerProps);

  /**
   * Balances {@code blobRows} from their own metadata: {@link #assign} sees the batch without {@code blob}, so it never reads or
   * shuffles the bytes.
   *
   * @return the balanced rows and the persisted assignment they are joined from; close it once the rows are consumed
   */
  @Override
  public final BalancedRows balance(Dataset<Row> blobRows, TypedProperties props) {
    BalancingAssignment assignment = assign(blobRows.drop(BlobColumns.BLOB), props);
    Dataset<Row> assigned = assignment.getAssignment().persist(EmbeddingTableConfig.persistLevel(props));
    BalancedRows balanced = null;
    // by design: a Dataset is not closeable; the finally releases the cached assignment only when no BalancedRows owns it
    try {
      // fills the cache, so the join below is planned on the assignment's real size, with or without adaptive execution
      assigned.count();
      StructType rowSchema = blobRows.schema();
      String strategy = getClass().getName();
      balanced = new BalancedRows(joinAssignment(blobRows, assigned).javaRDD()
          .mapToPair(row -> new Tuple2<>(assignedPartition(row, strategy), row))
          .partitionBy(new ExactPartitioner(assignment.getNumPartitions()))
          .values()
          .map(row -> dropLast(row, rowSchema)), assigned);
      return balanced;
    } finally {
      if (balanced == null) {
        // no BalancedRows reaches the caller to release the assignment; a failed Spark job is a checked SparkException
        assigned.unpersist(false);
      }
    }
  }

  /**
   * Joins the assignment back to the full blob rows on the version columns, null-safe, with no join hint: Spark broadcasts it
   * automatically when its size is under {@code spark.sql.autoBroadcastJoinThreshold}.
   *
   * @return the blob rows (all columns), then their {@code _balance_partition}
   */
  public static Dataset<Row> joinAssignment(Dataset<Row> blobRows, Dataset<Row> assignment) {
    Dataset<Row> source = blobRows.alias("balance_source");
    Dataset<Row> assigned = assignment.alias("balance_assignment");
    Column matches = functions.lit(true);
    for (String column : BalancingAssignment.KEY_COLUMNS) {
      // null-safe: a row with a null version column keeps its assignment instead of being dropped without a trace
      matches = matches.and(source.col(column).eqNullSafe(assigned.col(column)));
    }
    // source columns keep their original layout
    return source.join(assigned, matches, "left").select(source.col("*"), assigned.col(BalancingAssignment.PARTITION));
  }

  /** The partition the assignment gives {@code row}; refuses a version the assignment does not name. */
  private static int assignedPartition(Row row, String strategy) {
    int last = row.length() - 1;
    if (row.isNullAt(last)) {
      StringBuilder version = new StringBuilder();
      for (String column : BalancingAssignment.KEY_COLUMNS) {
        version.append(version.length() == 0 ? "" : ", ").append(column).append('=').append((Object) row.getAs(column));
      }
      throw new HoodieException(strategy + " assigned no partition to the version (" + version + "): an assignment must "
          + "name every input version once");
    }
    return row.getInt(last);
  }

  private static Row dropLast(Row row, StructType schema) {
    Object[] values = new Object[row.length() - 1];
    for (int i = 0; i < values.length; i++) {
      values[i] = row.get(i);
    }
    return new GenericRowWithSchema(values, schema);
  }
}
