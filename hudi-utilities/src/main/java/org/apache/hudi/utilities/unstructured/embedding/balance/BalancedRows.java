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

import org.apache.hudi.common.util.Option;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Output of {@link RowBalancingStrategy#balance}: the balanced blob rows, plus, for an {@link AssignmentBalancingStrategy}, the
 * persisted assignment they are joined from. The rows read the assignment when they are computed (and again if a lost shuffle
 * output is recomputed), so the assignment stays persisted until {@link #close()} to save computation cost. Executor loss
 * may recompute the assignment from lineage; {@link RowBalancingStrategy}'s determinism rule keeps retries consistent.
 * Close it once the rows have been consumed.
 */
public final class BalancedRows implements AutoCloseable {

  private final JavaRDD<Row> rows;
  private final Option<Dataset<Row>> assignment;

  /** Rows a strategy spread itself, with no assignment to release. */
  public BalancedRows(JavaRDD<Row> rows) {
    this.rows = rows;
    this.assignment = Option.empty();
  }

  /** Rows joined from {@code assignment}, which this object unpersists on {@link #close()}. */
  public BalancedRows(JavaRDD<Row> rows, Dataset<Row> assignment) {
    this.rows = rows;
    this.assignment = Option.of(assignment);
  }

  /**
   * @return the blob rows (all columns, {@code blob} included), Spark partition k holding exactly the rows the strategy put in k
   */
  public JavaRDD<Row> getRows() {
    return rows;
  }

  /**
   * @return the persisted assignment the rows are joined from, the source version (sha, modification time, size) and
   *     {@code _balance_partition}; present only for an {@link AssignmentBalancingStrategy}
   */
  public Option<Dataset<Row>> getAssignment() {
    return assignment;
  }

  /** Unpersists the assignment if there is one, else does nothing; the rows must not be computed afterwards. */
  @Override
  public void close() {
    if (assignment.isPresent()) {
      assignment.get().unpersist(false);
    }
  }
}
