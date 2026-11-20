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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * The global rank of every row of a batch in a total order, for an assignment that must not depend on the batch's partition
 * order: one distributed sort (range partitioned, so a batch larger than the shuffle's advisory partition size is spread over
 * several tasks rather than ranked in one) and the index of each row in the sorted batch. When {@code order} is a total order
 * over the rows (it ends with key columns that are distinct per row), a row's rank is a function of the batch's rows alone,
 * so the same rows in any partition order, and a recomputation of the ranks after a lost block, give every row the same rank.
 */
public final class SortedRanks {

  private SortedRanks() {
  }

  /**
   * Under adaptive execution, or whenever the sorted batch has more than one partition, runs the sort's range sampling (a pass
   * over {@code rows}), its map stage and the per-partition row count of the sorted batch before it returns, so a caller may
   * release a cached {@code rows} afterwards; otherwise a later compute reads {@code rows} again. The ranks themselves are
   * computed, from the sort's shuffle output, when the returned RDD is.
   *
   * @return each row of {@code rows} with its 0-based rank in {@code order}
   */
  public static JavaPairRDD<Row, Long> of(Dataset<Row> rows, Column... order) {
    return rows.sort(order).javaRDD().zipWithIndex();
  }

  /**
   * @return the assignment schema for {@code rows}: its three source version key columns, with their own types, then
   *     {@link BalancingAssignment#PARTITION}, so the assignment joins back to the rows column for column
   */
  static StructType assignmentSchema(StructType rows) {
    List<StructField> fields = new ArrayList<>();
    for (String key : BalancingAssignment.KEY_COLUMNS) {
      fields.add(rows.apply(key));
    }
    fields.add(DataTypes.createStructField(BalancingAssignment.PARTITION, DataTypes.IntegerType, false));
    return DataTypes.createStructType(fields);
  }
}
