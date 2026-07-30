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

package org.apache.hudi.sink.bulk.sort;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Utilities for defining and sorting the internal rows used by Flink LSM bulk insert.
 *
 * <p>Each internal row contains a shuffle key, the actual encoded record key, and the original
 * table row. The shuffle key is the partition path for the regular bulk-insert path and the file ID
 * for the bucket-index path. The external sorter orders rows by the shuffle key first and the
 * record key second, so records for each writer route form a record-key-ordered run.
 *
 * <p>The original table row remains nested in the internal row and is not part of the sort key.
 * After sorting, the LSM writer helpers consume the internal row directly and reuse the retained
 * record key.
 */
public final class LsmBulkInsertSortUtils {

  public static final String SHUFFLE_KEY_FIELD = "_shuffle_key";
  public static final String RECORD_KEY_FIELD = "_record_key";
  public static final String RECORD_FIELD = "_record";

  private LsmBulkInsertSortUtils() {
  }

  /**
   * Returns the internal row type passed from LSM key decoration through the external sorter.
   *
   * <p>The fields are ordered as shuffle key, encoded record key, and original table row.
   *
   * @param rowType logical type of the original table row
   * @return logical type of the internal LSM sort row
   */
  public static RowType sortRowType(RowType rowType) {
    LogicalType[] types = new LogicalType[] {
        DataTypes.STRING().getLogicalType(),
        DataTypes.STRING().getLogicalType(),
        rowType
    };
    String[] names = new String[] {SHUFFLE_KEY_FIELD, RECORD_KEY_FIELD, RECORD_FIELD};
    return RowType.of(types, names);
  }

  /**
   * Creates an external sorter ordered by shuffle key and encoded record key.
   *
   * <p>The nested payload is deliberately excluded from the sort keys, so duplicate record keys
   * are retained without comparing or aggregating their payloads.
   *
   * @param sortRowType logical type returned by {@link #sortRowType(RowType)}
   * @return generator for the LSM bulk-insert sort operator
   */
  public static SortOperatorGen getLsmSorterGen(RowType sortRowType) {
    return new SortOperatorGen(sortRowType,
        new String[] {
            SHUFFLE_KEY_FIELD,
            RECORD_KEY_FIELD
        });
  }
}
