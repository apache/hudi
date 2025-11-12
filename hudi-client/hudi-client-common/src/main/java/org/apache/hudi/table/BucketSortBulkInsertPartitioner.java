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

package org.apache.hudi.table;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;

/**
 * Bucket-Index based Bulk Insert Partitioner and provides the unified sorting logic
 */
public abstract class BucketSortBulkInsertPartitioner<T> implements BulkInsertPartitioner<T> {

  protected final String[] sortColumnNames;

  protected final HoodieTable table;

  public BucketSortBulkInsertPartitioner(HoodieTable table, String sortString) {
    this.table = table;
    if (!StringUtils.isNullOrEmpty(sortString)) {
      this.sortColumnNames = sortString.split(",");
    } else {
      this.sortColumnNames = null;
    }
  }

  /**
   * Check if the records can be sorted by custom columns
   */
  protected boolean isCustomSorted() {
    return sortColumnNames != null && sortColumnNames.length > 0;
  }

  /**
   * Check if the records can be sorted by record key
   */
  protected boolean isRecordKeySorted() {
    return table.requireSortedRecords() || table.getConfig().getBulkInsertSortMode() != BulkInsertSortMode.NONE;
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return isCustomSorted() || isRecordKeySorted();
  }

}
