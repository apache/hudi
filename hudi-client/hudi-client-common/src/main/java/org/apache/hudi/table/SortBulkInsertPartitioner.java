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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.SortMarker;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;

public abstract class SortBulkInsertPartitioner<T> implements BulkInsertPartitioner<T> {

  protected final String[] sortColumnNames;

  protected final HoodieTable table;

  public SortBulkInsertPartitioner(HoodieTable table, String sortString) {
    this.table = table;
    if (!StringUtils.isNullOrEmpty(sortString)) {
      this.sortColumnNames = sortString.split(",");
    } else {
      this.sortColumnNames = null;
    }
  }

  protected boolean isCustomSorted() {
    return sortColumnNames != null && sortColumnNames.length > 0;
  }

  protected boolean isRecordKeySorted() {
    return table.requireSortedRecords() || table.getConfig().getBulkInsertSortMode() != BulkInsertSortMode.NONE;
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return isCustomSorted() || isRecordKeySorted();
  }

  public Option<SortMarker> getSortMarker() {
    if (isCustomSorted()) {
      return Option.of(SortMarker.of(sortColumnNames, SortMarker.SortMode.LINEAR, SortMarker.SortOrder.ASC));
    }
    if (isRecordKeySorted()) {
      return Option.of(SortMarker.of(new String[]{HoodieRecord.RECORD_KEY_METADATA_FIELD}, SortMarker.SortMode.LINEAR, SortMarker.SortOrder.ASC));
    }
    return Option.empty();
  }
}
