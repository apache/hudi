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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;

public abstract class BulkInsertInternalPartitioner<T extends HoodieRecordPayload> implements
    UserDefinedBulkInsertPartitioner<T> {

  public static BulkInsertInternalPartitioner get(BulkInsertSortMode sortMode) {
    switch (sortMode) {
      case NONE:
        return new NonSortPartitioner();
      case GLOBALLY_SORTED:
        return new GlobalSortPartitioner();
      case PARTITION_SORTED:
        return new RDDPartitionLocalSortPartitioner();
      case PARTITION_RANGE_SORTED:
        return new RDDPartitionRangePartitioner();
      default:
        throw new HoodieException(
            "The bulk insert mode \"" + sortMode.name() + "\" is not supported.");
    }
  }

  public enum BulkInsertSortMode {
    NONE,
    GLOBALLY_SORTED,
    PARTITION_SORTED,
    PARTITION_RANGE_SORTED
  }
}
