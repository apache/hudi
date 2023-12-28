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

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;

/**
 * A factory to generate built-in partitioner to repartition input records into at least
 * expected number of output spark partitions for bulk insert operation.
 */
public abstract class BulkInsertInternalPartitionerFactory {

  public static BulkInsertPartitioner get(HoodieTable table,
                                          HoodieWriteConfig config) {
    return get(table, config, false);
  }

  public static BulkInsertPartitioner get(HoodieTable table,
                                          HoodieWriteConfig config,
                                          boolean enforceNumOutputPartitions) {
    if (config.getIndexType().equals(HoodieIndex.IndexType.BUCKET)) {
      if (config.getBucketIndexEngineType().equals(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING)) {
        return new RDDConsistentBucketBulkInsertPartitioner(table);
      } else if (config.getBucketIndexEngineType().equals(HoodieIndex.BucketIndexEngineType.SIMPLE)) {
        return new RDDSimpleBucketBulkInsertPartitioner(table);
      }
    }
    return get(config, table.isPartitioned(), enforceNumOutputPartitions);
  }

  public static BulkInsertPartitioner get(HoodieWriteConfig config,
                                          boolean isTablePartitioned,
                                          boolean enforceNumOutputPartitions) {
    BulkInsertSortMode sortMode = config.getBulkInsertSortMode();

    switch (sortMode) {
      case NONE:
        return new NonSortPartitioner(enforceNumOutputPartitions);
      case GLOBAL_SORT:
        return new GlobalSortPartitioner(config);
      case PARTITION_SORT:
        return new RDDPartitionSortPartitioner(config);
      case PARTITION_PATH_REPARTITION:
        return new PartitionPathRepartitionPartitioner(isTablePartitioned, config);
      case PARTITION_PATH_REPARTITION_AND_SORT:
        return new PartitionPathRepartitionAndSortPartitioner(isTablePartitioned, config);
      default:
        throw new HoodieException("The bulk insert sort mode \"" + sortMode.name() + "\" is not supported.");
    }
  }
}
