/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.hash.BucketIndexUtil;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Partitioned record index input partitioner, which is aligned with the mapping of record key
 * to the file group of record index partition in metadata table. It prevents multiple
 * index write subtasks from writing the same record index file group, thereby effectively
 * reducing the number of small files.
 *
 * <p>Partitioned RLI stores record-index file groups under the corresponding data table
 * partition. Since RLI file groups for different data partitions can have the same bucket id,
 * shuffling only by bucket id only would route those equally numbered buckets to the same
 * downstream task and can easily cause data skew. Therefore an index row must be routed by
 * its data partition first, then by the record-key hash within that partition's RLI file
 * group range.
 */
public class RecordIndexPartitioner implements Partitioner<HoodieKey> {
  private final Configuration conf;
  private Map<String, Integer> partitionedRLIFileGroupCounts;
  private Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc;

  /**
   * Creates a partitioner for partitioned RLI index writes.
   *
   * @param conf Flink write configuration
   */
  public RecordIndexPartitioner(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Routes an index row by data partition and record-key-derived partitioned RLI file group.
   *
   * @param recordKey index row key containing both record key and data partition path
   * @param numPartitions downstream index writer parallelism
   * @return downstream subtask index
   */
  @Override
  public int partition(HoodieKey recordKey, int numPartitions) {
    if (partitionedRLIFileGroupCounts == null) {
      partitionedRLIFileGroupCounts = getPartitionedRLIFileGroupCounts();
    }
    int fileGroupCount = getFileGroupCountForPartitionedRLI(recordKey.getPartitionPath());
    int fgIndex = HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(recordKey.getRecordKey(), fileGroupCount);
    if (partitionIndexFunc == null) {
      partitionIndexFunc = BucketIndexUtil.getPartitionIndexFunc(numPartitions);
    }
    return partitionIndexFunc.apply(fileGroupCount, recordKey.getPartitionPath(), fgIndex);
  }

  /**
   * Get the file group count for each data partition in the partitioned record index.
   */
  private Map<String, Integer> getPartitionedRLIFileGroupCounts() {
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    if (!metaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)) {
      return Collections.emptyMap();
    }
    try (HoodieTableMetadata metadataTable = metaClient.getTableFormat().getMetadataFactory().create(
        HoodieFlinkEngineContext.DEFAULT,
        metaClient.getStorage(),
        StreamerUtil.metadataConfig(conf),
        conf.get(FlinkOptions.PATH))) {
      Map<String, Integer> fileGroupCounts = new HashMap<>();
      metadataTable.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX)
          .forEach((partitionPath, fileSlices) -> fileGroupCounts.put(partitionPath, fileSlices.size()));
      return fileGroupCounts;
    } catch (Exception e) {
      throw new HoodieException("Failed to get file group counts for partitioned record index.", e);
    }
  }

  /**
   * Get the partitioned record index file group count for the given data partition.
   */
  private int getFileGroupCountForPartitionedRLI(String partitionPath) {
    int fileGroupCount = partitionedRLIFileGroupCounts.getOrDefault(partitionPath, 0);
    // HoodieBackedTableMetadataWriter initializes record-index file groups for a newly seen
    // data partition with RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP, so the writer-side
    // partitioner should use the same count before that partition appears in the MDT view.
    return fileGroupCount > 0 ? fileGroupCount : getMinFileGroupCountForPartitionedRLI();
  }

  /**
   * Get the minimum file group count used to initialize newly seen partitioned record index partitions.
   */
  private int getMinFileGroupCountForPartitionedRLI() {
    return Integer.parseInt(conf.getString(
        HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(),
        HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.defaultValue().toString()));
  }
}
