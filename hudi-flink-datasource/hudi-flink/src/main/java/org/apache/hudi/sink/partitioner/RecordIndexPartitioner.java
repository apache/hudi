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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;

/**
 * Record index input partitioner, which is aligned with the mapping of record key
 * to the file group of record index partition in metadata table. It prevents multiple
 * index write subtasks from writing the same record index file group, thereby effectively
 * reducing the number of small files.
 */
public class RecordIndexPartitioner implements Partitioner<HoodieKey> {
  private final Configuration conf;
  /**
   * The number of file groups for record index partition in metadata data table. The number
   * cannot be calculated during compiling the writing pipeline, since the hoodie table may
   * not be created yet, so the number is lazily calculated during job running.
   */
  private int numFileGroupsForRecordIndexPartition = -1;

  public RecordIndexPartitioner(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int partition(HoodieKey recordKey, int numPartitions) {
    // initialize numFileGroupsForRecordIndexPartition lazily.
    if (numFileGroupsForRecordIndexPartition < 0) {
      numFileGroupsForRecordIndexPartition = getNumFileGroupsForRecordIndexPartition();
    }
    // note: the hashing is in line with GLOBAL_RECORD_LEVEL_INDEX currently.
    // if partitioned record level index is supported, the partition path should be considered here as well.
    int fgIndex = HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(recordKey.getRecordKey(), numFileGroupsForRecordIndexPartition);
    return fgIndex % numPartitions;
  }

  /**
   * Get the number of file groups for record index partition in metadata table.
   */
  private int getNumFileGroupsForRecordIndexPartition() {
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    HoodieTableMetadata metadataTable = metaClient.getTableFormat().getMetadataFactory().create(
        HoodieFlinkEngineContext.DEFAULT,
        metaClient.getStorage(),
        StreamerUtil.metadataConfig(conf),
        conf.get(FlinkOptions.PATH));
    return metadataTable.getNumFileGroupsForPartition(MetadataPartitionType.RECORD_INDEX);
  }
}