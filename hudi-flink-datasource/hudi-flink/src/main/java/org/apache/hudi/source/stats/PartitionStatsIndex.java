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

package org.apache.hudi.source.stats;

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.source.prune.ColumnStatsProbe;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;

/**
 * An index support implementation that leverages Partition Stats Index to prune partitions.
 */
public class PartitionStatsIndex extends FileStatsIndex {
  private static final long serialVersionUID = 1L;

  public PartitionStatsIndex(
      String basePath,
      RowType tableRowType,
      Configuration conf,
      @Nullable HoodieTableMetaClient metaClient) {
    super(basePath, tableRowType, conf, metaClient);
  }

  @Override
  public String getIndexPartitionName() {
    return HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS;
  }

  @Override
  public Set<String> computeCandidateFiles(ColumnStatsProbe probe, List<String> allFiles) {
    throw new UnsupportedOperationException("This method is not supported by " + this.getClass().getSimpleName());
  }

  /**
   * NOTE: The stats payload stored in Metadata table for Partition Stats Index
   * is {@link HoodieMetadataColumnStats}}, with schema:
   *
   * <pre>
   *   |- partition_name: string
   *   |- min_val: row
   *   |- max_val: row
   *   |- null_cnt: long
   *   |- val_cnt: long
   *   |- column_name: string
   * </pre>
   * Thus, the loading/transposing and candidates computing logic can be reused.
   *
   * @param probe         Column stats probe constructed from pushed down column filters.
   * @param allPartitions All partitions to be pruned by partition stats.
   *
   * @return the candidate partitions pruned by partition stats.
   */
  @Override
  public Set<String> computeCandidatePartitions(ColumnStatsProbe probe, List<String> allPartitions) {
    return super.computeCandidateFiles(probe, allPartitions);
  }
}
