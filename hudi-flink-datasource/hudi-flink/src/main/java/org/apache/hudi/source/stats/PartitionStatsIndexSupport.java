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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.source.prune.DataPruner;

import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Set;

/**
 * An index support implementation that leverages Partition Stats Index to prune partitions.
 */
public class PartitionStatsIndexSupport extends ColumnStatsIndexSupport {
  private static final long serialVersionUID = 1L;

  public PartitionStatsIndexSupport(
      String basePath,
      RowType tableRowType,
      HoodieMetadataConfig metadataConfig) {
    super(basePath, tableRowType, metadataConfig);
  }

  @Override
  public String getIndexName() {
    return HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS;
  }

  @Override
  public Set<String> computeCandidateFiles(DataPruner dataPruner, List<String> allFiles) {
    throw new UnsupportedOperationException("This method is not supported by " + this.getClass().getSimpleName());
  }

  /**
   * NOTE: The stats payload stored in Metadata table for both Partition Stats Index and Column Stats Index
   * is {@link HoodieMetadataColumnStats}}, with schema:
   * |- target_name: string    -- partition name or file name
   * |- min_val: row
   * |- max_val: row
   * |- null_cnt: long
   * |- val_cnt: long
   * |- column_name: string
   * Thus, the loading/transposing and candidates computing logic can be reused.
   *
   * @param dataPruner data pruner constructed from pushed down column filters.
   * @param allPartitions all partitions before pruning by partition stats.
   * @return the candidate partitions after pruning by partition stats.
   */
  @Override
  public Set<String> computeCandidatePartitions(DataPruner dataPruner, List<String> allPartitions) {
    return super.computeCandidateFiles(dataPruner, allPartitions);
  }
}
