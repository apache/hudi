/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;
import java.util.List;

/**
 * Encapsulates all parameters required to generate metadata index for enabled index types.
 *
 * @deprecated this component currently duplicates configuration coming from the {@code HoodieWriteConfig}
 *             which is problematic; instead we should break this component down and use source of truth
 *             for each respective data-point directly ({@code HoodieWriteConfig}, {@code HoodieTableMetaClient}, etc)
 */
@Deprecated
public class MetadataRecordsGenerationParams implements Serializable {

  private final HoodieTableMetaClient dataMetaClient;
  private final List<MetadataPartitionType> enabledPartitionTypes;
  private final String bloomFilterType;
  private final int bloomIndexParallelism;
  private final boolean isColumnStatsIndexEnabled;
  private final int columnStatsIndexParallelism;
  private final List<String> targetColumnsForColumnStatsIndex;
  private final List<String> targetColumnsForBloomFilterIndex;
  private final boolean enableBasePathOverride;
  private final Option<String> basePathOverride;

  MetadataRecordsGenerationParams(HoodieTableMetaClient dataMetaClient, List<MetadataPartitionType> enabledPartitionTypes, String bloomFilterType, int bloomIndexParallelism,
                                  boolean isColumnStatsIndexEnabled, int columnStatsIndexParallelism, List<String> targetColumnsForColumnStatsIndex, List<String> targetColumnsForBloomFilterIndex,
                                  boolean enableBasePathOverride, String basePathOverride) {
    this.dataMetaClient = dataMetaClient;
    this.enabledPartitionTypes = enabledPartitionTypes;
    this.bloomFilterType = bloomFilterType;
    this.bloomIndexParallelism = bloomIndexParallelism;
    this.isColumnStatsIndexEnabled = isColumnStatsIndexEnabled;
    this.columnStatsIndexParallelism = columnStatsIndexParallelism;
    this.targetColumnsForColumnStatsIndex = targetColumnsForColumnStatsIndex;
    this.targetColumnsForBloomFilterIndex = targetColumnsForBloomFilterIndex;
    this.enableBasePathOverride = enableBasePathOverride;
    this.basePathOverride = Option.of(basePathOverride);
  }

  public HoodieTableMetaClient getDataMetaClient() {
    return dataMetaClient;
  }

  public List<MetadataPartitionType> getEnabledPartitionTypes() {
    return enabledPartitionTypes;
  }

  public String getBloomFilterType() {
    return bloomFilterType;
  }

  public boolean isColumnStatsIndexEnabled() {
    return isColumnStatsIndexEnabled;
  }

  public int getBloomIndexParallelism() {
    return bloomIndexParallelism;
  }

  public int getColumnStatsIndexParallelism() {
    return columnStatsIndexParallelism;
  }

  public List<String> getTargetColumnsForColumnStatsIndex() {
    return targetColumnsForColumnStatsIndex;
  }

  public List<String> getSecondaryKeysForBloomFilterIndex() {
    return targetColumnsForBloomFilterIndex;
  }

  public boolean shouldEnableBasePathOverride() {
    return enableBasePathOverride;
  }

  public Option<String> getBasePathOverride() {
    return basePathOverride;
  }
}
