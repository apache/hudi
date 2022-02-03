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

import java.io.Serializable;
import java.util.List;

public class MetadataRecordsGenerationParams implements Serializable {

  private final HoodieTableMetaClient dataMetaClient;
  private final List<MetadataPartitionType> enabledPartitionTypes;
  private final String bloomFilterType;
  private final int bloomIndexParallelism;
  private final boolean isAllColumnStatsIndexEnabled;

  MetadataRecordsGenerationParams(HoodieTableMetaClient dataMetaClient, List<MetadataPartitionType> enabledPartitionTypes,
                                  String bloomFilterType, int bloomIndexParallelism, boolean isAllColumnStatsIndexEnabled) {

    this.dataMetaClient = dataMetaClient;
    this.enabledPartitionTypes = enabledPartitionTypes;
    this.bloomFilterType = bloomFilterType;
    this.bloomIndexParallelism = Math.max(bloomIndexParallelism, 1);
    this.isAllColumnStatsIndexEnabled = isAllColumnStatsIndexEnabled;
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

  public boolean isAllColumnStatsIndexEnabled() {
    return isAllColumnStatsIndexEnabled;
  }

  public int getBloomIndexParallelism() {
    return bloomIndexParallelism;
  }
}
