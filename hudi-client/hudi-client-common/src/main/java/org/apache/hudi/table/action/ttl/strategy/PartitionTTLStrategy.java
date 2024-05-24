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

package org.apache.hudi.table.action.ttl.strategy;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Strategy for partition-level ttl management.
 */
public abstract class PartitionTTLStrategy implements TTLStrategy, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionTTLStrategy.class);

  protected final HoodieTable hoodieTable;
  protected final HoodieWriteConfig writeConfig;
  protected final String instantTime;

  public PartitionTTLStrategy(HoodieTable hoodieTable, String instantTime) {
    this.writeConfig = hoodieTable.getConfig();
    this.hoodieTable = hoodieTable;
    this.instantTime = instantTime;
  }

  /**
   * Get expired partition paths for a specific partition ttl strategy.
   *
   * @return Expired partition paths.
   */
  public abstract List<String> getExpiredPartitionPaths();

  /**
   * Scan and list all partitions for partition ttl management.
   *
   * @return all partitions paths for the dataset.
   */
  protected List<String> getPartitionPathsForTTL() {
    String partitionSelected = writeConfig.getClusteringPartitionSelected();
    HoodieTimer timer = HoodieTimer.start();
    List<String> partitionsForTTL;
    if (StringUtils.isNullOrEmpty(partitionSelected)) {
      // Return All partition paths
      partitionsForTTL = FSUtils.getAllPartitionPaths(
          hoodieTable.getContext(), hoodieTable.getStorage(), writeConfig.getMetadataConfig(), writeConfig.getBasePath());
    } else {
      partitionsForTTL = Arrays.asList(partitionSelected.split(","));
    }
    LOG.info("Get partitions for ttl cost {} ms", timer.endTimer());
    return partitionsForTTL;
  }

}
