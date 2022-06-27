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

package org.apache.hudi.util;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.table.HoodieFlinkTable;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for flink hudi clustering.
 */
public class ClusteringUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ClusteringUtil.class);

  public static void validateClusteringScheduling(Configuration conf) {
    if (OptionsResolver.isBucketIndexType(conf)) {
      throw new UnsupportedOperationException("Clustering is not supported for bucket index.");
    }
  }

  /**
   * Schedules clustering plan by condition.
   *
   * @param conf        The configuration
   * @param writeClient The write client
   * @param committed   Whether the instant was committed
   */
  public static void scheduleClustering(Configuration conf, HoodieFlinkWriteClient writeClient, boolean committed) {
    validateClusteringScheduling(conf);
    if (committed) {
      writeClient.scheduleClustering(Option.empty());
    }
  }

  /**
   * Force rolls back all the inflight clustering instants, especially for job failover restart.
   *
   * @param table The hoodie table
   * @param writeClient The write client
   */
  public static void rollbackClustering(HoodieFlinkTable<?> table, HoodieFlinkWriteClient writeClient) {
    List<HoodieInstant> inflightInstants = ClusteringUtils.getPendingClusteringInstantTimes(table.getMetaClient())
        .stream()
        .filter(instant -> instant.getState() == HoodieInstant.State.INFLIGHT)
        .collect(Collectors.toList());
    inflightInstants.forEach(inflightInstant -> {
      LOG.info("Rollback the inflight clustering instant: " + inflightInstant + " for failover");
      table.rollbackInflightClustering(inflightInstant,
          commitToRollback -> writeClient.getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false));
      table.getMetaClient().reloadActiveTimeline();
    });
  }
}
