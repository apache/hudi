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

package org.apache.hudi.utils;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.StreamReadMonitoringFunction;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Common test utils.
 */
public class TestUtils {
  public static String getLastPendingInstant(String basePath) {
    final HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(StreamerUtil.getHadoopConf()).setBasePath(basePath).build();
    return StreamerUtil.getLastPendingInstant(metaClient);
  }

  public static String getLastCompleteInstant(String basePath) {
    final HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(StreamerUtil.getHadoopConf()).setBasePath(basePath).build();
    return StreamerUtil.getLastCompletedInstant(metaClient);
  }

  public static String getLastDeltaCompleteInstant(String basePath) {
    final HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(StreamerUtil.getHadoopConf()).setBasePath(basePath).build();
    return metaClient.getCommitsTimeline().filterCompletedInstants()
        .filter(hoodieInstant -> hoodieInstant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION))
        .lastInstant()
        .map(HoodieInstant::getTimestamp)
        .orElse(null);
  }

  public static String getFirstCompleteInstant(String basePath) {
    final HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(StreamerUtil.getHadoopConf()).setBasePath(basePath).build();
    return metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants().firstInstant()
        .map(HoodieInstant::getTimestamp).orElse(null);
  }

  public static String getSplitPartitionPath(MergeOnReadInputSplit split) {
    assertTrue(split.getLogPaths().isPresent());
    final String logPath = split.getLogPaths().get().get(0);
    String[] paths = logPath.split(Path.SEPARATOR);
    return paths[paths.length - 2];
  }

  public static StreamReadMonitoringFunction getMonitorFunc(Configuration conf) {
    final String basePath = conf.getString(FlinkOptions.PATH);
    return new StreamReadMonitoringFunction(conf, new Path(basePath), 1024 * 1024L, null);
  }
}
