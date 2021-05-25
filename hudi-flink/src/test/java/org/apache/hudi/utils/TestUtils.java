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
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.StreamReadMonitoringFunction;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Common test utils.
 */
public class TestUtils {

  public static String getLatestCommit(String basePath) {
    final HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(StreamerUtil.getHadoopConf()).setBasePath(basePath).build();
    return metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants().lastInstant().get().getTimestamp();
  }

  public static String getFirstCommit(String basePath) {
    final HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(StreamerUtil.getHadoopConf()).setBasePath(basePath).build();
    return metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants().firstInstant().get().getTimestamp();
  }

  public static String getSplitPartitionPath(MergeOnReadInputSplit split) {
    assertTrue(split.getLogPaths().isPresent());
    final String logPath = split.getLogPaths().get().get(0);
    String[] paths = logPath.split(File.separator);
    return paths[paths.length - 2];
  }

  public static StreamReadMonitoringFunction getMonitorFunc(Configuration conf) {
    final String basePath = conf.getString(FlinkOptions.PATH);
    final HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(StreamerUtil.getHadoopConf()).setBasePath(basePath).build();
    return new StreamReadMonitoringFunction(conf, new Path(basePath), metaClient, 1024 * 1024L);
  }
}
