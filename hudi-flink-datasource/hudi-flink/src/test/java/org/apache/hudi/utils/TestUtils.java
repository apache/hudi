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

import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.sink.utils.MockStreamingRuntimeContext;
import org.apache.hudi.source.StreamReadMonitoringFunction;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Common test utils.
 */
public class TestUtils {
  public static String getLastPendingInstant(String basePath) {
    final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(new Configuration())), basePath);
    return StreamerUtil.getLastPendingInstant(metaClient);
  }

  public static String getLastCompleteInstant(String basePath) {
    final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(new Configuration())), basePath);
    return StreamerUtil.getLastCompletedInstant(metaClient);
  }

  public static String getLastCompleteInstant(String basePath, String commitAction) {
    final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(new Configuration())), basePath);
    return metaClient.getCommitsTimeline().filterCompletedInstants()
        .filter(instant -> commitAction.equals(instant.getAction()))
        .lastInstant()
        .map(HoodieInstant::requestedTime)
        .orElse(null);
  }

  public static String getLastDeltaCompleteInstant(String basePath) {
    final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(new Configuration())), basePath);
    return metaClient.getCommitsTimeline().filterCompletedInstants()
        .filter(hoodieInstant -> hoodieInstant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION))
        .lastInstant()
        .map(HoodieInstant::requestedTime)
        .orElse(null);
  }

  public static String getFirstCompleteInstant(String basePath) {
    final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(new Configuration())), basePath);
    return metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants().firstInstant()
        .map(HoodieInstant::requestedTime).orElse(null);
  }

  @Nullable
  public static String getNthCompleteInstant(StoragePath basePath, int n, String action) {
    final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(new Configuration())), basePath, HoodieTableVersion.EIGHT);
    return metaClient.getActiveTimeline()
        .filterCompletedInstants()
        .filter(instant -> action.equals(instant.getAction()))
        .nthInstant(n).map(HoodieInstant::requestedTime)
        .orElse(null);
  }

  @Nullable
  public static String getNthArchivedInstant(String basePath, int n) {
    final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(new Configuration())), basePath);
    return metaClient.getArchivedTimeline().getCommitsTimeline().filterCompletedInstants()
        .nthInstant(n).map(HoodieInstant::requestedTime).orElse(null);
  }

  public static String getSplitPartitionPath(MergeOnReadInputSplit split) {
    assertTrue(split.getLogPaths().isPresent());
    final String logPath = split.getLogPaths().get().get(0);
    String[] paths = logPath.split(StoragePath.SEPARATOR);
    return paths[paths.length - 2];
  }

  public static StreamReadMonitoringFunction getMonitorFunc(Configuration conf) {
    final String basePath = conf.get(FlinkOptions.PATH);
    return new StreamReadMonitoringFunction(conf, new Path(basePath), TestConfigurations.ROW_TYPE, 1024 * 1024L, null);
  }

  public static MockStreamingRuntimeContext getMockRuntimeContext() {
    return new org.apache.hudi.sink.utils.MockStreamingRuntimeContext(false, 4, 0);
  }

  public static int getCompletedInstantCount(String basePath, String action) {
    final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(new Configuration())), basePath);
    return metaClient.getActiveTimeline()
        .filterCompletedInstants()
        .filter(instant -> action.equals(instant.getAction()))
        .countInstants();
  }

  public static HoodieCommitMetadata deleteInstantFile(HoodieTableMetaClient metaClient, HoodieInstant instant) throws Exception {
    ValidationUtils.checkArgument(instant.isCompleted());
    HoodieCommitMetadata metadata = TimelineUtils.getCommitMetadata(instant, metaClient.getActiveTimeline());
    TimelineUtils.deleteInstantFile(metaClient.getStorage(), metaClient.getTimelinePath(),
        instant, metaClient.getInstantFileNameGenerator());
    return metadata;
  }

  public static void saveInstantAsComplete(HoodieTableMetaClient metaClient, HoodieInstant instant, HoodieCommitMetadata metadata) {
    metaClient.getActiveTimeline().saveAsComplete(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, instant.getAction(), instant.requestedTime()),
        Option.of(metadata));
  }

  public static String amendCompletionTimeToLatest(HoodieTableMetaClient metaClient, java.nio.file.Path sourcePath, String instantTime) throws IOException {
    String fileExt = sourcePath.getFileName().toString().split("\\.")[1];
    String newCompletionTime = WriteClientTestUtils.createNewInstantTime();
    String newFileName = instantTime + "_" + newCompletionTime + "." + fileExt;

    java.nio.file.Path newFilePath = sourcePath.getParent().resolve(newFileName);
    Files.move(sourcePath, newFilePath);
    return newCompletionTime;
  }

  /**
   * Waits for a condition to be met within a specific timeout.
   *
   * @param condition      The condition to poll.
   * @param timeoutSeconds Maximum time to wait in seconds.
   */
  public static boolean waitUntil(BooleanSupplier condition, int timeoutSeconds) throws InterruptedException {
    long limit = System.currentTimeMillis() + (timeoutSeconds * 1000L);
    while (System.currentTimeMillis() < limit) {
      if (condition.getAsBoolean()) {
        return true;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    return false;
  }
}
