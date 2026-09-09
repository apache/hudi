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

package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieTTLConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getCommitTimeAtUTC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests {@link HoodieTTLJob} on a table whose partitions were written at different times.
 */
public class TestHoodieTTLJob extends HoodieSparkClientTestBase {

  private static final int RECORDS_PER_PARTITION = 4;

  /**
   * Both constructors are covered: with an explicit props/meta client pair, and with the (jsc, cfg) constructor
   * that has to read --props and --hoodie-conf itself.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testTtlDropsOnlyExpiredPartitions(boolean readPropsFromFileSystem) throws IOException {
    writeOnePartitionPerInstant();

    HoodieTTLJob.Config cfg = new HoodieTTLJob.Config();
    cfg.basePath = basePath;
    cfg.parallelism = 2;

    HoodieTTLJob job;
    if (readPropsFromFileSystem) {
      Path propsFile = tempDir.resolve("ttl.properties");
      Files.write(propsFile, Arrays.asList(
          HoodieWriteConfig.TBL_NAME.key() + "=" + metaClient.getTableConfig().getTableName(),
          HoodieTTLConfig.PARTITION_TTL_STRATEGY_TYPE.key() + "=KEEP_BY_TIME"), StandardCharsets.UTF_8);
      cfg.propsFilePath = propsFile.toAbsolutePath().toString();
      cfg.configs.add(HoodieTTLConfig.DAYS_RETAIN.key() + "=10");
      job = new HoodieTTLJob(jsc, cfg);
    } else {
      TypedProperties props = new TypedProperties();
      props.setProperty(HoodieWriteConfig.TBL_NAME.key(), metaClient.getTableConfig().getTableName());
      props.setProperty(HoodieTTLConfig.PARTITION_TTL_STRATEGY_TYPE.key(), "KEEP_BY_TIME");
      props.setProperty(HoodieTTLConfig.DAYS_RETAIN.key(), "10");
      job = new HoodieTTLJob(jsc, cfg, props, metaClient);
      // the job turns async cleaning off on the properties it was handed
      assertEquals("false", props.get(HoodieCleanConfig.ASYNC_CLEAN.key()).toString());
    }

    job.run();

    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant replaceInstant =
        reloaded.getActiveTimeline().getCompletedReplaceTimeline().lastInstant().get();
    HoodieReplaceCommitMetadata replaceMetadata =
        reloaded.getActiveTimeline().readReplaceCommitMetadata(replaceInstant);
    assertEquals(
        new HashSet<>(Arrays.asList(DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH)),
        replaceMetadata.getPartitionToReplaceFileIds().keySet(),
        "only the partitions older than the retention are dropped");

    assertEquals(0, latestBaseFileCount(DEFAULT_FIRST_PARTITION_PATH));
    assertEquals(0, latestBaseFileCount(DEFAULT_SECOND_PARTITION_PATH));
    assertEquals(1, latestBaseFileCount(DEFAULT_THIRD_PARTITION_PATH),
        "the fresh partition must survive");
    assertFalse(replaceMetadata.getPartitionToReplaceFileIds().containsKey(DEFAULT_THIRD_PARTITION_PATH));
  }

  private void writeOnePartitionPerInstant() {
    HoodieWriteConfig writeConfig = getConfigBuilder().build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      // two partitions written far in the past, one written now
      writeRecordsForPartition(client, DEFAULT_FIRST_PARTITION_PATH, getCommitTimeAtUTC(0));
      writeRecordsForPartition(client, DEFAULT_SECOND_PARTITION_PATH, getCommitTimeAtUTC(1000));
      writeRecordsForPartition(client, DEFAULT_THIRD_PARTITION_PATH, WriteClientTestUtils.createNewInstantTime());
    }
  }

  private void writeRecordsForPartition(SparkRDDWriteClient client, String partition, String instantTime) {
    List<HoodieRecord> records =
        new ArrayList<>(dataGen.generateInsertsForPartition(instantTime, RECORDS_PER_PARTITION, partition));
    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    JavaRDD<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), instantTime);
    client.commit(instantTime, writeStatuses);
  }

  private long latestBaseFileCount(String partition) {
    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(metaClient);
    try (HoodieTableFileSystemView fsView = FileSystemViewManager.createInMemoryFileSystemView(
        context, reloaded, HoodieMetadataConfig.newBuilder().enable(false).build())) {
      return fsView.getLatestBaseFiles(partition).count();
    }
  }
}
