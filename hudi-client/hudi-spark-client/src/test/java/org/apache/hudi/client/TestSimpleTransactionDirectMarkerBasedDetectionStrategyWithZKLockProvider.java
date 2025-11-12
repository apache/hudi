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

package org.apache.hudi.client;

import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.marker.SimpleTransactionDirectMarkerBasedDetectionStrategy;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.curator.test.TestingServer;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.config.LockConfiguration.ZK_BASE_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECT_URL_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_LOCK_KEY_PROP_KEY;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSimpleTransactionDirectMarkerBasedDetectionStrategyWithZKLockProvider extends HoodieClientTestBase {

  private HoodieWriteConfig config;
  private TestingServer server;

  private void setUp(boolean partitioned) throws Exception {
    initPath();
    //initSparkContexts();
    if (partitioned) {
      initTestDataGenerator();
    } else {
      initTestDataGenerator(new String[] {""});
    }
    initHoodieStorage();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ);

    Properties properties = getPropertiesForKeyGen();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    server = new TestingServer();
    properties.setProperty(ZK_BASE_PATH_PROP_KEY, basePath);
    properties.setProperty(ZK_CONNECT_URL_PROP_KEY, server.getConnectString());
    properties.setProperty(ZK_BASE_PATH_PROP_KEY, server.getTempDirectory().getAbsolutePath());
    properties.setProperty(ZK_LOCK_KEY_PROP_KEY, "key");

    config = getConfigBuilder()
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.MEMORY)
            .withSecondaryStorageType(FileSystemViewStorageType.MEMORY).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAutoClean(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .withAutoArchive(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withMarkersType(MarkerType.DIRECT.name())
        .withEarlyConflictDetectionEnable(true)
        .withEarlyConflictDetectionStrategy(SimpleTransactionDirectMarkerBasedDetectionStrategy.class.getName())
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(ZookeeperBasedLockProvider.class).build())
        .withProperties(properties)
        .build();
  }

  @AfterEach
  public void clean() throws IOException {
    cleanupResources();
    FileIOUtils.deleteDirectory(new File(basePath));
    if (server != null) {
      server.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSimpleTransactionDirectMarkerBasedDetectionStrategy(boolean partitioned) throws Exception {
    setUp(partitioned);

    final String nextCommitTime1 = "00000000000001";
    final SparkRDDWriteClient client1 = getHoodieWriteClient(config);
    Function2<List<HoodieRecord>, String, Integer> recordGenFunction1 = generateWrapRecordsFn(false, config, dataGen::generateInserts);
    final List<HoodieRecord> records1 = recordGenFunction1.apply(nextCommitTime1, 200);
    final JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(records1, 1);
    // Finish first base commit
    WriteClientTestUtils.startCommitWithTime(client1, nextCommitTime1);
    JavaRDD<WriteStatus> writeStatusList1 =  client1.insert(writeRecords1, nextCommitTime1);
    assertTrue(client1.commit(nextCommitTime1, writeStatusList1), "Commit should succeed");

    final SparkRDDWriteClient client2 = getHoodieWriteClient(config, true);
    // We do not want to close client2 so setting shouldCloseOlderClient to false while creating client3
    final SparkRDDWriteClient client3 = getHoodieWriteClient(config, false);
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction2 =
        generateWrapRecordsFn(false, config, dataGen::generateUniqueUpdates);

    // Prepare update records
    final String nextCommitTime2 = "00000000000002";
    final List<HoodieRecord> records2 = recordGenFunction2.apply(nextCommitTime2, 200);
    final JavaRDD<HoodieRecord> writeRecords2 = jsc.parallelize(records2, 1);
    // start to write commit 002
    WriteClientTestUtils.startCommitWithTime(client2, nextCommitTime2);
    JavaRDD<WriteStatus> writeStatusList2 =  client2.upsert(writeRecords2, nextCommitTime2);
    assertNoWriteErrors(writeStatusList2.collect());

    // start to write commit 003
    // this commit 003 will failed quickly because early conflict detection before create marker.
    final String nextCommitTime3 = "00000000000003";
    assertThrows(SparkException.class, () -> {
      final List<HoodieRecord> records3 = recordGenFunction2.apply(nextCommitTime3, 200);
      final JavaRDD<HoodieRecord> writeRecords3 = jsc.parallelize(records3, 1);
      WriteClientTestUtils.startCommitWithTime(client3, nextCommitTime3);
      JavaRDD<WriteStatus> writeStatusList3 =  client3.upsert(writeRecords3, nextCommitTime3);
      client3.commit(nextCommitTime3, writeStatusList3);
    }, "Early conflict detected but cannot resolve conflicts for overlapping writes");

    // start to commit 002 and success
    assertDoesNotThrow(() -> {
      client2.commit(nextCommitTime2, writeStatusList2);
    });

    client2.close();
    client3.close();
  }

}
