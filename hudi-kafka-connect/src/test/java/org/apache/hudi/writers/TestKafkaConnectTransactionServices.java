/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.writers;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.apache.hudi.connect.writers.KafkaConnectTransactionServices;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sync.common.HoodieSyncTool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestKafkaConnectTransactionServices {

  @TempDir
  Path path;

  @Test
  public void testTableCreation() {
    Properties props = tableProps();
    props.put("hoodie.table.version", "6");
    KafkaConnectConfigs configs = KafkaConnectConfigs.newBuilder()
        .withProperties(props)
        .build();
    new KafkaConnectTransactionServices(configs);

    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(path.toFile().getPath());
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion());
    assertEquals(HoodieTableType.COPY_ON_WRITE, metaClient.getTableConfig().getTableType());
  }

  @Test
  public void testEndCommitSucceedsWhenMetaSyncFailsAfterCompletion() {
    Properties props = tableProps();
    props.put(KafkaConnectConfigs.META_SYNC_ENABLE.key(), "true");
    props.put(KafkaConnectConfigs.META_SYNC_CLASSES.key(), FailingMetaSyncTool.class.getName());
    FailingMetaSyncTool.SYNC_ATTEMPTS.set(0);
    try (KafkaConnectTransactionServices transactionServices = new KafkaConnectTransactionServices(
        KafkaConnectConfigs.newBuilder().withProperties(props).build())) {
      // Like ConnectTransactionCoordinator.start(), cache the timeline before the commit, so endCommit has to reload it
      transactionServices.fetchLatestExtraCommitMetadata();
      String commitTime = transactionServices.startCommit();

      // The instant completes and only the meta sync after it fails, so the commit must succeed
      assertTrue(transactionServices.endCommit(commitTime, Collections.emptyList(),
          Collections.singletonMap("kafka.commit.offsets", "0=10")));
      assertEquals(1, FailingMetaSyncTool.SYNC_ATTEMPTS.get());
      assertTrue(isCompleted(commitTime));
    }
  }

  @Test
  public void testEndCommitSucceedsWhenInlineClusteringFailsAfterCompletion() {
    Properties props = tableProps();
    props.put(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true");
    props.put(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "1");
    props.put(HoodieClusteringConfig.PLAN_STRATEGY_CLASS_NAME.key(), "org.apache.hudi.NonExistentClusteringPlanStrategy");
    props.put(KafkaConnectConfigs.META_SYNC_ENABLE.key(), "true");
    props.put(KafkaConnectConfigs.META_SYNC_CLASSES.key(), FailingMetaSyncTool.class.getName());
    FailingMetaSyncTool.SYNC_ATTEMPTS.set(0);
    try (KafkaConnectTransactionServices transactionServices = new KafkaConnectTransactionServices(
        KafkaConnectConfigs.newBuilder().withProperties(props).build())) {
      transactionServices.fetchLatestExtraCommitMetadata();
      String commitTime = transactionServices.startCommit();

      // Like cleaning and archiving, inline clustering runs inside javaClient.commit after the instant completed;
      // it fails to load its plan strategy, so the commit must still succeed and the meta sync must still run
      assertTrue(transactionServices.endCommit(commitTime, Collections.emptyList(),
          Collections.singletonMap("kafka.commit.offsets", "0=10")));
      assertEquals(1, FailingMetaSyncTool.SYNC_ATTEMPTS.get());
      assertTrue(isCompleted(commitTime));
    }
  }

  @Test
  public void testEndCommitFailsWhenInstantDoesNotComplete() throws IOException {
    try (KafkaConnectTransactionServices transactionServices = new KafkaConnectTransactionServices(
        KafkaConnectConfigs.newBuilder().withProperties(tableProps()).build())) {
      String commitTime = transactionServices.startCommit();
      // Losing the inflight file makes the commit fail while its instant is still pending
      FileCreateUtils.deleteInflightCommit(HoodieTestUtils.createMetaClient(path.toFile().getPath()), commitTime);

      assertThrows(RuntimeException.class, () -> transactionServices.endCommit(commitTime, Collections.emptyList(),
          Collections.emptyMap()));
      HoodieTimeline timeline = HoodieTestUtils.createMetaClient(path.toFile().getPath()).getActiveTimeline();
      assertTrue(timeline.filterInflightsAndRequested().containsInstant(commitTime));
      assertFalse(timeline.filterCompletedInstants().containsInstant(commitTime));
    }
  }

  private boolean isCompleted(String commitTime) {
    return HoodieTestUtils.createMetaClient(path.toFile().getPath()).getActiveTimeline()
        .filterCompletedInstants().containsInstant(commitTime);
  }

  private Properties tableProps() {
    Properties props = new Properties();
    props.put("hoodie.table.name", "test");
    props.put("hoodie.base.path", path);
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "key");
    return props;
  }

  /**
   * A meta sync tool that fails like an unreachable metastore.
   */
  public static class FailingMetaSyncTool extends HoodieSyncTool {

    static final AtomicInteger SYNC_ATTEMPTS = new AtomicInteger();

    public FailingMetaSyncTool(Properties props) {
      super(props);
    }

    @Override
    public void syncHoodieTable() {
      SYNC_ATTEMPTS.incrementAndGet();
      throw new HoodieException("Metastore unavailable");
    }
  }
}
