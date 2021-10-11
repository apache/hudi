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

package org.apache.hudi.client;

import org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieClientMultiWriter extends HoodieClientTestBase {

  @Test
  public void testHoodieClientBasicMultiWriter() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    HoodieWriteConfig cfg =
        getConfigBuilder()
            .withCompactionConfig(
                HoodieCompactionConfig.newBuilder()
                    .withFailedWritesCleaningPolicy(
                        HoodieFailedWritesCleaningPolicy.LAZY)
                    .withAutoClean(false)
                    .build())
            .withWriteConcurrencyMode(
                WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
            .withLockConfig(
                HoodieLockConfig.newBuilder()
                    .withLockProvider(
                        FileSystemBasedLockProviderTestClass.class)
                    .build())
            .withAutoCommit(false)
            .withProperties(properties)
            .build();
    // Create the first commit
    createCommitWithInserts(getHoodieWriteClient(cfg), "001", 1);
    try {
      ExecutorService executors = Executors.newFixedThreadPool(2);
      HoodieFlinkWriteClient<?> client1 = getHoodieWriteClient(cfg);
      HoodieFlinkWriteClient<?> client2 = getHoodieWriteClient(cfg);
      Future<?> future1 =
          executors.submit(
              () -> {
                String newCommitTime = "002";
                int numRecords = 1;
                try {
                  createCommitWithUpserts(client1, newCommitTime, numRecords);
                } catch (Exception e1) {
                  assertTrue(e1 instanceof HoodieWriteConflictException);
                  throw new RuntimeException(e1);
                }
              });
      Future<?> future2 =
          executors.submit(
              () -> {
                String newCommitTime = "003";
                int numRecords = 1;
                try {
                  createCommitWithUpserts(client2, newCommitTime, numRecords);
                } catch (Exception e2) {
                  assertTrue(e2 instanceof HoodieWriteConflictException);
                  throw new RuntimeException(e2);
                }
              });
      future1.get();
      future2.get();
      Assertions.fail(
          "Should not reach here, this means concurrent writes were handled incorrectly");
    } catch (Exception e) {
      // Expected to fail due to overlapping commits
    }
  }

  private void createCommitWithInserts(
      HoodieFlinkWriteClient<?> client, String newCommitTime, int numRecords)
      throws Exception {
    List<WriteStatus> result =
        insertFirstBatch(client, newCommitTime, numRecords, HoodieFlinkWriteClient::insert);
    assertTrue(client.commit(newCommitTime, result), "Commit should succeed");
  }

  private void createCommitWithUpserts(
      HoodieFlinkWriteClient<?> client, String newCommitTime, int numRecords)
      throws Exception {
    List<WriteStatus> result =
        updateBatch(client, newCommitTime, numRecords, HoodieFlinkWriteClient::upsert);
    client.commit(newCommitTime, result);
  }
}
