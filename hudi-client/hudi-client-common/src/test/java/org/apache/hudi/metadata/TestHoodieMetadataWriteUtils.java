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

package org.apache.hudi.metadata;

import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestHoodieMetadataWriteUtils {

  @Test
  public void testCreateMetadataWriteConfigForCleaner() {
    HoodieWriteConfig writeConfig1 = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .build();

    HoodieWriteConfig metadataWriteConfig1 = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig1, HoodieFailedWritesCleaningPolicy.EAGER,
        HoodieTableVersion.SIX);
    assertEquals(HoodieFailedWritesCleaningPolicy.EAGER, metadataWriteConfig1.getFailedWritesCleanPolicy());
    assertEquals(HoodieCleaningPolicy.KEEP_LATEST_COMMITS, metadataWriteConfig1.getCleanerPolicy());
    assertEquals(1, metadataWriteConfig1.getCleaningMaxCommits());
    // default value already greater than data cleaner commits retained * 1.2
    assertEquals(HoodieMetadataConfig.DEFAULT_METADATA_CLEANER_COMMITS_RETAINED, metadataWriteConfig1.getCleanerCommitsRetained());

    assertNotEquals(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS, metadataWriteConfig1.getCleanerPolicy());
    assertNotEquals(HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS, metadataWriteConfig1.getCleanerPolicy());

    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(20)
            .withMaxCommitsBeforeCleaning(10)
            .build())
        .build();
    HoodieWriteConfig metadataWriteConfig2 = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig2, HoodieFailedWritesCleaningPolicy.EAGER,
        HoodieTableVersion.SIX);
    assertEquals(HoodieFailedWritesCleaningPolicy.EAGER, metadataWriteConfig2.getFailedWritesCleanPolicy());
    assertEquals(HoodieCleaningPolicy.KEEP_LATEST_COMMITS, metadataWriteConfig2.getCleanerPolicy());
    // data cleaner commits retained * 1.2 is greater than default
    assertEquals(24, metadataWriteConfig2.getCleanerCommitsRetained());
    assertEquals(10, metadataWriteConfig2.getCleaningMaxCommits());
  }

  @Test
  public void testCreateMetadataWriteConfigForNBCC() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withStreamingWriteEnabled(true).build())
        .build();

    HoodieWriteConfig metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig, HoodieFailedWritesCleaningPolicy.EAGER,
        HoodieTableVersion.EIGHT);
    validateMetadataWriteConfig(metadataWriteConfig, HoodieFailedWritesCleaningPolicy.LAZY,
        WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL, InProcessLockProvider.class.getCanonicalName());

    // disable streaming writes to metadata table.
    Properties properties = new Properties();
    properties.put(HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key(), "false");
    writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path/.hoodie/metadata/")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .withProperties(properties)
        .build();

    metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig, HoodieFailedWritesCleaningPolicy.EAGER,
        HoodieTableVersion.EIGHT);
    validateMetadataWriteConfig(metadataWriteConfig, HoodieFailedWritesCleaningPolicy.EAGER,
        WriteConcurrencyMode.SINGLE_WRITER, null);
  }

  private void validateMetadataWriteConfig(HoodieWriteConfig metadataWriteConfig, HoodieFailedWritesCleaningPolicy expectedPolicy,
                                           WriteConcurrencyMode expectedWriteConcurrencyMode, String expectedLockProviderClass) {
    assertEquals(expectedPolicy, metadataWriteConfig.getFailedWritesCleanPolicy());
    assertEquals(expectedWriteConcurrencyMode, metadataWriteConfig.getWriteConcurrencyMode());
    if (expectedLockProviderClass != null) {
      assertEquals(expectedLockProviderClass, metadataWriteConfig.getLockProviderClass());
    } else {
      assertNull(metadataWriteConfig.getLockProviderClass());
    }
  }
}
