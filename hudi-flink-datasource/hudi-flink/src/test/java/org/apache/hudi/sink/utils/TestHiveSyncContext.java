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

package org.apache.hudi.sink.utils;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.hive.HiveSyncConfig;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HiveSyncContext}.
 */
public class TestHiveSyncContext {
  /**
   * Test partition path fields sync.
   */
  @Test
  void testSyncedPartitions() {
    Configuration configuration1 = new Configuration();
    Configuration configuration2 = new Configuration();
    String hiveSyncPartitionField = "hiveSyncPartitionField";
    String partitionPathField = "partitionPathField";

    configuration1.set(FlinkOptions.HIVE_SYNC_PARTITION_FIELDS, hiveSyncPartitionField);
    configuration1.set(FlinkOptions.PARTITION_PATH_FIELD, partitionPathField);

    configuration2.set(FlinkOptions.PARTITION_PATH_FIELD, partitionPathField);

    Properties props1 = HiveSyncContext.buildSyncConfig(configuration1);
    Properties props2 = HiveSyncContext.buildSyncConfig(configuration2);

    assertEquals(hiveSyncPartitionField, props1.getProperty(META_SYNC_PARTITION_FIELDS.key()));
    assertEquals(partitionPathField, props2.getProperty(META_SYNC_PARTITION_FIELDS.key()));
  }

  /**
   * Test an option that has no shortcut key.
   */
  @Test
  void testOptionWithoutShortcutKey() {
    Configuration configuration3 = new Configuration();
    configuration3.setString(HiveSyncConfig.HIVE_CREATE_MANAGED_TABLE.key(), "true");
    Properties props3 = HiveSyncContext.buildSyncConfig(configuration3);
    assertTrue(Boolean.parseBoolean(props3.getProperty(HiveSyncConfig.HIVE_CREATE_MANAGED_TABLE.key(), "false")));
  }
}
