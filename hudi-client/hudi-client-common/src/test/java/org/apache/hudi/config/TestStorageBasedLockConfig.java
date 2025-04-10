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

package org.apache.hudi.config;

import org.apache.hudi.common.config.TypedProperties;

import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.config.HoodieCommonConfig.BASE_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestStorageBasedLockConfig {

  @Test
  void testDefaultValues() {
    // Asserts that the defaults are correct
    TypedProperties props = new TypedProperties();
    props.setProperty(BASE_PATH.key(), "s3://hudi-bucket/table/basepath");

    StorageBasedLockConfig.Builder builder = new StorageBasedLockConfig.Builder();
    StorageBasedLockConfig config = builder
        .fromProperties(props)
        .build();

    assertEquals("", config.getLocksLocation());
    assertEquals(5 * 60, config.getLockValidityTimeout(), "Default lock validity should be 5 minutes");
    assertEquals(30, config.getHeartbeatPoll(), "Default heartbeat poll time should be 30 seconds");
  }

  @Test
  void testCustomValues() {
    // Testing that custom values which differ from defaults can be read properly
    TypedProperties props = new TypedProperties();
    props.setProperty(StorageBasedLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "s3://bucket/path/locks");
    props.setProperty(StorageBasedLockConfig.LOCK_VALIDITY_TIMEOUT.key(), "120");
    props.setProperty(StorageBasedLockConfig.HEARTBEAT_POLL.key(), "10");
    props.setProperty(BASE_PATH.key(), "/hudi/table/basepath");

    StorageBasedLockConfig config = new StorageBasedLockConfig.Builder()
        .fromProperties(props)
        .build();

    assertEquals("s3://bucket/path/locks", config.getLocksLocation());
    assertEquals(120, config.getLockValidityTimeout());
    assertEquals(10, config.getHeartbeatPoll());
    assertEquals("/hudi/table/basepath", config.getHudiTableBasePath());
  }

  @Test
  void testBasePathPropertiesValidation() {
    // Tests that validations around the base path are present.
    TypedProperties props = new TypedProperties();
    props.setProperty(StorageBasedLockConfig.LOCK_VALIDITY_TIMEOUT.key(), "120000");
    StorageBasedLockConfig.Builder propsBuilder = new StorageBasedLockConfig.Builder();

    // Missing base path
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> propsBuilder.fromProperties(props));
    assertTrue(exception.getMessage().contains(BASE_PATH.key()));
    props.setProperty(BASE_PATH.key(), "s3://bucket/path/locks");
    // Ensure we cannot write to the same lock location as the base path.
    props.setProperty(StorageBasedLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "s3://bucket/path/locks");
    assertThrows(IllegalArgumentException.class, () -> propsBuilder.fromProperties(props));
    // Ensure we cannot write to the metadata directory.
    props.setProperty(StorageBasedLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "s3://bucket/path/locks/.hoodie/.metadata");
    assertThrows(IllegalArgumentException.class, () -> propsBuilder.fromProperties(props));
    // Ensure we cannot write to a partition.
    props.setProperty(StorageBasedLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "s3://bucket/path/locks/partition");
    assertThrows(IllegalArgumentException.class, () -> propsBuilder.fromProperties(props));
    // Ensure we do not throw an exception.
    props.setProperty(StorageBasedLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "s3://bucket/other-path");
    propsBuilder.fromProperties(props);
  }

  @Test
  void testTimeThresholds() {
    // Ensure that validations which restrict the time-based inputs are working.
    TypedProperties props = new TypedProperties();
    props.setProperty(BASE_PATH.key(), "/hudi/table/basepath");
    props.setProperty(StorageBasedLockConfig.LOCK_VALIDITY_TIMEOUT.key(), "5");
    props.setProperty(StorageBasedLockConfig.HEARTBEAT_POLL.key(), "3");
    StorageBasedLockConfig.Builder propsBuilder = new StorageBasedLockConfig.Builder();

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> propsBuilder.fromProperties(props));
    assertTrue(exception.getMessage().contains(StorageBasedLockConfig.LOCK_VALIDITY_TIMEOUT.key()));
    props.setProperty(StorageBasedLockConfig.LOCK_VALIDITY_TIMEOUT.key(), "4");
    props.setProperty(StorageBasedLockConfig.HEARTBEAT_POLL.key(), "1");
    exception = assertThrows(IllegalArgumentException.class, () -> propsBuilder.fromProperties(props));
    assertTrue(exception.getMessage().contains(StorageBasedLockConfig.LOCK_VALIDITY_TIMEOUT.key()));
    props.setProperty(StorageBasedLockConfig.LOCK_VALIDITY_TIMEOUT.key(), "5");
    props.setProperty(StorageBasedLockConfig.HEARTBEAT_POLL.key(), "0");
    exception = assertThrows(IllegalArgumentException.class, () -> propsBuilder.fromProperties(props));
    assertTrue(exception.getMessage().contains(StorageBasedLockConfig.HEARTBEAT_POLL.key()));
  }
}