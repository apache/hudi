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

    assertEquals(5 * 60, config.getValiditySeconds(), "Default lock validity should be 5 minutes");
    assertEquals(30, config.getHeartbeatPollSeconds(), "Default heartbeat poll time should be 30 seconds");
  }

  @Test
  void testCustomValues() {
    // Testing that custom values which differ from defaults can be read properly
    TypedProperties props = new TypedProperties();
    props.setProperty(StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key(), "120");
    props.setProperty(StorageBasedLockConfig.HEARTBEAT_POLL_SECONDS.key(), "10");
    props.setProperty(BASE_PATH.key(), "/hudi/table/basepath");

    StorageBasedLockConfig config = new StorageBasedLockConfig.Builder()
        .fromProperties(props)
        .build();

    assertEquals(120, config.getValiditySeconds());
    assertEquals(10, config.getHeartbeatPollSeconds());
    assertEquals("/hudi/table/basepath", config.getHudiTableBasePath());
  }

  @Test
  void testBasePathPropertiesValidation() {
    // Tests that validations around the base path are present.
    TypedProperties props = new TypedProperties();
    props.setProperty(StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key(), "120");
    StorageBasedLockConfig.Builder propsBuilder = new StorageBasedLockConfig.Builder();

    // Missing base path
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> propsBuilder.fromProperties(props));
    assertTrue(exception.getMessage().contains(BASE_PATH.key()));
  }

  @Test
  void testTimeThresholds() {
    // Ensure that validations which restrict the time-based inputs are working.
    TypedProperties props = new TypedProperties();
    props.setProperty(BASE_PATH.key(), "/hudi/table/basepath");
    // Invalid config case: validity timeout is less than 10x of heartbeat poll period
    props.setProperty(StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key(), "5");
    props.setProperty(StorageBasedLockConfig.HEARTBEAT_POLL_SECONDS.key(), "3");
    StorageBasedLockConfig.Builder propsBuilder = new StorageBasedLockConfig.Builder();

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> propsBuilder.fromProperties(props));
    assertTrue(exception.getMessage().contains(StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key()));
    // Invalid config case: validity timeout is less than 10 seconds
    props.setProperty(StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key(), "9");
    props.setProperty(StorageBasedLockConfig.HEARTBEAT_POLL_SECONDS.key(), "1");
    exception = assertThrows(IllegalArgumentException.class, () -> propsBuilder.fromProperties(props));
    assertTrue(exception.getMessage().contains(StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key()));
    // Invalid config case: heartbeat poll period is less than 1 second
    props.setProperty(StorageBasedLockConfig.VALIDITY_TIMEOUT_SECONDS.key(), "10");
    props.setProperty(StorageBasedLockConfig.HEARTBEAT_POLL_SECONDS.key(), "0");
    exception = assertThrows(IllegalArgumentException.class, () -> propsBuilder.fromProperties(props));
    assertTrue(exception.getMessage().contains(StorageBasedLockConfig.HEARTBEAT_POLL_SECONDS.key()));
  }
}
