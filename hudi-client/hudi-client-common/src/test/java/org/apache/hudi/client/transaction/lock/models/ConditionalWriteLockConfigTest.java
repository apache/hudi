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

package org.apache.hudi.client.transaction.lock.models;

import org.apache.hudi.client.transaction.lock.ConditionalWriteLockConfig;
import org.apache.hudi.common.config.TypedProperties;

import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.config.HoodieConfig.BASE_PATH_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConditionalWriteLockConfigTest {

  @Test
  void testDefaultValues() {
    TypedProperties props = new TypedProperties();
    props.setProperty(BASE_PATH_KEY, "s3://hudi-bucket/table/basepath");

    ConditionalWriteLockConfig.Builder builder = new ConditionalWriteLockConfig.Builder();
    ConditionalWriteLockConfig config = builder
        .fromProperties(props)
        .build();

    assertEquals("", config.getLocksLocation());
    assertEquals(5 * 60 * 1000, config.getLockValidityTimeoutMs(), "Default lock validity should be 5 minutes");
    assertEquals(30 * 1000, config.getHeartbeatPollMs(), "Default heartbeat poll time should be 30 seconds");
  }

  @Test
  void testCustomValues() {
    TypedProperties props = new TypedProperties();
    props.setProperty(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "s3://bucket/path/locks");
    props.setProperty(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "120000");
    props.setProperty(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), "10000");
    props.setProperty(BASE_PATH_KEY, "/hudi/table/basepath");

    ConditionalWriteLockConfig config = new ConditionalWriteLockConfig.Builder()
        .fromProperties(props)
        .build();

    assertEquals("s3://bucket/path/locks", config.getLocksLocation());
    assertEquals(120000, config.getLockValidityTimeoutMs());
    assertEquals(10000, config.getHeartbeatPollMs());
    assertEquals("/hudi/table/basepath", config.getHudiTableBasePath());
  }

  @Test
  void testMissingRequiredProperties() {
    TypedProperties props = new TypedProperties();
    props.setProperty(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "120000");
    ConditionalWriteLockConfig.Builder propsBuilder = new ConditionalWriteLockConfig.Builder();

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> propsBuilder.fromProperties(props));
    assertTrue(exception.getMessage().contains(BASE_PATH_KEY));
    props.setProperty(BASE_PATH_KEY, "s3://bucket/path/locks");
    props.setProperty(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "s3://bucket/path/locks");
    assertThrows(IllegalArgumentException.class, () -> propsBuilder.fromProperties(props));
  }

  @Test
  void testTimeThresholds() {
    TypedProperties props = new TypedProperties();
    props.setProperty(BASE_PATH_KEY, "/hudi/table/basepath");
    props.setProperty(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "5000");
    props.setProperty(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), "3000");
    ConditionalWriteLockConfig.Builder propsBuilder = new ConditionalWriteLockConfig.Builder();

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> propsBuilder.fromProperties(props));
    assertTrue(exception.getMessage().contains(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key()));
    props.setProperty(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "4999");
    props.setProperty(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), "1000");
    exception = assertThrows(IllegalArgumentException.class, () -> propsBuilder.fromProperties(props));
    assertTrue(exception.getMessage().contains(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key()));
    props.setProperty(ConditionalWriteLockConfig.LOCK_VALIDITY_TIMEOUT_MS.key(), "5000");
    props.setProperty(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key(), "999");
    exception = assertThrows(IllegalArgumentException.class, () -> propsBuilder.fromProperties(props));
    assertTrue(exception.getMessage().contains(ConditionalWriteLockConfig.HEARTBEAT_POLL_MS.key()));
  }

  @Test
  void testBucketPathValidation() {
    TypedProperties props = new TypedProperties();
    props.setProperty(ConditionalWriteLockConfig.LOCK_INTERNAL_STORAGE_LOCATION.key(), "invalid/path");
    props.setProperty(BASE_PATH_KEY, "/hudi/table/basepath");

    ConditionalWriteLockConfig config = new ConditionalWriteLockConfig.Builder()
        .fromProperties(props)
        .build();

    assertEquals("invalid/path", config.getLocksLocation(), "Locks location should not modify invalid inputs");
  }
}
