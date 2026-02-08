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

package org.apache.hudi.client.transaction.lock;

import org.apache.hudi.common.config.LockConfiguration;

import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_HOLDER_APP_ID_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link HoodieInterProcessMutex#getLockNodeBytes()}.
 */
public class TestHoodieInterProcessMutex {

  private static final String LOCK_PATH = "/hudi/test/lock";

  @Test
  public void testGetLockNodeBytesContainsApplicationIdFromConfig() throws Exception {
    String appId = "my-spark-app-12345";
    LockConfiguration config = new LockConfiguration(createPropsWithAppId(appId));

    HoodieInterProcessMutex mutex = new HoodieInterProcessMutex(mock(CuratorFramework.class), LOCK_PATH, config);
    byte[] lockNodeBytes = getLockNodeBytes(mutex);

    String lockNodeData = new String(lockNodeBytes, StandardCharsets.UTF_8);
    assertTrue(lockNodeData.contains("application_id=" + appId),
        "Lock node data should contain application_id from config: " + lockNodeData);
  }

  @Test
  public void testGetLockNodeBytesDefaultsToUnknownWhenAppIdNotSet() throws Exception {
    LockConfiguration config = new LockConfiguration(new Properties());

    HoodieInterProcessMutex mutex = new HoodieInterProcessMutex(mock(CuratorFramework.class), LOCK_PATH, config);
    byte[] lockNodeBytes = getLockNodeBytes(mutex);

    String lockNodeData = new String(lockNodeBytes, StandardCharsets.UTF_8);
    assertTrue(lockNodeData.contains("application_id=Unknown"),
        "Lock node data should default application_id to Unknown: " + lockNodeData);
  }

  @Test
  public void testGetLockNodeBytesFormat() throws Exception {
    String appId = "test-app";
    LockConfiguration config = new LockConfiguration(createPropsWithAppId(appId));

    HoodieInterProcessMutex mutex = new HoodieInterProcessMutex(mock(CuratorFramework.class), LOCK_PATH, config);
    byte[] lockNodeBytes = getLockNodeBytes(mutex);

    String expected = "application_id=" + appId;
    assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8), lockNodeBytes,
        "Lock node bytes should be in key=value format");
  }

  private static Properties createPropsWithAppId(String appId) {
    Properties props = new Properties();
    props.setProperty(LOCK_HOLDER_APP_ID_KEY, appId);
    return props;
  }

  private static byte[] getLockNodeBytes(HoodieInterProcessMutex mutex) throws Exception {
    Method method = HoodieInterProcessMutex.class.getDeclaredMethod("getLockNodeBytes");
    method.setAccessible(true);
    return (byte[]) method.invoke(mutex);
  }
}
