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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client.transaction.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.hudi.common.config.LockConfiguration;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is a HUDI specific wrapper for {@link InterProcessMutex} to allow for
 * passing in and utilizing information from {@link LockConfiguration}.
 * For example, the application id is passed as part of the lock information,
 * meaning that when the (distributed) lock is acquired other users can see the
 * application id as part of the lock information (when using tools like zkcli)
 */
public class HoodieInterProcessMutex extends InterProcessMutex {

  // Data that will be added to lock node upon lock being acquired.
  // This can be used to provide metadata about the thread holding the lock,
  // such as application id of the job
  private final byte[] lockNodeBytes;

  public HoodieInterProcessMutex(final CuratorFramework client, final String path,
      final LockConfiguration config) {
    super(client, path);
    Map<String, String> lockNodeData = new HashMap<>();
    String applicationId = config
        .getConfig()
        .getString(LockConfiguration.LOCK_HOLDER_APP_ID_KEY, "Unknown");
    lockNodeData.put("application_id", applicationId);
    this.lockNodeBytes = lockNodeData
        .entrySet()
        .stream()
        .map(entry -> entry.getKey() + "=" + entry.getValue())
        .collect(Collectors.joining(","))
        .getBytes(StandardCharsets.UTF_8);
  }

  @Override
  protected byte[] getLockNodeBytes() {
    return this.lockNodeBytes;
  }
}
