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

package org.apache.hudi.azure.transaction.lock;

import org.apache.hudi.client.transaction.lock.StorageBasedLockProvider;
import org.apache.hudi.client.transaction.lock.StorageBasedLockProviderTestBase;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.testutils.HoodieTestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

import static org.apache.hudi.common.config.HoodieCommonConfig.BASE_PATH;

@Disabled("Requires Azurite/Testcontainers-based integration environment (not enabled by default).")
public class TestADLSStorageBasedLockProvider extends StorageBasedLockProviderTestBase {

  @BeforeEach
  void setupLockProvider() {
    providerProperties.put(BASE_PATH.key(),
        "abfs://container@account.dfs.core.windows.net/lake/db/tbl-default");
    lockProvider = createLockProvider();
  }

  @Override
  protected StorageBasedLockProvider createLockProvider() {
    LockConfiguration lockConf = new LockConfiguration(providerProperties);
    return new StorageBasedLockProvider(lockConf, HoodieTestUtils.getDefaultStorageConf());
  }
}
