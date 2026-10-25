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

package org.apache.hudi.hive.transaction.lock;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.apache.hudi.common.config.LockConfiguration.HIVE_METASTORE_URI_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECT_URL_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_PORT_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_SESSION_TIMEOUT_MS_PROP_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestHiveMetastoreBasedLockProviderConfiguration extends HiveMetastoreBasedLockProviderTestBase {

  @Test
  void appliesOptionalMetastoreAndZooKeeperSettings() throws Exception {
    lockConfiguration.getConfig().setProperty(HIVE_METASTORE_URI_PROP_KEY, "thrift://localhost:9083");
    lockConfiguration.getConfig().setProperty(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, "4");
    lockConfiguration.getConfig().setProperty(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "25");
    lockConfiguration.getConfig().setProperty(ZK_CONNECT_URL_PROP_KEY, "zk-a,zk-b");
    lockConfiguration.getConfig().setProperty(ZK_PORT_PROP_KEY, "2182");
    lockConfiguration.getConfig().setProperty(ZK_SESSION_TIMEOUT_MS_PROP_KEY, "45000");

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(
        lockConfiguration, mock(IMetaStoreClient.class));
    try {
      HiveConf hiveConf = new HiveConf();
      Method method = HiveMetastoreBasedLockProvider.class.getDeclaredMethod("setHiveLockConfs", HiveConf.class);
      method.setAccessible(true);
      method.invoke(provider, hiveConf);

      assertEquals("thrift://localhost:9083", hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
      assertEquals("zk-a,zk-b", hiveConf.get("hive.zookeeper.quorum"));
      assertEquals("2182", hiveConf.get("hive.zookeeper.client.port"));
      assertEquals("45000", hiveConf.get("hive.zookeeper.session.timeout"));
      assertEquals("4", hiveConf.get("hive.lock.numretries"));
      assertEquals("25", hiveConf.get("hive.lock.sleep.between.retries"));
      assertTrue(hiveConf.getBoolean("hive.support.concurrency", false));
    } finally {
      provider.close();
    }
  }
}
