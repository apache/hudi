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
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hadoop.conf.Configuration;
import javax.annotation.concurrent.NotThreadSafe;
import static org.apache.hudi.common.config.LockConfiguration.ZK_BASE_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_LOCK_KEY_PROP_KEY;

/**
 * A zookeeper based lock. This {@link LockProvider} implementation allows to lock table operations
 * using zookeeper. Users need to have a Zookeeper cluster deployed to be able to use this lock.
 * The lock provider requires mandatory config "hoodie.write.lock.zookeeper.base_path" and
 * "hoodie.write.lock.zookeeper.lock_key" to be set.
 */
@NotThreadSafe
public class ZookeeperBasedLockProvider extends BaseZookeeperBasedLockProvider {

  public ZookeeperBasedLockProvider(final LockConfiguration lockConfiguration, final Configuration conf) {
    super(lockConfiguration, conf);
  }

  @Override
  protected String getZkBasePath(LockConfiguration lockConfiguration) {
    ValidationUtils.checkArgument(lockConfiguration.getConfig().getString(ZK_BASE_PATH_PROP_KEY) != null);
    return lockConfiguration.getConfig().getString(ZK_BASE_PATH_PROP_KEY);
  }

  @Override
  protected String getLockKey(LockConfiguration lockConfiguration) {
    ValidationUtils.checkArgument(lockConfiguration.getConfig().getString(ZK_LOCK_KEY_PROP_KEY) != null);
    return this.lockConfiguration.getConfig().getString(ZK_LOCK_KEY_PROP_KEY);
  }
}
