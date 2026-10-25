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

package org.apache.hudi.client.transaction.lock;

import org.apache.hudi.CompatAlias;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.storage.StorageConfiguration;

/**
 * Compatibility alias for {@link org.apache.hudi.core.transaction.lock.InProcessLockProvider},
 * kept only so existing {@code hoodie.write.lock.provider} configs referencing the old class
 * name keep resolving. Holds no logic; do not use in new code.
 *
 * @deprecated use {@link org.apache.hudi.core.transaction.lock.InProcessLockProvider} instead.
 */
@Deprecated
@CompatAlias(of = org.apache.hudi.core.transaction.lock.InProcessLockProvider.class, since = "1.3.0")
public class InProcessLockProvider extends org.apache.hudi.core.transaction.lock.InProcessLockProvider {

  public InProcessLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
    super(lockConfiguration, conf);
  }
}
