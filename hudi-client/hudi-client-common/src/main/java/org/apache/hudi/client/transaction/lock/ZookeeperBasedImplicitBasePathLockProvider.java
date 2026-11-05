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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.hash.HashID;
import org.apache.hudi.storage.StorageConfiguration;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.hudi.common.fs.FSUtils.normalizeBasePathForLocking;
import static org.apache.hudi.common.fs.FSUtils.s3aToS3;

/**
 * A zookeeper based lock. This {@link LockProvider} implementation allows to lock table operations
 * using zookeeper. Users need to have a Zookeeper cluster deployed to be able to use this lock.
 *
 * This class derives the zookeeper base path from the hudi table base path (hoodie.base.path) and
 * table name (hoodie.table.name), with lock key set to a hard-coded value.
 */
@NotThreadSafe
@Slf4j
public class ZookeeperBasedImplicitBasePathLockProvider extends BaseZookeeperBasedLockProvider {

  public static final String LOCK_KEY = "lock_key";
  private final String normalizedHudiTableBasePath;

  /**
   * Compute the Zookeeper lock base path for a given Hudi table base path.
   *
   * <p>Accepts a raw basePath - normalization is applied here. {@code normalizeBasePathForLocking}
   * is idempotent, so an already-normalized value can be passed through without harm.
   *
   * <p>ROLLOUT: for a table whose configured {@code hoodie.base.path} ends in '/' or carries
   * surrounding whitespace, this returns a different znode than releases before HUDI's
   * normalization fix. Such a table must have all of its writers upgraded together - a rolling
   * upgrade would leave old and new writers holding two different znodes for the same table,
   * losing mutual exclusion. Base paths without a trailing slash are unaffected.
   */
  public static String getLockBasePath(String hudiTableBasePath) {
    String normalized = normalizeBasePathForLocking(hudiTableBasePath);
    String lockBasePath = "/tmp/" + HashID.generateXXHashAsString(normalized, HashID.Size.BITS_64);
    log.info("The Zookeeper lock key for the base path {} (normalized: {}) is {}",
        hudiTableBasePath, normalized, lockBasePath);
    // Releases before this change hashed s3aToS3(basePath) directly. When the canonical form
    // differs, this writer has moved to a new znode and cannot exclude a writer still running
    // the old code, so say so loudly rather than leaving it to be inferred from the INFO line.
    String legacyForm = s3aToS3(hudiTableBasePath);
    if (!legacyForm.equals(normalized)) {
      log.warn("Zookeeper lock key for base path {} moved from {} to {}. Every writer of this "
              + "table must be upgraded together; a writer still on the previous release locks "
              + "on the old znode and will NOT be excluded by this one.",
          hudiTableBasePath,
          "/tmp/" + HashID.generateXXHashAsString(legacyForm, HashID.Size.BITS_64),
          lockBasePath);
    }
    return lockBasePath;
  }

  public ZookeeperBasedImplicitBasePathLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
    super(lockConfiguration, conf);
    normalizedHudiTableBasePath = normalizeBasePathForLocking(
        lockConfiguration.getConfig().getString(HoodieCommonConfig.BASE_PATH.key()));
  }

  @Override
  protected String getZkBasePath(LockConfiguration lockConfiguration) {
    // No explicit null check: TypedProperties#getString already throws IllegalArgumentException
    // for a missing key, and getLockBasePath rejects null/blank/unlockable paths.
    return getLockBasePath(lockConfiguration.getConfig().getString(HoodieCommonConfig.BASE_PATH.key()));
  }

  @Override
  protected String getLockKey(LockConfiguration lockConfiguration) {
    return LOCK_KEY;
  }

  @Override
  protected String generateLogSuffixString() {
    return StringUtils.join("ZkBasePath = ", zkBasePath,
        ", lock key = ", lockKey, ", hudi table base path = ", normalizedHudiTableBasePath);
  }
}
