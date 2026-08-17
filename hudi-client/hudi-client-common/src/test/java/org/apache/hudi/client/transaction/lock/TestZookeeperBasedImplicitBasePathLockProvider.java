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

import org.apache.hudi.common.util.hash.HashID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Exercises {@link ZookeeperBasedImplicitBasePathLockProvider#getLockBasePath} as a pure function
 * - no Zookeeper server required. See
 * {@code TestZookeeperBasedLockProvider#testTrailingSlashBasePathContendsForTheSameLock} for the
 * end-to-end proof that two providers deriving the same znode actually exclude each other.
 *
 * <p>The lock base path is a znode that deployed clusters hold, so this pins the exact string
 * rather than only asserting that variants agree with each other.
 */
class TestZookeeperBasedImplicitBasePathLockProvider {

  private static final String CANONICAL_BASE_PATH = "s3://my-bucket/my_lake/my_table";

  /**
   * Golden value. Deliberately a literal rather than a recomputation: an equality-only test
   * ({@code path(a).equals(path(b))}) still passes if the whole derivation changes, which is
   * exactly how a lock-key scheme change ships undetected.
   */
  private static final String CANONICAL_LOCK_BASE_PATH = "/tmp/C0E15D0CE1AD11CC";

  @Test
  void derivesThePinnedLockBasePathForTheCanonicalBasePath() {
    assertEquals(CANONICAL_LOCK_BASE_PATH,
        ZookeeperBasedImplicitBasePathLockProvider.getLockBasePath(CANONICAL_BASE_PATH));
  }

  @Test
  void aBasePathWithoutTrailingSlashKeepsThePreNormalizationZnode() {
    // Releases before the normalization fix hashed s3aToS3(basePath) directly. For the common
    // no-trailing-slash form the canonicalized input is byte-identical, so the znode must not
    // move - otherwise every in-flight lock is orphaned on upgrade.
    assertEquals("/tmp/" + HashID.generateXXHashAsString(CANONICAL_BASE_PATH, HashID.Size.BITS_64),
        ZookeeperBasedImplicitBasePathLockProvider.getLockBasePath(CANONICAL_BASE_PATH));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "s3://my-bucket/my_lake/my_table/",
      "s3://my-bucket/my_lake/my_table//",
      "s3://my-bucket/my_lake/my_table///",
      "  s3://my-bucket/my_lake/my_table  ",
      "\ts3://my-bucket/my_lake/my_table/\n",
      // Whitespace in front of the trailing slash: the strip must consume both, otherwise a
      // trailing space survives and this hashes to a different znode than the canonical form.
      "s3://my-bucket/my_lake/my_table /",
      "s3a://my-bucket/my_lake/my_table",
      "s3a://my-bucket/my_lake/my_table/",
      "S3A://my-bucket/my_lake/my_table//",
  })
  void benignFormattingVariantsFoldOntoTheCanonicalLockBasePath(String basePath) {
    assertEquals(CANONICAL_LOCK_BASE_PATH,
        ZookeeperBasedImplicitBasePathLockProvider.getLockBasePath(basePath));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   ", "/", "///", "s3://", "s3a:///"})
  void unlockableBasePathsAreRejected(String basePath) {
    assertThrows(IllegalArgumentException.class,
        () -> ZookeeperBasedImplicitBasePathLockProvider.getLockBasePath(basePath));
  }
}
