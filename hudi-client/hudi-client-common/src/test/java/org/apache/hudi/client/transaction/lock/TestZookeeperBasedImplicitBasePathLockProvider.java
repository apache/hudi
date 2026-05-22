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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link ZookeeperBasedImplicitBasePathLockProvider#getLockBasePath} as a pure
 * function — no Zookeeper server required. Verifies that benign formatting variations in the
 * hudi table base path (trailing slash, multi-slash, whitespace, s3a vs s3 scheme) all produce
 * the same Zookeeper lock base path. Without this invariant, two engines writing the same Hudi
 * table can take independent locks and lose mutual exclusion.
 */
class TestZookeeperBasedImplicitBasePathLockProvider {

  private static final String BASE_PATH_WITH_SLASH = "s3://my-bucket/my_lake/my_table/";
  private static final String BASE_PATH_NO_SLASH = "s3://my-bucket/my_lake/my_table";

  @Test
  void trailingSlashVariantsProduceSameLockBasePath() {
    String withSlash = ZookeeperBasedImplicitBasePathLockProvider.getLockBasePath(BASE_PATH_WITH_SLASH);
    String noSlash = ZookeeperBasedImplicitBasePathLockProvider.getLockBasePath(BASE_PATH_NO_SLASH);
    Assertions.assertEquals(withSlash, noSlash);
    Assertions.assertTrue(withSlash.startsWith("/tmp/"), "Lock path must keep its /tmp/ prefix");
  }

  @Test
  void multipleTrailingSlashesProduceSameLockBasePath() {
    String once = ZookeeperBasedImplicitBasePathLockProvider.getLockBasePath(BASE_PATH_WITH_SLASH);
    String twice = ZookeeperBasedImplicitBasePathLockProvider.getLockBasePath(BASE_PATH_NO_SLASH + "//");
    String thrice = ZookeeperBasedImplicitBasePathLockProvider.getLockBasePath(BASE_PATH_NO_SLASH + "///");
    Assertions.assertEquals(once, twice);
    Assertions.assertEquals(once, thrice);
  }

  @Test
  void surroundingWhitespaceProducesSameLockBasePath() {
    String clean = ZookeeperBasedImplicitBasePathLockProvider.getLockBasePath(BASE_PATH_WITH_SLASH);
    String padded = ZookeeperBasedImplicitBasePathLockProvider.getLockBasePath("  " + BASE_PATH_NO_SLASH + "  ");
    Assertions.assertEquals(clean, padded);
  }

  @Test
  void s3aSchemeProducesSameLockBasePathAsS3() {
    String s3 = ZookeeperBasedImplicitBasePathLockProvider.getLockBasePath(BASE_PATH_WITH_SLASH);
    String s3a = ZookeeperBasedImplicitBasePathLockProvider.getLockBasePath(
        BASE_PATH_WITH_SLASH.replaceFirst("^s3://", "s3a://"));
    Assertions.assertEquals(s3, s3a);
  }
}
