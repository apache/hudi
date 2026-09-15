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

package org.apache.hudi.aws.transaction.lock;

import org.apache.hudi.common.util.hash.HashID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Exercises {@link DynamoDBBasedImplicitPartitionKeyLockProvider#derivePartitionKey} as a pure
 * function - no DynamoDB client required.
 *
 * <p>Two writers on the same Hudi table must derive the same DynamoDB partition key, or they take
 * independent locks and lose mutual exclusion. This class pins both halves of that contract: the
 * exact key produced for the canonical base path (so the formula cannot move silently), and the
 * set of benign formatting variants that must fold onto it.
 */
class TestDynamoDBBasedImplicitPartitionKeyLockProvider {

  private static final String CANONICAL_BASE_PATH = "s3://my-bucket/my_lake/my_table";

  /**
   * Golden value. Deliberately a literal rather than a recomputation: an equality-only test
   * ({@code key(a).equals(key(b))}) still passes if the whole derivation changes, which is
   * exactly how a lock-key scheme change ships undetected.
   */
  private static final String CANONICAL_PARTITION_KEY = "C0E15D0CE1AD11CC";

  @Test
  void derivesThePinnedPartitionKeyForTheCanonicalBasePath() {
    assertEquals(CANONICAL_PARTITION_KEY,
        DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(CANONICAL_BASE_PATH));
  }

  @Test
  void basePathWithoutTrailingSlashKeepsThePreNormalizationKey() {
    // Releases before the normalization fix hashed s3aToS3(basePath) directly. For the common
    // no-trailing-slash form the canonicalized input is byte-identical, so the partition key must
    // not move - otherwise every deployed lock row is orphaned on upgrade.
    assertEquals(HashID.generateXXHashAsString(CANONICAL_BASE_PATH, HashID.Size.BITS_64),
        DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(CANONICAL_BASE_PATH));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "s3://my-bucket/my_lake/my_table/",
      "s3://my-bucket/my_lake/my_table//",
      "s3://my-bucket/my_lake/my_table///",
      "  s3://my-bucket/my_lake/my_table  ",
      "\ts3://my-bucket/my_lake/my_table/\n",
      // Whitespace in front of the trailing slash: the strip must consume both, otherwise a
      // trailing space survives and this hashes to a different row than the canonical form.
      "s3://my-bucket/my_lake/my_table /",
      "s3a://my-bucket/my_lake/my_table",
      "s3a://my-bucket/my_lake/my_table/",
      "S3A://my-bucket/my_lake/my_table//",
  })
  void benignFormattingVariantsFoldOntoTheCanonicalPartitionKey(String basePath) {
    assertEquals(CANONICAL_PARTITION_KEY,
        DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(basePath));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   ", "/", "///", "s3://", "s3a:///"})
  void unlockableBasePathsAreRejected(String basePath) {
    assertThrows(IllegalArgumentException.class,
        () -> DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(basePath));
  }
}
