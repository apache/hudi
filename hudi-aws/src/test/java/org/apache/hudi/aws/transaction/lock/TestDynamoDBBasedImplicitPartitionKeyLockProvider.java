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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link DynamoDBBasedImplicitPartitionKeyLockProvider#derivePartitionKey} as a pure
 * function — no DynamoDB client required. Verifies that benign formatting variations in the
 * hudi table base path (trailing slash, multi-slash, whitespace, s3a vs s3 scheme) all produce
 * the same DynamoDB partition key. Without this invariant, two engines writing the same Hudi
 * table can take independent locks and lose mutual exclusion.
 */
class TestDynamoDBBasedImplicitPartitionKeyLockProvider {

  private static final String BASE_PATH_WITH_SLASH = "s3://my-bucket/my_lake/my_table/";
  private static final String BASE_PATH_NO_SLASH = "s3://my-bucket/my_lake/my_table";

  @Test
  void trailingSlashVariantsProduceSamePartitionKey() {
    String withSlash = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(BASE_PATH_WITH_SLASH);
    String noSlash = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(BASE_PATH_NO_SLASH);
    Assertions.assertEquals(withSlash, noSlash);
  }

  @Test
  void multipleTrailingSlashesProduceSamePartitionKey() {
    String once = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(BASE_PATH_WITH_SLASH);
    String twice = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(BASE_PATH_NO_SLASH + "//");
    String thrice = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(BASE_PATH_NO_SLASH + "///");
    Assertions.assertEquals(once, twice);
    Assertions.assertEquals(once, thrice);
  }

  @Test
  void surroundingWhitespaceProducesSamePartitionKey() {
    String clean = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(BASE_PATH_WITH_SLASH);
    String padded = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey("  " + BASE_PATH_NO_SLASH + "  ");
    Assertions.assertEquals(clean, padded);
  }

  @Test
  void s3aSchemeProducesSamePartitionKeyAsS3() {
    String s3 = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(BASE_PATH_WITH_SLASH);
    String s3a = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(
        BASE_PATH_WITH_SLASH.replaceFirst("^s3://", "s3a://"));
    Assertions.assertEquals(s3, s3a);
  }
}
