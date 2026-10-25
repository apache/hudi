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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests the small result enums used by the storage-based lock provider.
 */
public class TestLockResultEnums {

  @Test
  void lockGetResultCodesAreStable() {
    assertEquals(0, LockGetResult.NOT_EXISTS.getCode());
    assertEquals(1, LockGetResult.SUCCESS.getCode());
    assertEquals(2, LockGetResult.UNKNOWN_ERROR.getCode());
    // Codes must be unique so callers can map them one-to-one.
    assertEquals(3, LockGetResult.values().length);
  }

  @Test
  void lockGetResultValueOfRoundTrips() {
    for (LockGetResult result : LockGetResult.values()) {
      assertSame(result, LockGetResult.valueOf(result.name()));
    }
  }

  @Test
  void lockUpsertResultCodesAreStable() {
    assertEquals(0, LockUpsertResult.SUCCESS.getCode());
    assertEquals(1, LockUpsertResult.ACQUIRED_BY_OTHERS.getCode());
    assertEquals(2, LockUpsertResult.UNKNOWN_ERROR.getCode());
    assertEquals(3, LockUpsertResult.THROTTLED.getCode());
    assertEquals(4, LockUpsertResult.values().length);
  }

  @Test
  void lockUpsertResultValueOfRoundTrips() {
    for (LockUpsertResult result : LockUpsertResult.values()) {
      assertSame(result, LockUpsertResult.valueOf(result.name()));
    }
  }
}
