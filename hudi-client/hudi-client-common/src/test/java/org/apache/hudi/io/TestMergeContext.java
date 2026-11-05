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

package org.apache.hudi.io;

import org.apache.hudi.common.model.HoodieRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class TestMergeContext {

  @Test
  void testCreateWithIteratorOnlyDefaultsToUnknown() {
    Iterator<HoodieRecord<Object>> itr = Collections.emptyIterator();
    MergeContext<Object> ctx = MergeContext.create(itr);
    assertEquals(MergeContext.UNKNOWN_NUM_UPDATES, ctx.getNumUpdates());
    assertSame(itr, ctx.getRecordIterator());
  }

  @ParameterizedTest
  @ValueSource(longs = {MergeContext.UNKNOWN_NUM_UPDATES, 0L, 100L})
  void testCreateWithExplicitNumUpdates(long numUpdates) {
    Iterator<HoodieRecord<Object>> itr = Collections.emptyIterator();
    MergeContext<Object> ctx = MergeContext.create(numUpdates, itr);
    assertEquals(numUpdates, ctx.getNumUpdates());
    assertSame(itr, ctx.getRecordIterator());
  }
}
