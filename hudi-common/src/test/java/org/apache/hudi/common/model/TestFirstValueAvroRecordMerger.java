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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

class TestFirstValueAvroRecordMerger {
  private TestableFirstValueAvroRecordMerger merger;
  private HoodieRecord oldRecord;
  private HoodieRecord newRecord;
  private Schema oldSchema;
  private Schema newSchema;
  private TypedProperties props;

  static class TestableFirstValueAvroRecordMerger extends FirstValueAvroRecordMerger {
    @Override
    protected boolean shouldKeepNewerRecord(HoodieRecord oldRecord, HoodieRecord newRecord,
                                            Schema oldSchema, Schema newSchema,
                                            TypedProperties props) throws IOException {
      return super.shouldKeepNewerRecord(oldRecord, newRecord, oldSchema, newSchema, props);
    }
  }

  @BeforeEach
  void setUp() {
    merger = new TestableFirstValueAvroRecordMerger();
    oldRecord = mock(HoodieRecord.class);
    newRecord = mock(HoodieRecord.class);
    oldSchema = mock(Schema.class);
    newSchema = mock(Schema.class);
    props = new TypedProperties();
  }

  @Test
  void testShouldKeepNewerRecordTrueWhenNewer() throws IOException {
    when(newRecord.getOrderingValue(newSchema, props)).thenReturn(200L);
    when(oldRecord.getOrderingValue(oldSchema, props)).thenReturn(100L);
    when(newRecord.isDelete(newSchema, props)).thenReturn(false);

    assertTrue(merger.shouldKeepNewerRecord(oldRecord, newRecord, oldSchema, newSchema, props));
  }

  @Test
  void testShouldKeepNewerRecordFalseWhenOlderOrEqual() throws IOException {
    when(newRecord.getOrderingValue(newSchema, props)).thenReturn(100L);
    when(oldRecord.getOrderingValue(oldSchema, props)).thenReturn(100L);
    when(newRecord.isDelete(newSchema, props)).thenReturn(false);

    assertFalse(merger.shouldKeepNewerRecord(oldRecord, newRecord, oldSchema, newSchema, props));

    when(newRecord.getOrderingValue(newSchema, props)).thenReturn(50L);
    assertFalse(merger.shouldKeepNewerRecord(oldRecord, newRecord, oldSchema, newSchema, props));
  }

  @Test
  void testShouldKeepNewerRecordTrueWhenDeleteWithDefaultOrdering() throws IOException {
    when(newRecord.getOrderingValue(newSchema, props)).thenReturn(DEFAULT_ORDERING_VALUE);
    when(newRecord.isDelete(newSchema, props)).thenReturn(true);

    assertTrue(merger.shouldKeepNewerRecord(oldRecord, newRecord, oldSchema, newSchema, props));
  }

  @Test
  void testGetMergingStrategy() {
    assertEquals(HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID, merger.getMergingStrategy());
  }
}
