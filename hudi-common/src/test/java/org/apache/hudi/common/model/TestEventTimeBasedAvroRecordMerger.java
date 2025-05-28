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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestEventTimeBasedAvroRecordMerger {
  private EventTimeBasedAvroRecordMerger merger;
  private HoodieRecord oldRecord;
  private HoodieRecord newRecord;
  private Schema oldSchema;
  private Schema newSchema;
  private TypedProperties props;

  @BeforeEach
  void setUp() {
    merger = EventTimeBasedAvroRecordMerger.INSTANCE;
    oldRecord = mock(HoodieRecord.class);
    newRecord = mock(HoodieRecord.class);
    oldSchema = mock(Schema.class);
    newSchema = mock(Schema.class);
    props = new TypedProperties();
  }

  @Test
  void testNewerRecordIsKept() throws IOException {
    when(oldRecord.getOrderingValue(oldSchema, props)).thenReturn(100L);
    when(newRecord.getOrderingValue(newSchema, props)).thenReturn(200L);
    when(newRecord.isDelete(newSchema, props)).thenReturn(false);

    Option<Pair<HoodieRecord, Schema>> result = merger.merge(oldRecord, oldSchema, newRecord, newSchema, props);
    assertTrue(result.isPresent());
    assertEquals(newRecord, result.get().getLeft());
  }

  @Test
  void testOlderRecordIsKept() throws IOException {
    when(oldRecord.getOrderingValue(oldSchema, props)).thenReturn(200L);
    when(newRecord.getOrderingValue(newSchema, props)).thenReturn(100L);
    when(newRecord.isDelete(newSchema, props)).thenReturn(false);

    Option<Pair<HoodieRecord, Schema>> result = merger.merge(oldRecord, oldSchema, newRecord, newSchema, props);
    assertEquals(oldRecord, result.get().getLeft());
  }

  @Test
  void testDeleteRecordWithDefaultOrderingIsKept() throws IOException {
    when(newRecord.getOrderingValue(newSchema, props)).thenReturn(HoodieRecord.DEFAULT_ORDERING_VALUE);
    when(newRecord.isDelete(newSchema, props)).thenReturn(true);

    Option<Pair<HoodieRecord, Schema>> result = merger.merge(oldRecord, oldSchema, newRecord, newSchema, props);
    assertTrue(result.isPresent());
    assertEquals(newRecord, result.get().getLeft());
  }

  @Test
  void testRecordType() {
    assertEquals(HoodieRecord.HoodieRecordType.AVRO, merger.getRecordType());
  }

  @Test
  void testMergingStrategy() {
    assertEquals(HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID, merger.getMergingStrategy());
  }
}
