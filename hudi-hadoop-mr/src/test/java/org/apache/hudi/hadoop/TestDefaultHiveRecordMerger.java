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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.OrderingValues;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

class TestDefaultHiveRecordMerger {
  private final RecordContext<ArrayWritable> recordContext = mock(RecordContext.class);
  private final DefaultHiveRecordMerger merger = new DefaultHiveRecordMerger();

  static Stream<Arguments> storedNullOrderingValues() {
    return Stream.of(Arguments.of((Comparable) null), Arguments.of(OrderingValues.getDefault()));
  }

  @ParameterizedTest
  @MethodSource("storedNullOrderingValues")
  void testMergingWithStoredNullOrderingValue(Comparable storedOrderingValue) throws IOException {
    BufferedRecord<ArrayWritable> oldRecord = record(storedOrderingValue, 1L);
    BufferedRecord<ArrayWritable> newRecord = record(2L, 2L);

    assertSame(newRecord, merger.merge(oldRecord, newRecord, recordContext, new TypedProperties()));
  }

  @Test
  void testMergingKeepsOldRecordWithHigherOrderingValue() throws IOException {
    BufferedRecord<ArrayWritable> oldRecord = record(9L, 1L);
    BufferedRecord<ArrayWritable> newRecord = record(2L, 2L);

    assertSame(oldRecord, merger.merge(oldRecord, newRecord, recordContext, new TypedProperties()));
  }

  private static BufferedRecord<ArrayWritable> record(Comparable orderingValue, long value) {
    ArrayWritable writable = new ArrayWritable(Writable.class, new Writable[] {new LongWritable(value)});
    return new BufferedRecord<>("key0", orderingValue, writable, 0, null);
  }
}
