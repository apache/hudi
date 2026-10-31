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

package org.apache.hudi.metadata;

import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieTableMetadataUtil.ColumnStatsCollector;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.generic.GenericData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestColumnStatsCollector {
  @ParameterizedTest
  @EnumSource(HoodieIndexVersion.class)
  void testIncrementalRangesWithReusedRecords(HoodieIndexVersion version) {
    HoodieSchema schema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"StatsRecord\",\"fields\":["
        + "{\"name\":\"score\",\"type\":[\"null\",\"int\"]},"
        + "{\"name\":\"name\",\"type\":[\"null\",\"string\"]}]}");
    List<Pair<String, HoodieSchemaField>> fields = Arrays.asList(
        Pair.of("score", schema.getField("score").get()), Pair.of("name", schema.getField("name").get()));
    StorageConfiguration<?> storageConfig = mock(StorageConfiguration.class);
    when(storageConfig.getString(anyString(), anyString())).thenAnswer(invocation -> invocation.getArgument(1));
    ColumnStatsCollector collector = new ColumnStatsCollector(fields, "file.lance", schema, storageConfig, version);
    assertEquals(0, collector.getColumnStats().get("score").getValueCount());
    assertNull(collector.getColumnStats().get("score").getMinValue());

    GenericData.Record row = new GenericData.Record(schema.toAvroSchema());
    row.put("score", 7);
    row.put("name", "alpha");
    collector.collect(new HoodieAvroIndexedRecord(row));
    Map<String, HoodieColumnRangeMetadata<Comparable>> firstSnapshot = collector.getColumnStats();
    row.put("score", -4);
    row.put("name", null);
    collector.collect(new HoodieAvroIndexedRecord(row));
    row.put("score", null);
    row.put("name", "omega");
    collector.collect(new HoodieAvroIndexedRecord(row));
    row.put("score", 100);

    Map<String, HoodieColumnRangeMetadata<Comparable>> stats = collector.getColumnStats();
    assertEquals(-4, stats.get("score").getMinValue());
    assertEquals(7, stats.get("score").getMaxValue());
    assertEquals(3, stats.get("score").getValueCount());
    assertEquals(1, stats.get("score").getNullCount());
    assertEquals("alpha", stats.get("name").getMinValue().toString());
    assertEquals("omega", stats.get("name").getMaxValue().toString());
    assertEquals(1, stats.get("name").getNullCount());
    assertEquals(1, firstSnapshot.get("score").getValueCount());
    assertEquals(7, firstSnapshot.get("score").getMinValue());
  }

  @ParameterizedTest
  @EnumSource(HoodieIndexVersion.class)
  void testBinaryBoundsDoNotRetainBorrowedBuffers(HoodieIndexVersion version) {
    HoodieSchema schema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"BinaryStats\",\"fields\":[{\"name\":\"data\",\"type\":\"bytes\"}]}");
    StorageConfiguration<?> storageConfig = mock(StorageConfiguration.class);
    when(storageConfig.getString(anyString(), anyString())).thenAnswer(invocation -> invocation.getArgument(1));
    ColumnStatsCollector collector = new ColumnStatsCollector(
        Collections.singletonList(Pair.of("data", schema.getField("data").get())), "file.lance", schema, storageConfig, version);
    byte[] bytes = {9};
    GenericData.Record row = new GenericData.Record(schema.toAvroSchema());
    row.put("data", ByteBuffer.wrap(bytes));
    collector.collect(new HoodieAvroIndexedRecord(row));
    bytes[0] = 1;
    collector.collect(new HoodieAvroIndexedRecord(row));
    bytes[0] = 6;
    assertEquals(ByteBuffer.wrap(new byte[] {1}), collector.getColumnStats().get("data").getMinValue());
    assertEquals(ByteBuffer.wrap(new byte[] {9}), collector.getColumnStats().get("data").getMaxValue());
  }
}
