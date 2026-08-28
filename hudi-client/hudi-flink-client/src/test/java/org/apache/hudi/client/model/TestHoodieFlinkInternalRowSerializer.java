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

package org.apache.hudi.client.model;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieFlinkInternalRowSerializer {

  private static final RowType ROW_TYPE = (RowType) DataTypes.ROW(
      DataTypes.FIELD("name", DataTypes.STRING()),
      DataTypes.FIELD("amount", DataTypes.DECIMAL(10, 2)),
      DataTypes.FIELD("created", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)),
      DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())))
      .getLogicalType();

  @Test
  void testRoundTripCopyAndStreamCopy() throws Exception {
    HoodieFlinkInternalRowSerializer serializer = new HoodieFlinkInternalRowSerializer(ROW_TYPE);
    GenericRowData payload = GenericRowData.of(
        StringData.fromString("alice"),
        DecimalData.fromBigDecimal(new BigDecimal("12.34"), 10, 2),
        TimestampData.fromInstant(Instant.parse("2025-02-03T04:05:06.123456Z")),
        new GenericArrayData(new Object[] {StringData.fromString("x"), null}));
    HoodieFlinkInternalRow original = new HoodieFlinkInternalRow(
        "key", "partition", "file", "instant", "I", false, payload);

    DataOutputSerializer output = new DataOutputSerializer(128);
    serializer.serialize(original, output);
    HoodieFlinkInternalRow restored = serializer.deserialize(
        new DataInputDeserializer(output.getCopyOfBuffer()));
    assertDataRecord(restored);

    HoodieFlinkInternalRow copied = serializer.copy(original);
    assertNotSame(original, copied);
    assertNotSame(original.getRowData(), copied.getRowData());
    assertDataRecord(copied);

    DataOutputSerializer copiedOutput = new DataOutputSerializer(128);
    serializer.copy(new DataInputDeserializer(output.getCopyOfBuffer()), copiedOutput);
    assertDataRecord(serializer.deserialize(new DataInputDeserializer(copiedOutput.getCopyOfBuffer())));
  }

  @Test
  void testIndexRecordRoundTripAndSerializerContract() throws Exception {
    HoodieFlinkInternalRowSerializer serializer = new HoodieFlinkInternalRowSerializer(ROW_TYPE);
    HoodieFlinkInternalRow indexRecord =
        new HoodieFlinkInternalRow("key", "partition", "file", "instant");
    DataOutputSerializer output = new DataOutputSerializer(64);
    serializer.serialize(indexRecord, output);

    HoodieFlinkInternalRow restored = serializer.deserialize(
        indexRecord, new DataInputDeserializer(output.getCopyOfBuffer()));
    assertTrue(restored.isIndexRecord());
    assertEquals("key", restored.getRecordKey());
    assertEquals("partition", restored.getPartitionPath());
    assertEquals("file", restored.getFileId());
    assertEquals("instant", restored.getInstantTime());
    assertEquals("", restored.getOperationType());

    DataOutputSerializer copiedOutput = new DataOutputSerializer(64);
    serializer.copy(new DataInputDeserializer(output.getCopyOfBuffer()), copiedOutput);
    assertTrue(serializer.deserialize(
        new DataInputDeserializer(copiedOutput.getCopyOfBuffer())).isIndexRecord());

    assertFalse(serializer.isImmutableType());
    assertEquals(-1, serializer.getLength());
    assertEquals(serializer, serializer.duplicate());
    assertEquals(serializer.hashCode(), serializer.duplicate().hashCode());
    assertFalse(serializer.equals(null));
    assertFalse(serializer.equals("serializer"));
    assertThrows(UnsupportedOperationException.class, serializer::createInstance);
    assertThrows(UnsupportedOperationException.class,
        () -> serializer.copy(indexRecord, indexRecord));
    assertThrows(UnsupportedOperationException.class, serializer::snapshotConfiguration);
  }

  private static void assertDataRecord(HoodieFlinkInternalRow record) {
    assertFalse(record.isIndexRecord());
    assertEquals("key", record.getRecordKey());
    assertEquals("partition", record.getPartitionPath());
    assertEquals("file", record.getFileId());
    assertEquals("instant", record.getInstantTime());
    assertEquals("I", record.getOperationType());
    assertEquals("alice", record.getRowData().getString(0).toString());
    assertEquals(new BigDecimal("12.34"), record.getRowData().getDecimal(1, 10, 2).toBigDecimal());
    assertEquals(Instant.parse("2025-02-03T04:05:06.123456Z"),
        record.getRowData().getTimestamp(2, 6).toInstant());
    assertEquals("x", record.getRowData().getArray(3).getString(0).toString());
    assertTrue(record.getRowData().getArray(3).isNullAt(1));
  }
}
