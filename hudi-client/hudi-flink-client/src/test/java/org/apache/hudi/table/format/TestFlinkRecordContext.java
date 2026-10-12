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

package org.apache.hudi.table.format;

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapIntVector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestFlinkRecordContext {

  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("test_record", null, null, Arrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.INT))));

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testConstructDeleteRecord(boolean hasRowData) {
    HoodieSchema schema = HoodieSchema.createRecord("delete_record", null, null,
        Collections.singletonList(HoodieSchemaField.of("ts", HoodieSchema.createTimestampMillis())));
    TimestampData orderingValue = TimestampData.fromEpochMillis(60);
    RowData row = hasRowData ? GenericRowData.of(orderingValue) : null;
    FlinkRecordContext context = FlinkRecordContext.getDeleteCheckingInstance();
    BufferedRecord<RowData> buffered = new BufferedRecord<>("id1", orderingValue, row, context.encodeSchema(schema), HoodieOperation.DELETE);

    HoodieRecord<RowData> record = context.constructHoodieRecord(buffered, "par1").newInstance(new HoodieKey("id1", "par2"));

    assertEquals(hasRowData ? HoodieFlinkRecord.class : HoodieEmptyRecord.class, record.getClass());
    assertSame(row, record.getData());
    assertEquals(HoodieOperation.DELETE, record.getOperation());
    assertTrue(record.isDelete(null, new Properties()));
    if (hasRowData) {
      assertEquals(60L, record.getOrderingValueAsJava(schema, new Properties(), new String[] {"ts"}));
    }
    assertSame(orderingValue, record.getOrderingValue(schema, new Properties(), new String[] {"ts"}));
    BufferedRecord<RowData> roundTrip = BufferedRecords.fromHoodieRecord(record, schema, context, new Properties(), new String[] {"ts"}, true);
    assertTrue(roundTrip.isDelete());
    assertSame(row, roundTrip.getRecord());
    assertSame(orderingValue, roundTrip.getOrderingValue());
  }

  @Test
  void testSealDoesNotCopyGenericRowData() {
    GenericRowData rowData = new GenericRowData(2);

    assertSame(rowData, FlinkRecordContext.getDeleteCheckingInstance().seal(SCHEMA, rowData));
  }

  @Test
  void testSealCopiesReusedColumnarRowData() {
    HeapBytesVector idVector = new HeapBytesVector(2);
    byte[] id1 = "id1".getBytes(StandardCharsets.UTF_8);
    byte[] id2 = "id2".getBytes(StandardCharsets.UTF_8);
    idVector.appendBytes(0, id1, 0, id1.length);
    idVector.appendBytes(1, id2, 0, id2.length);
    HeapIntVector valueVector = new HeapIntVector(2);
    valueVector.setInt(0, 1);
    valueVector.setInt(1, 2);
    VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {idVector, valueVector});
    batch.setNumRows(2);
    ColumnarRowData reusedRow = new ColumnarRowData(batch, 0);

    RowData sealed = FlinkRecordContext.getDeleteCheckingInstance().seal(SCHEMA, reusedRow);
    reusedRow.setRowId(1);

    assertNotSame(reusedRow, sealed);
    assertEquals("id1", sealed.getString(0).toString());
    assertEquals(1, sealed.getInt(1));
  }
}
