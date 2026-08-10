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

package org.apache.hudi.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.avro.generic.GenericData;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAvroToRowDataConverters {

  @Test
  public void testConvertVectorFixedBytesToArrays() {
    HoodieSchema floatVector = HoodieSchema.createVector(2);
    HoodieSchema doubleVector = HoodieSchema.createVector(2, HoodieSchema.Vector.VectorElementType.DOUBLE);
    HoodieSchema int8Vector = HoodieSchema.createVector(3, HoodieSchema.Vector.VectorElementType.INT8);
    HoodieSchema schema = HoodieSchema.createRecord("VectorRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("float_vec", floatVector),
        HoodieSchemaField.of("double_vec", doubleVector),
        HoodieSchemaField.of("int8_vec", int8Vector)));

    GenericData.Record record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", 1);
    record.put("float_vec", new GenericData.Fixed(floatVector.toAvroSchema(), vectorBytes(new float[] {1.0f, 2.5f})));
    record.put("double_vec", new GenericData.Fixed(doubleVector.toAvroSchema(), vectorBytes(new double[] {3.0d, 4.5d})));
    record.put("int8_vec", new GenericData.Fixed(int8Vector.toAvroSchema(), new byte[] {1, 2, 3}));

    RowData rowData = (RowData) RowDataQueryContexts.fromSchema(schema).getAvroToRowDataConverter().convert(record);

    assertEquals(1, rowData.getInt(0));
    ArrayData floatArray = rowData.getArray(1);
    assertEquals(2, floatArray.size());
    assertEquals(1.0f, floatArray.getFloat(0), 0.0f);
    assertEquals(2.5f, floatArray.getFloat(1), 0.0f);
    ArrayData doubleArray = rowData.getArray(2);
    assertEquals(2, doubleArray.size());
    assertEquals(3.0d, doubleArray.getDouble(0), 0.0d);
    assertEquals(4.5d, doubleArray.getDouble(1), 0.0d);
    ArrayData int8Array = rowData.getArray(3);
    assertEquals(3, int8Array.size());
    assertEquals((byte) 1, int8Array.getByte(0));
    assertEquals((byte) 2, int8Array.getByte(1));
    assertEquals((byte) 3, int8Array.getByte(2));
  }

  @Test
  public void testConvertNullableVector() {
    HoodieSchema nullableVector = HoodieSchema.createNullable(HoodieSchema.createVector(2));
    HoodieSchema schema = HoodieSchema.createRecord("NullableVectorRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("embedding", nullableVector)));

    GenericData.Record record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", 1);
    record.put("embedding", null);

    RowData rowData = (RowData) RowDataQueryContexts.fromSchema(schema).getAvroToRowDataConverter().convert(record);

    assertEquals(1, rowData.getInt(0));
    assertTrue(rowData.isNullAt(1));
  }

  @Test
  public void testConvertVectorWithWrongByteLengthFails() {
    HoodieSchema vector = HoodieSchema.createVector(2);
    HoodieSchema schema = HoodieSchema.createRecord("InvalidVectorRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("embedding", vector)));

    GenericData.Record record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", 1);
    record.put("embedding", ByteBuffer.wrap(new byte[] {1, 2, 3}));

    assertThrows(IllegalArgumentException.class,
        () -> RowDataQueryContexts.fromSchema(schema).getAvroToRowDataConverter().convert(record));
  }

  private static byte[] vectorBytes(float[] values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * Float.BYTES)
        .order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    for (float value : values) {
      buffer.putFloat(value);
    }
    return buffer.array();
  }

  private static byte[] vectorBytes(double[] values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * Double.BYTES)
        .order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    for (double value : values) {
      buffer.putDouble(value);
    }
    return buffer.array();
  }
}
