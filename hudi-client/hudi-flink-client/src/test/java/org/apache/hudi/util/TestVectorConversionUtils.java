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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.SchemaCompatibilityException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestVectorConversionUtils {

  private static final HoodieSchema.Vector FLOAT_VECTOR = HoodieSchema.createVector(2);
  private static final HoodieSchema.Vector DOUBLE_VECTOR = HoodieSchema.createVector(
      2, HoodieSchema.Vector.VectorElementType.DOUBLE);
  private static final HoodieSchema.Vector INT8_VECTOR = HoodieSchema.createVector(
      3, HoodieSchema.Vector.VectorElementType.INT8);

  @Test
  void testDetectAndRewriteVectorColumns() {
    HoodieSchema schema = vectorRecordSchema();
    String[] names = {"id", "Float_Vec", "double_vec", "codes"};
    int[] selected = {3, 0, 1};
    Map<Integer, HoodieSchema.Vector> detected =
        VectorConversionUtils.detectVectorColumns(names, selected, schema);
    assertEquals(INT8_VECTOR, detected.get(0));
    assertEquals(FLOAT_VECTOR, detected.get(2));
    assertFalse(detected.containsKey(1));

    DataType requested = DataTypes.ROW(
        DataTypes.FIELD("codes", DataTypes.ARRAY(DataTypes.TINYINT()).notNull()),
        DataTypes.FIELD("id", DataTypes.INT().notNull()),
        DataTypes.FIELD("float_vec", DataTypes.ARRAY(DataTypes.FLOAT()))).notNull();
    HoodieSchema projectedSchema = HoodieSchema.createRecord("projected", null, null, Arrays.asList(
        HoodieSchemaField.of("codes", INT8_VECTOR),
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("float_vec", FLOAT_VECTOR)));
    DataType physical = VectorConversionUtils.getParquetReadDataType(
        requested, projectedSchema, detected);
    RowType physicalRow = (RowType) physical.getLogicalType();
    assertEquals(LogicalTypeRoot.VARBINARY, physicalRow.getTypeAt(0).getTypeRoot());
    assertEquals(LogicalTypeRoot.INTEGER, physicalRow.getTypeAt(1).getTypeRoot());
    assertEquals(LogicalTypeRoot.VARBINARY, physicalRow.getTypeAt(2).getTypeRoot());
    assertFalse(physicalRow.getTypeAt(0).isNullable());
    assertSame(requested, VectorConversionUtils.getParquetReadDataType(
        requested, projectedSchema, Collections.emptyMap()));

    DataType[] fieldTypes = {
        DataTypes.INT(), DataTypes.ARRAY(DataTypes.FLOAT()),
        DataTypes.ARRAY(DataTypes.DOUBLE()), DataTypes.ARRAY(DataTypes.TINYINT())};
    DataType[] physicalTypes = VectorConversionUtils.getParquetReadFieldTypes(names, fieldTypes, schema);
    assertEquals(LogicalTypeRoot.INTEGER, physicalTypes[0].getLogicalType().getTypeRoot());
    assertEquals(LogicalTypeRoot.VARBINARY, physicalTypes[1].getLogicalType().getTypeRoot());
    assertEquals(LogicalTypeRoot.VARBINARY, physicalTypes[2].getLogicalType().getTypeRoot());
    assertEquals(LogicalTypeRoot.VARBINARY, physicalTypes[3].getLogicalType().getTypeRoot());
  }

  @Test
  void testEncodeDecodeAndIteratorConversion() throws Exception {
    byte[] floatBytes = VectorConversionUtils.encodeVectorArrayData(
        new GenericArrayData(new float[] {1.25f, -2.5f}), FLOAT_VECTOR);
    byte[] doubleBytes = VectorConversionUtils.encodeVectorArrayData(
        new GenericArrayData(new double[] {3.5d, -4.75d}), DOUBLE_VECTOR);
    byte[] int8Bytes = VectorConversionUtils.encodeVectorArrayData(
        new GenericArrayData(new byte[] {1, -2, 3}), INT8_VECTOR);

    ArrayData floats = VectorConversionUtils.createVectorArrayData(floatBytes, FLOAT_VECTOR);
    assertEquals(1.25f, floats.getFloat(0));
    assertEquals(-2.5f, floats.getFloat(1));
    ArrayData doubles = VectorConversionUtils.createVectorArrayData(doubleBytes, DOUBLE_VECTOR);
    assertEquals(3.5d, doubles.getDouble(0));
    assertEquals(-4.75d, doubles.getDouble(1));
    ArrayData codes = VectorConversionUtils.createVectorArrayData(int8Bytes, INT8_VECTOR);
    assertEquals((byte) -2, codes.getByte(1));
    assertArrayEquals(int8Bytes, VectorConversionUtils.encodeVectorArrayData(codes, INT8_VECTOR));
    assertThrows(IllegalArgumentException.class, () -> VectorConversionUtils.encodeVectorArrayData(
        new GenericArrayData(new float[] {1.0f}), FLOAT_VECTOR));

    GenericRowData physical = GenericRowData.of(
        StringData.fromString("key"), floatBytes, null);
    physical.setRowKind(RowKind.UPDATE_AFTER);
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.STRING()),
        DataTypes.FIELD("vector", DataTypes.BYTES()),
        DataTypes.FIELD("optional", DataTypes.INT())).getLogicalType();
    ClosableIterator<RowData> wrapped = VectorConversionUtils.wrapVectorColumnIterator(
        ClosableIterator.wrap(Collections.<RowData>singletonList(physical).iterator()),
        rowType, Collections.singletonMap(1, FLOAT_VECTOR));
    RowData converted = wrapped.next();
    assertEquals(RowKind.UPDATE_AFTER, converted.getRowKind());
    assertEquals("key", converted.getString(0).toString());
    assertEquals(1.25f, converted.getArray(1).getFloat(0));
    assertTrue(converted.isNullAt(2));
    wrapped.close();
  }

  @Test
  void testVectorLogicalTypeValidation() {
    VectorConversionUtils.validateVectorLogicalType(
        FLOAT_VECTOR, DataTypes.ARRAY(DataTypes.FLOAT()).getLogicalType());
    VectorConversionUtils.validateVectorLogicalType(
        DOUBLE_VECTOR, DataTypes.ARRAY(DataTypes.DOUBLE()).getLogicalType());
    VectorConversionUtils.validateVectorLogicalType(
        INT8_VECTOR, DataTypes.ARRAY(DataTypes.TINYINT()).getLogicalType());
    assertThrows(SchemaCompatibilityException.class,
        () -> VectorConversionUtils.validateVectorLogicalType(
            FLOAT_VECTOR, DataTypes.ARRAY(DataTypes.DOUBLE()).getLogicalType()));
  }

  private static HoodieSchema vectorRecordSchema() {
    return HoodieSchema.createRecord("vectors", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("float_vec", FLOAT_VECTOR),
        HoodieSchemaField.of("double_vec", DOUBLE_VECTOR),
        HoodieSchemaField.of("codes", INT8_VECTOR)));
  }
}
