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

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.hadoop.HiveHoodieReaderContext;
import org.apache.hudi.hadoop.HiveRecordContext;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.HiveTypeUtils;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieArrayWritableAvroUtils {

  HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
  Schema tableSchema = HoodieTestDataGenerator.AVRO_SCHEMA;

  @Test
  public void testProjection() {
    Schema from =  tableSchema;
    Schema to = HoodieAvroUtils.generateProjectionSchema(from, Arrays.asList("trip_type", "current_ts", "weight"));
    UnaryOperator<ArrayWritable> reverseProjection = HoodieArrayWritableAvroUtils.getReverseProjection(to, from);

    //We reuse the ArrayWritable, so we need to get the values before projecting
    ArrayWritable record = convertArrayWritable(dataGen.generateGenericRecord());
    HiveAvroSerializer fromSerializer = new HiveAvroSerializer(from);
    Object tripType = fromSerializer.getValue(record, "trip_type");
    Object currentTs = fromSerializer.getValue(record, "current_ts");
    Object weight = fromSerializer.getValue(record, "weight");

    //Make sure the projected fields can be read
    ArrayWritable projectedRecord = HoodieArrayWritableAvroUtils.rewriteRecordWithNewSchema(record, from, to, Collections.emptyMap());
    HiveAvroSerializer toSerializer = new HiveAvroSerializer(to);
    assertEquals(tripType, toSerializer.getValue(projectedRecord, "trip_type"));
    assertEquals(currentTs, toSerializer.getValue(projectedRecord, "current_ts"));
    assertEquals(weight, toSerializer.getValue(projectedRecord, "weight"));

    //Reverse projection, the fields are in the original spots, but only the fields we set can be read.
    //Therefore, we can only check the 3 fields that were in the projection
    ArrayWritable reverseProjected = reverseProjection.apply(projectedRecord);
    assertEquals(tripType, fromSerializer.getValue(reverseProjected, "trip_type"));
    assertEquals(currentTs, fromSerializer.getValue(reverseProjected, "current_ts"));
    assertEquals(weight, fromSerializer.getValue(reverseProjected, "weight"));
  }

  private static ArrayWritable convertArrayWritable(GenericRecord record) {
    return  (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(record, record.getSchema(), false);
  }

  @Test
  public void testCastOrderingField() {
    HiveHoodieReaderContext readerContext = mock(HiveHoodieReaderContext.class, Mockito.CALLS_REAL_METHODS);
    RecordContext recordContext = mock(HiveRecordContext.class, Mockito.CALLS_REAL_METHODS);
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    assertEquals(new Text("ASDF"), readerContext.getRecordContext().convertValueToEngineType(new Text("ASDF")));
    assertEquals(new Text("ASDF"), readerContext.getRecordContext().convertValueToEngineType("ASDF"));
    assertEquals(new IntWritable(8), readerContext.getRecordContext().convertValueToEngineType(new IntWritable(8)));
    assertEquals(new IntWritable(8), readerContext.getRecordContext().convertValueToEngineType(8));
    assertEquals(new LongWritable(Long.MAX_VALUE / 2L), readerContext.getRecordContext().convertValueToEngineType(new LongWritable(Long.MAX_VALUE / 2L)));
    assertEquals(new LongWritable(Long.MAX_VALUE / 2L), readerContext.getRecordContext().convertValueToEngineType(Long.MAX_VALUE / 2L));
    assertEquals(new FloatWritable(20.24f), readerContext.getRecordContext().convertValueToEngineType(new FloatWritable(20.24f)));
    assertEquals(new FloatWritable(20.24f), readerContext.getRecordContext().convertValueToEngineType(20.24f));
    assertEquals(new DoubleWritable(21.12d), readerContext.getRecordContext().convertValueToEngineType(new DoubleWritable(21.12d)));
    assertEquals(new DoubleWritable(21.12d), readerContext.getRecordContext().convertValueToEngineType(21.12d));

    // make sure that if input is a writeable, then it still works
    WritableComparable reflexive = new IntWritable(8675309);
    assertEquals(reflexive, readerContext.getRecordContext().convertValueToEngineType(reflexive));
  }

  @Test
  void testRewriteStringToDateInt() throws AvroSerdeException {
    Schema oldSchema = Schema.create(Schema.Type.STRING);
    Schema newSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Writable oldWritable = new Text("2023-01-01");
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, oldSchema, newSchema);
    Writable expected = HoodieHiveUtils.getDateWriteable(HoodieAvroUtils.fromJavaDate(Date.valueOf("2023-01-01")));
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, oldSchema, result, newSchema);
  }

  @Test
  void testRewriteIntToLong() throws AvroSerdeException {
    Writable oldWritable = new IntWritable(42);
    Schema oldSchema = Schema.create(Schema.Type.INT);
    Schema newSchema = Schema.create(Schema.Type.LONG);
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, oldSchema, newSchema);
    Writable expected = new LongWritable(42);
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, oldSchema, result, newSchema);
  }

  @Test
  void testRewriteLongToFloat() throws AvroSerdeException {
    Writable oldWritable = new LongWritable(123);
    Schema oldSchema = Schema.create(Schema.Type.LONG);
    Schema newSchema = Schema.create(Schema.Type.FLOAT);
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, oldSchema, newSchema);
    Writable expected = new FloatWritable(123.0f);
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, oldSchema, result, newSchema);
  }

  @Test
  void testRewriteFloatToDouble() throws AvroSerdeException {
    Writable oldWritable = new FloatWritable(3.14f);
    Schema oldSchema = Schema.create(Schema.Type.FLOAT);
    Schema newSchema = Schema.create(Schema.Type.DOUBLE);
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, oldSchema, newSchema);
    Writable expected = new DoubleWritable(3.14d);
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, oldSchema, result, newSchema);
  }

  @Test
  void testRewriteBytesToString() throws AvroSerdeException {
    BytesWritable oldWritable = new BytesWritable("hello".getBytes());
    Schema oldSchema = Schema.create(Schema.Type.BYTES);
    Schema newSchema = Schema.create(Schema.Type.STRING);
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, oldSchema, newSchema);
    Writable expected = new Text("hello");
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, oldSchema, result, newSchema);
  }

  @Test
  void testRewriteIntToString() throws AvroSerdeException {
    Writable oldWritable = new IntWritable(123);
    Schema oldSchema = Schema.create(Schema.Type.INT);
    Schema newSchema = Schema.create(Schema.Type.STRING);
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, oldSchema, newSchema);
    Writable expected = new Text("123");
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, oldSchema, result, newSchema);
  }

  @Test
  void testRewriteFixedDecimalToString() throws AvroSerdeException {
    Schema decimalSchema = LogicalTypes.decimal(10, 2).addToSchema(Schema.createFixed("decimal", null, null, 5));
    HiveDecimalWritable oldWritable = new HiveDecimalWritable(HiveDecimal.create(new BigDecimal("123.45")));
    Schema newSchema = Schema.create(Schema.Type.STRING);
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, decimalSchema, newSchema);
    Writable expected = new Text("123.45");
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, decimalSchema, result, newSchema);
  }

  @Test
  void testRewriteStringToFixedDecimal() throws AvroSerdeException {
    Schema decimalSchema = LogicalTypes.decimal(10, 2).addToSchema(Schema.createFixed("decimal", null, null, 5));
    Writable oldWritable = new Text("123.45");
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, Schema.create(Schema.Type.STRING), decimalSchema);
    assertInstanceOf(HiveDecimalWritable.class, result);
    assertEquals(new BigDecimal("123.45"), ((HiveDecimalWritable) result).getHiveDecimal().bigDecimalValue());
    validateRewriteWithAvro(oldWritable, Schema.create(Schema.Type.STRING), result, decimalSchema);
  }

  @Test
  void testRewriteBytesToFixedDecimal() throws AvroSerdeException {
    BigDecimal input = new BigDecimal("123.45");
    byte[] bytes = input.unscaledValue().toByteArray();
    BytesWritable oldWritable = new BytesWritable(bytes);
    Schema decimalSchema = LogicalTypes.decimal(5, 2).addToSchema(Schema.createFixed("decimal", null, null, 5));
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, Schema.create(Schema.Type.BYTES), decimalSchema);
    assertEquals(input, ((HiveDecimalWritable) result).getHiveDecimal().bigDecimalValue());
    validateRewriteWithAvro(oldWritable, Schema.create(Schema.Type.BYTES), result, decimalSchema);
  }

  @Test
  void testUnsupportedTypeConversionThrows() {
    Schema oldSchema = Schema.createMap(Schema.create(Schema.Type.INT));
    Schema newSchema = Schema.create(Schema.Type.STRING);
    assertThrows(HoodieAvroSchemaException.class, () ->
        HoodieArrayWritableAvroUtils.rewritePrimaryType(null, oldSchema, newSchema));
  }

  @Test
  void testRewriteEnumToString() throws AvroSerdeException {
    Schema enumSchema = Schema.createEnum("TestEnum", null, null, Arrays.asList("A", "B", "C"));
    Writable oldWritable = new Text("B");
    Schema newSchema = Schema.create(Schema.Type.STRING);
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, enumSchema, newSchema);
    Writable expected = new Text("B");
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, enumSchema, result, newSchema);
  }

  @Test
  void testRewriteFixedWithSameSizeAndFullName() {
    Schema oldFixed = Schema.createFixed("decimal", null, "ns", 5);
    Schema newFixed = Schema.createFixed("decimal", null, "ns", 5);
    HiveDecimalWritable hdw = new HiveDecimalWritable(HiveDecimal.create("123.45"));
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(hdw, oldFixed, newFixed);
    assertSame(hdw, result);
  }

  @Test
  void testRewriteFixedWithSameSizeButDifferentNameUsesDecimalFallback() throws AvroSerdeException {
    Schema oldFixed = LogicalTypes.decimal(5, 2).addToSchema(Schema.createFixed("decA", null, "ns1", 5));
    Schema newFixed = LogicalTypes.decimal(5, 2).addToSchema(Schema.createFixed("decB", null, "ns2", 5));
    HiveDecimalWritable oldWritable = new HiveDecimalWritable(HiveDecimal.create("123.45"));
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, oldFixed, newFixed);
    assertInstanceOf(HiveDecimalWritable.class, result);
    assertEquals(new BigDecimal("123.45"), ((HiveDecimalWritable) result).getHiveDecimal().bigDecimalValue());
    validateRewriteWithAvro(oldWritable, oldFixed, result, newFixed);
  }

  @Test
  void testRewriteBooleanPassthrough() {
    Schema boolSchema = Schema.create(Schema.Type.BOOLEAN);
    BooleanWritable bool = new BooleanWritable(true);
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(bool, boolSchema, boolSchema);
    assertSame(bool, result);
  }

  @Test
  void testUnsupportedRewriteMapToIntThrows() {
    Schema oldSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
    Schema newSchema = Schema.create(Schema.Type.INT);
    assertThrows(HoodieAvroSchemaException.class, () ->
        HoodieArrayWritableAvroUtils.rewritePrimaryType(new Text("foo"), oldSchema, newSchema));
  }

  @Test
  void testRewriteIntToDecimalFixed() throws AvroSerdeException {
    Schema fixedDecimal = LogicalTypes.decimal(8, 2).addToSchema(Schema.createFixed("dec", null, null, 6));
    IntWritable oldWritable = new IntWritable(12345);
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, Schema.create(Schema.Type.INT), fixedDecimal);
    assertInstanceOf(HiveDecimalWritable.class, result);
    assertEquals(new BigDecimal("12345"), ((HiveDecimalWritable) result).getHiveDecimal().bigDecimalValue());
    validateRewriteWithAvro(oldWritable, Schema.create(Schema.Type.INT), result, fixedDecimal);
  }

  @Test
  void testRewriteDoubleToDecimalFixed() throws AvroSerdeException {
    Schema fixedDecimal = LogicalTypes.decimal(10, 3).addToSchema(Schema.createFixed("dec", null, null, 8));
    DoubleWritable oldWritable = new DoubleWritable(987.654);
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, Schema.create(Schema.Type.DOUBLE), fixedDecimal);
    assertInstanceOf(HiveDecimalWritable.class, result);
    assertEquals(new BigDecimal("987.654"), ((HiveDecimalWritable) result).getHiveDecimal().bigDecimalValue());
    validateRewriteWithAvro(oldWritable, Schema.create(Schema.Type.DOUBLE), result, fixedDecimal);
  }

  @Test
  void testRewriteDecimalBytesToFixed() throws AvroSerdeException {
    Schema decimalSchema = LogicalTypes.decimal(6, 2).addToSchema(Schema.createFixed("dec", null, null, 6));
    BigDecimal value = new BigDecimal("999.99");
    byte[] unscaledBytes = value.unscaledValue().toByteArray();
    BytesWritable oldWritable = new BytesWritable(unscaledBytes);
    Writable result = HoodieArrayWritableAvroUtils.rewritePrimaryType(oldWritable, Schema.create(Schema.Type.BYTES), decimalSchema);
    assertEquals(value, ((HiveDecimalWritable) result).getHiveDecimal().bigDecimalValue());
    validateRewriteWithAvro(oldWritable, Schema.create(Schema.Type.BYTES), result, decimalSchema);
  }

  private void validateRewriteWithAvro(
      Writable oldWritable,
      Schema oldSchema,
      Writable newWritable,
      Schema newSchema
  ) throws AvroSerdeException {
    TypeInfo oldTypeInfo = HiveTypeUtils.generateTypeInfo(oldSchema, Collections.emptySet());
    TypeInfo newTypeInfo = HiveTypeUtils.generateTypeInfo(newSchema, Collections.emptySet());

    ObjectInspector oldObjectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(oldTypeInfo);
    ObjectInspector newObjectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(newTypeInfo);

    ObjectInspector writableOIOld = getWritableOIForType(oldTypeInfo);
    ObjectInspector writableOINew = getWritableOIForType(newTypeInfo);

    Object javaInput = ObjectInspectorConverters.getConverter(writableOIOld, oldObjectInspector).convert(oldWritable);
    if (isDecimalSchema(oldSchema)) {
      javaInput = HoodieAvroUtils.DECIMAL_CONVERSION.toFixed(getDecimalValue(javaInput, oldSchema), oldSchema, oldSchema.getLogicalType());
    } else if (javaInput instanceof byte[]) {
      javaInput = ByteBuffer.wrap((byte[]) javaInput);
    }
    Object javaOutput = HoodieAvroUtils.rewritePrimaryType(javaInput, oldSchema, newSchema, false);
    Object javaExpected = ObjectInspectorConverters.getConverter(writableOINew, newObjectInspector).convert(newWritable);

    if (isDecimalSchema(newSchema)) {
      BigDecimal outputDecimal = getDecimalValue(javaOutput, newSchema);
      BigDecimal expectedDecimal = getDecimalValue(javaExpected, newSchema);
      assertEquals(0, outputDecimal.compareTo(expectedDecimal));
    } else if (newSchema.getLogicalType() instanceof LogicalTypes.Date) {
      assertEquals(HoodieAvroUtils.toJavaDate((int) javaOutput), javaExpected);
    } else {
      assertEquals(javaOutput, javaExpected);
    }
  }

  private boolean isDecimalSchema(Schema schema) {
    return schema.getLogicalType() instanceof LogicalTypes.Decimal;
  }

  private BigDecimal getDecimalValue(Object value, Schema decimalSchema) {
    if (value instanceof HiveDecimal) {
      return ((HiveDecimal) value).bigDecimalValue();
    } else if (value instanceof HiveDecimalWritable) {
      return ((HiveDecimalWritable) value).getHiveDecimal().bigDecimalValue();
    } else if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    } else if (value instanceof byte[]) {
      int scale = ((LogicalTypes.Decimal) decimalSchema.getLogicalType()).getScale();
      return new BigDecimal(new BigInteger((byte[]) value), scale);
    } else if (value instanceof GenericData.Fixed) {
      int scale = ((LogicalTypes.Decimal) decimalSchema.getLogicalType()).getScale();
      byte[] bytes = ((GenericData.Fixed) value).bytes();
      return new BigDecimal(new BigInteger(bytes), scale);
    }
    throw new IllegalArgumentException("Unsupported decimal object: " + value.getClass() + " -> " + value);
  }

  private ObjectInspector getWritableOIForType(TypeInfo typeInfo) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
        switch (pti.getPrimitiveCategory()) {
          case BOOLEAN:
            return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
          case BYTE:
            return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
          case SHORT:
            return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
          case INT:
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
          case LONG:
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
          case FLOAT:
            return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
          case DOUBLE:
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
          case STRING:
          case CHAR:
          case VARCHAR:
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
          case BINARY:
            return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
          case DECIMAL:
            return PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector;
          case DATE:
            return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
          default:
            throw new UnsupportedOperationException("Unsupported primitive type: " + pti.getPrimitiveCategory());
        }
      default:
        throw new UnsupportedOperationException("Unsupported category: " + typeInfo.getCategory());
    }
  }
}
