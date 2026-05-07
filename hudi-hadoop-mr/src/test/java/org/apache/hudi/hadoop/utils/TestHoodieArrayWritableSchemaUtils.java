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
import org.apache.hudi.common.schema.HoodieProjectionMask;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaTestUtils;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.hadoop.HiveHoodieReaderContext;
import org.apache.hudi.hadoop.HiveRecordContext;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
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
import org.apache.hadoop.io.NullWritable;
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

public class TestHoodieArrayWritableSchemaUtils {

  private final HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

  @Test
  public void testProjection() {
    HoodieSchema from = HoodieTestDataGenerator.HOODIE_SCHEMA;
    HoodieSchema to = HoodieSchemaUtils.generateProjectionSchema(from, Arrays.asList("trip_type", "current_ts", "weight"));
    UnaryOperator<ArrayWritable> reverseProjection = HoodieArrayWritableSchemaUtils.getReverseProjection(to, from);

    //We reuse the ArrayWritable, so we need to get the values before projecting
    ArrayWritable record = convertArrayWritable(dataGen.generateGenericRecord());
    HiveAvroSerializer fromSerializer = new HiveAvroSerializer(from);
    Object tripType = fromSerializer.getValue(record, "trip_type");
    Object currentTs = fromSerializer.getValue(record, "current_ts");
    Object weight = fromSerializer.getValue(record, "weight");

    //Make sure the projected fields can be read
    ArrayWritable projectedRecord = HoodieArrayWritableSchemaUtils.rewriteRecordWithNewSchema(record, from, to, Collections.emptyMap());
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
    HoodieSchema oldSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema newSchema = HoodieSchema.create(HoodieSchemaType.DATE);
    Writable oldWritable = new Text("2023-01-01");
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, oldSchema, newSchema);
    Writable expected = HoodieHiveUtils.getDateWriteable(HoodieAvroUtils.fromJavaDate(Date.valueOf("2023-01-01")));
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, oldSchema, result, newSchema);
  }

  @Test
  void testRewriteIntToLong() throws AvroSerdeException {
    Writable oldWritable = new IntWritable(42);
    HoodieSchema oldSchema = HoodieSchema.create(HoodieSchemaType.INT);
    HoodieSchema newSchema = HoodieSchema.create(HoodieSchemaType.LONG);
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, oldSchema, newSchema);
    Writable expected = new LongWritable(42);
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, oldSchema, result, newSchema);
  }

  @Test
  void testRewriteLongToFloat() throws AvroSerdeException {
    Writable oldWritable = new LongWritable(123);
    HoodieSchema oldSchema = HoodieSchema.create(HoodieSchemaType.LONG);
    HoodieSchema newSchema = HoodieSchema.create(HoodieSchemaType.FLOAT);
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, oldSchema, newSchema);
    Writable expected = new FloatWritable(123.0f);
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, oldSchema, result, newSchema);
  }

  @Test
  void testRewriteFloatToDouble() throws AvroSerdeException {
    Writable oldWritable = new FloatWritable(3.14f);
    HoodieSchema oldSchema = HoodieSchema.create(HoodieSchemaType.FLOAT);
    HoodieSchema newSchema = HoodieSchema.create(HoodieSchemaType.DOUBLE);
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, oldSchema, newSchema);
    Writable expected = new DoubleWritable(3.14d);
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, oldSchema, result, newSchema);
  }

  @Test
  void testRewriteBytesToString() throws AvroSerdeException {
    BytesWritable oldWritable = new BytesWritable("hello".getBytes());
    HoodieSchema oldSchema = HoodieSchema.create(HoodieSchemaType.BYTES);
    HoodieSchema newSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, oldSchema, newSchema);
    Writable expected = new Text("hello");
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, oldSchema, result, newSchema);
  }

  @Test
  void testRewriteIntToString() throws AvroSerdeException {
    Writable oldWritable = new IntWritable(123);
    HoodieSchema oldSchema = HoodieSchema.create(HoodieSchemaType.INT);
    HoodieSchema newSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, oldSchema, newSchema);
    Writable expected = new Text("123");
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, oldSchema, result, newSchema);
  }

  @Test
  void testRewriteFixedDecimalToString() throws AvroSerdeException {
    HoodieSchema decimalSchema = HoodieSchema.createDecimal("decimal", null, null, 10, 2, 5);
    HiveDecimalWritable oldWritable = new HiveDecimalWritable(HiveDecimal.create(new BigDecimal("123.45")));
    HoodieSchema newSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, decimalSchema, newSchema);
    Writable expected = new Text("123.45");
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, decimalSchema, result, newSchema);
  }

  @Test
  void testRewriteStringToFixedDecimal() throws AvroSerdeException {
    HoodieSchema decimalSchema = HoodieSchema.createDecimal("decimal", null, null, 10, 2, 5);
    Writable oldWritable = new Text("123.45");
    HoodieSchema oldSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, oldSchema, decimalSchema);
    assertInstanceOf(HiveDecimalWritable.class, result);
    assertEquals(new BigDecimal("123.45"), ((HiveDecimalWritable) result).getHiveDecimal().bigDecimalValue());
    validateRewriteWithAvro(oldWritable, oldSchema, result, decimalSchema);
  }

  @Test
  void testRewriteBytesToFixedDecimal() throws AvroSerdeException {
    BigDecimal input = new BigDecimal("123.45");
    byte[] bytes = input.unscaledValue().toByteArray();
    BytesWritable oldWritable = new BytesWritable(bytes);
    HoodieSchema decimalSchema = HoodieSchema.createDecimal("decimal", null, null, 5, 2, 5);
    HoodieSchema oldSchema = HoodieSchema.create(HoodieSchemaType.BYTES);
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, oldSchema, decimalSchema);
    assertEquals(input, ((HiveDecimalWritable) result).getHiveDecimal().bigDecimalValue());
    validateRewriteWithAvro(oldWritable, oldSchema, result, decimalSchema);
  }

  @Test
  void testUnsupportedTypeConversionThrows() {
    HoodieSchema oldSchema = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.INT));
    HoodieSchema newSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertThrows(HoodieSchemaException.class, () ->
        HoodieArrayWritableSchemaUtils.rewritePrimaryType(null, oldSchema, newSchema));
  }

  @Test
  void testRewriteEnumToString() throws AvroSerdeException {
    HoodieSchema enumSchema = HoodieSchema.createEnum("TestEnum", null, null, Arrays.asList("A", "B", "C"));
    Writable oldWritable = new Text("B");
    HoodieSchema newSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, enumSchema, newSchema);
    Writable expected = new Text("B");
    assertEquals(expected, result);
    validateRewriteWithAvro(oldWritable, enumSchema, result, newSchema);
  }

  @Test
  void testRewriteFixedWithSameSizeAndFullName() {
    HoodieSchema oldFixed = HoodieSchema.createFixed("decimal", null, "ns", 5);
    HoodieSchema newFixed = HoodieSchema.createFixed("decimal", null, "ns", 5);
    HiveDecimalWritable hdw = new HiveDecimalWritable(HiveDecimal.create("123.45"));
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(hdw, oldFixed, newFixed);
    assertSame(hdw, result);
  }

  @Test
  void testRewriteFixedWithSameSizeButDifferentNameUsesDecimalFallback() throws AvroSerdeException {
    HoodieSchema oldFixed = HoodieSchema.createDecimal("decA", "ns1", null, 5, 2, 5);
    HoodieSchema newFixed = HoodieSchema.createDecimal("decB", "ns2", null, 5, 2, 5);
    HiveDecimalWritable oldWritable = new HiveDecimalWritable(HiveDecimal.create("123.45"));
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, oldFixed, newFixed);
    assertInstanceOf(HiveDecimalWritable.class, result);
    assertEquals(new BigDecimal("123.45"), ((HiveDecimalWritable) result).getHiveDecimal().bigDecimalValue());
    validateRewriteWithAvro(oldWritable, oldFixed, result, newFixed);
  }

  @Test
  void testRewriteBooleanPassthrough() {
    HoodieSchema boolSchema = HoodieSchema.create(HoodieSchemaType.BOOLEAN);
    BooleanWritable bool = new BooleanWritable(true);
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(bool, boolSchema, boolSchema);
    assertSame(bool, result);
  }

  @Test
  void testUnsupportedRewriteMapToIntThrows() {
    HoodieSchema oldSchema = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema newSchema = HoodieSchema.create(HoodieSchemaType.INT);
    assertThrows(HoodieSchemaException.class, () ->
        HoodieArrayWritableSchemaUtils.rewritePrimaryType(new Text("foo"), oldSchema, newSchema));
  }

  @Test
  void testRewriteIntToDecimalFixed() throws AvroSerdeException {
    HoodieSchema fixedDecimalSchema = HoodieSchema.createDecimal("dec", null, null, 8, 2, 5);
    HoodieSchema oldSchema = HoodieSchema.create(HoodieSchemaType.INT);
    IntWritable oldWritable = new IntWritable(12345);
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, oldSchema, fixedDecimalSchema);
    assertInstanceOf(HiveDecimalWritable.class, result);
    assertEquals(new BigDecimal("12345"), ((HiveDecimalWritable) result).getHiveDecimal().bigDecimalValue());
    validateRewriteWithAvro(oldWritable, oldSchema, result, fixedDecimalSchema);
  }

  @Test
  void testRewriteDoubleToDecimalFixed() throws AvroSerdeException {
    HoodieSchema fixedDecimal = HoodieSchema.createDecimal("dec", null, null, 10, 3, 8);
    HoodieSchema oldSchema = HoodieSchema.create(HoodieSchemaType.DOUBLE);
    DoubleWritable oldWritable = new DoubleWritable(987.654);
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, oldSchema, fixedDecimal);
    assertInstanceOf(HiveDecimalWritable.class, result);
    assertEquals(new BigDecimal("987.654"), ((HiveDecimalWritable) result).getHiveDecimal().bigDecimalValue());
    validateRewriteWithAvro(oldWritable, oldSchema, result, fixedDecimal);
  }

  @Test
  void testRewriteDecimalBytesToFixed() throws AvroSerdeException {
    HoodieSchema decimalSchema = HoodieSchema.createDecimal("dec", null, null, 6, 2, 6);
    HoodieSchema oldSchema = HoodieSchema.create(HoodieSchemaType.BYTES);
    BigDecimal value = new BigDecimal("999.99");
    byte[] unscaledBytes = value.unscaledValue().toByteArray();
    BytesWritable oldWritable = new BytesWritable(unscaledBytes);
    Writable result = HoodieArrayWritableSchemaUtils.rewritePrimaryType(oldWritable, oldSchema, decimalSchema);
    assertEquals(value, ((HiveDecimalWritable) result).getHiveDecimal().bigDecimalValue());
    validateRewriteWithAvro(oldWritable, oldSchema, result, decimalSchema);
  }

  @Test
  void testRewritePlainFixedToVectorPassesThrough() {
    // Pins the fix for the Hive vector-read path. Parquet stores VECTOR as bare
    // FIXED_LEN_BYTE_ARRAY (AvroSchemaConverterWithTimestampNTZ#convertField),
    // so Hive's Parquet reader reconstructs the Avro schema as plain FIXED using
    // the column name; Hudi then projects to the canonical VECTOR schema
    // (vector_float_3, size 12, logicalType=vector). Sizes match and VECTOR's
    // FIXED_BYTES backing is byte-identical, so the rewrite must pass through.
    // Before the fix this threw "cannot support rewrite value for schema type".
    HoodieSchema oldSchema = HoodieSchema.createFixed("embedding", null, null, 12);
    HoodieSchema newSchema = HoodieSchema.createVector(3, HoodieSchema.Vector.VectorElementType.FLOAT);
    BytesWritable bytes = new BytesWritable(new byte[12]);

    Writable rewritten = HoodieArrayWritableSchemaUtils.rewritePrimaryType(bytes, oldSchema, newSchema);

    assertSame(bytes, rewritten);
  }

  @Test
  void testRewriteBlobWithPrunedArrayWritableFillsMissingFieldsWithNull() {
    HoodieSchema oldSchema = HoodieSchemaTestUtils.createPlainBlobRecord("blob_data");
    HoodieSchema newSchema = HoodieSchema.createBlob();
    ArrayWritable prunedRecord = new ArrayWritable(Writable.class, new Writable[] {
        new Text("INLINE")
    });

    ArrayWritable rewritten = HoodieArrayWritableSchemaUtils.rewriteRecordWithNewSchema(
        prunedRecord, oldSchema, newSchema, Collections.emptyMap());

    assertEquals(3, rewritten.get().length);
    assertInstanceOf(BytesWritable.class, rewritten.get()[0]);
    assertEquals("INLINE", new String(((BytesWritable) rewritten.get()[0]).copyBytes()));
    assertEquals(NullWritable.get(), rewritten.get()[1]);
    assertEquals(NullWritable.get(), rewritten.get()[2]);
  }

  @Test
  void testRewriteBlobWithPrunedReferenceProjection() {
    // SELECT blob_data.reference: compacted-shape input must round-trip unchanged so
    // Hive's projected-schema ObjectInspector finds reference at slot 0.
    HoodieSchema oldSchema = HoodieSchemaTestUtils.createPlainBlobRecord("blob_data");
    HoodieSchema newSchema = HoodieSchema.createBlob();
    ArrayWritable referenceSubstruct = new ArrayWritable(Writable.class, new Writable[] {
        new Text("blobs/updated-1"),
        new LongWritable(0L),
        new LongWritable(11L),
        new BooleanWritable(false)
    });
    ArrayWritable prunedRecord = new ArrayWritable(Writable.class, new Writable[] {
        referenceSubstruct
    });
    HoodieProjectionMask mask = HoodieProjectionMask.builder().field("reference").build();

    ArrayWritable rewritten = HoodieArrayWritableSchemaUtils.rewriteRecordWithNewSchema(
        prunedRecord, oldSchema, newSchema, Collections.emptyMap(), mask);

    assertEquals(1, rewritten.get().length);
    ArrayWritable rewrittenRef = (ArrayWritable) rewritten.get()[0];
    assertEquals(4, rewrittenRef.get().length);
    assertEquals(new Text("blobs/updated-1"), rewrittenRef.get()[0]);
    assertEquals(new LongWritable(0L), rewrittenRef.get()[1]);
    assertEquals(new LongWritable(11L), rewrittenRef.get()[2]);
    assertEquals(new BooleanWritable(false), rewrittenRef.get()[3]);
  }

  @Test
  void testRewriteBlobWithPrunedReferenceExternalPathProjection() {
    // SELECT blob_data.reference.external_path — reproducer for the CCE at :149.
    HoodieSchema oldSchema = HoodieSchemaTestUtils.createPlainBlobRecord("blob_data");
    HoodieSchema newSchema = HoodieSchema.createBlob();
    ArrayWritable prunedReference = new ArrayWritable(Writable.class, new Writable[] {
        new Text("blobs/updated-1")
    });
    ArrayWritable prunedRecord = new ArrayWritable(Writable.class, new Writable[] {
        prunedReference
    });
    HoodieProjectionMask mask = HoodieProjectionMask.builder()
        .field("reference", HoodieProjectionMask.builder().field("external_path").build())
        .build();

    ArrayWritable rewritten = HoodieArrayWritableSchemaUtils.rewriteRecordWithNewSchema(
        prunedRecord, oldSchema, newSchema, Collections.emptyMap(), mask);

    assertEquals(1, rewritten.get().length);
    ArrayWritable rewrittenRef = (ArrayWritable) rewritten.get()[0];
    assertEquals(1, rewrittenRef.get().length);
    assertEquals(new Text("blobs/updated-1"), rewrittenRef.get()[0]);
  }

  @Test
  void testRewriteBlobWithCanonicalShapeStillWorksAfterMaskWiring() {
    // Regression guard: mask=all() must keep the legacy canonical-shape behavior.
    HoodieSchema oldSchema = HoodieSchemaTestUtils.createPlainBlobRecord("blob_data");
    HoodieSchema newSchema = HoodieSchema.createBlob();
    ArrayWritable referenceSubstruct = new ArrayWritable(Writable.class, new Writable[] {
        new Text("blobs/path-1"),
        new LongWritable(0L),
        new LongWritable(11L),
        new BooleanWritable(false)
    });
    ArrayWritable record = new ArrayWritable(Writable.class, new Writable[] {
        new Text("OUT_OF_LINE"),
        NullWritable.get(),
        referenceSubstruct
    });

    ArrayWritable rewritten = HoodieArrayWritableSchemaUtils.rewriteRecordWithNewSchema(
        record, oldSchema, newSchema, Collections.emptyMap(), HoodieProjectionMask.all());

    assertInstanceOf(BytesWritable.class, rewritten.get()[0]);
    assertEquals("OUT_OF_LINE", new String(((BytesWritable) rewritten.get()[0]).copyBytes()));
    assertEquals(NullWritable.get(), rewritten.get()[1]);
    ArrayWritable rewrittenRef = (ArrayWritable) rewritten.get()[2];
    assertEquals(new Text("blobs/path-1"), rewrittenRef.get()[0]);
    assertEquals(new LongWritable(0L), rewrittenRef.get()[1]);
    assertEquals(new LongWritable(11L), rewrittenRef.get()[2]);
    assertEquals(new BooleanWritable(false), rewrittenRef.get()[3]);
  }

  private void validateRewriteWithAvro(
      Writable oldWritable,
      HoodieSchema oldSchema,
      Writable newWritable,
      HoodieSchema newSchema
  ) throws AvroSerdeException {
    TypeInfo oldTypeInfo = HiveTypeUtils.generateTypeInfo(oldSchema, Collections.emptySet());
    TypeInfo newTypeInfo = HiveTypeUtils.generateTypeInfo(newSchema, Collections.emptySet());

    ObjectInspector oldObjectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(oldTypeInfo);
    ObjectInspector newObjectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(newTypeInfo);

    ObjectInspector writableOIOld = getWritableOIForType(oldTypeInfo);
    ObjectInspector writableOINew = getWritableOIForType(newTypeInfo);

    Object javaInput = ObjectInspectorConverters.getConverter(writableOIOld, oldObjectInspector).convert(oldWritable);
    if (oldSchema.getType() == HoodieSchemaType.DECIMAL) {
      javaInput = HoodieAvroUtils.DECIMAL_CONVERSION.toFixed(getDecimalValue(javaInput, (HoodieSchema.Decimal) oldSchema), oldSchema.toAvroSchema(), oldSchema.toAvroSchema().getLogicalType());
    } else if (javaInput instanceof byte[]) {
      javaInput = ByteBuffer.wrap((byte[]) javaInput);
    }
    Object javaOutput = HoodieAvroUtils.rewritePrimaryType(javaInput, oldSchema.toAvroSchema(), newSchema.toAvroSchema());
    Object javaExpected = ObjectInspectorConverters.getConverter(writableOINew, newObjectInspector).convert(newWritable);

    if (newSchema.getType() == HoodieSchemaType.DECIMAL) {
      BigDecimal outputDecimal = getDecimalValue(javaOutput, (HoodieSchema.Decimal) newSchema);
      BigDecimal expectedDecimal = getDecimalValue(javaExpected, (HoodieSchema.Decimal) newSchema);
      assertEquals(0, outputDecimal.compareTo(expectedDecimal));
    } else if (newSchema.getType() == HoodieSchemaType.DATE) {
      assertEquals(HoodieAvroUtils.toJavaDate((int) javaOutput), javaExpected);
    } else {
      assertEquals(javaOutput, javaExpected);
    }
  }

  private BigDecimal getDecimalValue(Object value, HoodieSchema.Decimal decimalSchema) {
    if (value instanceof HiveDecimal) {
      return ((HiveDecimal) value).bigDecimalValue();
    } else if (value instanceof HiveDecimalWritable) {
      return ((HiveDecimalWritable) value).getHiveDecimal().bigDecimalValue();
    } else if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    } else if (value instanceof byte[]) {
      return new BigDecimal(new BigInteger((byte[]) value), decimalSchema.getScale());
    } else if (value instanceof GenericData.Fixed) {
      byte[] bytes = ((GenericData.Fixed) value).bytes();
      return new BigDecimal(new BigInteger(bytes), decimalSchema.getScale());
    }
    throw new IllegalArgumentException("Unsupported decimal object: " + value.getClass() + " -> " + value);
  }

  @Test
  void testRewriteBlobToBlobProjectionEquivalentShortCircuits() {
    HoodieSchema blob = HoodieSchema.createBlob();
    ArrayWritable reference = new ArrayWritable(Writable.class, new Writable[]{
        new Text("blobs/path-1"),
        new LongWritable(0L),
        new LongWritable(11L),
        new BooleanWritable(false)
    });
    ArrayWritable record = new ArrayWritable(Writable.class, new Writable[]{
        new Text("OUT_OF_LINE"),
        NullWritable.get(),
        reference
    });
    ArrayWritable result = HoodieArrayWritableSchemaUtils.rewriteRecordWithNewSchema(
        record, blob, blob, Collections.emptyMap());
    // Same HoodieSchema on both sides -> areSchemasProjectionEquivalent short-circuits.
    assertSame(record, result);
  }

  @Test
  void testRewritePlainBlobRecordToCanonicalBlobSchema() {
    HoodieSchema oldSchema = HoodieSchemaTestUtils.createPlainBlobRecord("blob_data");
    HoodieSchema newSchema = HoodieSchema.createBlob();
    ArrayWritable reference = new ArrayWritable(Writable.class, new Writable[]{
        new Text("blobs/path-1"),
        new LongWritable(0L),
        new LongWritable(11L),
        new BooleanWritable(false)
    });
    ArrayWritable record = new ArrayWritable(Writable.class, new Writable[]{
        new Text("OUT_OF_LINE"),
        NullWritable.get(),
        reference
    });

    ArrayWritable rewritten = HoodieArrayWritableSchemaUtils.rewriteRecordWithNewSchema(
        record, oldSchema, newSchema, Collections.emptyMap());

    // Type field must have been converted from STRING (Text) to ENUM (BytesWritable).
    assertInstanceOf(BytesWritable.class, rewritten.get()[0]);
    assertEquals("OUT_OF_LINE", new String(((BytesWritable) rewritten.get()[0]).copyBytes()));
    // Data stays null, reference record passes through unchanged field-wise.
    assertEquals(NullWritable.get(), rewritten.get()[1]);
    ArrayWritable rewrittenRef = (ArrayWritable) rewritten.get()[2];
    assertEquals(new Text("blobs/path-1"), rewrittenRef.get()[0]);
    assertEquals(new LongWritable(0L), rewrittenRef.get()[1]);
    assertEquals(new LongWritable(11L), rewrittenRef.get()[2]);
    assertEquals(new BooleanWritable(false), rewrittenRef.get()[3]);
  }

  @Test
  void testRewriteVariantToVariantProjectionEquivalentShortCircuits() {
    HoodieSchema variant = HoodieSchema.createVariant();
    ArrayWritable record = new ArrayWritable(Writable.class, new Writable[]{
        new BytesWritable(new byte[]{1, 2, 3}),
        new BytesWritable(new byte[]{4, 5, 6})
    });
    ArrayWritable result = HoodieArrayWritableSchemaUtils.rewriteRecordWithNewSchema(
        record, variant, variant, Collections.emptyMap());
    assertSame(record, result);
  }

  @Test
  void testRewritePlainVariantRecordToCanonicalVariantSchema() {
    HoodieSchema oldSchema = HoodieSchemaTestUtils.createPlainVariantRecord("variant_data");
    HoodieSchema newSchema = HoodieSchema.createVariant();
    BytesWritable metadata = new BytesWritable(new byte[] {1, 2, 3});
    BytesWritable value = new BytesWritable(new byte[] {4, 5, 6});
    ArrayWritable record = new ArrayWritable(Writable.class, new Writable[] {metadata, value});

    ArrayWritable rewritten = HoodieArrayWritableSchemaUtils.rewriteRecordWithNewSchema(
        record, oldSchema, newSchema, Collections.emptyMap());

    assertEquals(metadata, rewritten.get()[0]);
    assertEquals(value, rewritten.get()[1]);
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
