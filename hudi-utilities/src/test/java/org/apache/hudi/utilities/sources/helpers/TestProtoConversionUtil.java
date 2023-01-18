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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.test.proto.Child;
import org.apache.hudi.utilities.test.proto.Nested;
import org.apache.hudi.utilities.test.proto.Parent;
import org.apache.hudi.utilities.test.proto.Sample;
import org.apache.hudi.utilities.test.proto.SampleEnum;
import org.apache.hudi.utilities.test.proto.WithOneOf;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import com.google.protobuf.util.Timestamps;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.utilities.sources.helpers.ProtoConversionUtil.toUnsignedBigInteger;

public class TestProtoConversionUtil {
  private static final Random RANDOM = new Random();
  private static final String MAX_UNSIGNED_LONG = "18446744073709551615";
  private static final String PRIMITIVE_UNSIGNED_LONG_FIELD_NAME = "primitive_unsigned_long";
  private static final String WRAPPED_UNSIGNED_LONG_FIELD_NAME = "wrapped_unsigned_long";
  private static final Conversions.DecimalConversion DECIMAL_CONVERSION = new Conversions.DecimalConversion();

  @Test
  public void allFieldsSet_wellKnownTypesAndTimestampsAsRecords() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema convertedSchema = parser.parse(getClass().getClassLoader().getResourceAsStream("schema-provider/proto/sample_schema_wrapped_and_timestamp_as_record.avsc"));
    Pair<Sample, GenericRecord> inputAndOutput = createInputOutputSampleWithRandomValues(convertedSchema, true);
    Sample input = inputAndOutput.getLeft();
    GenericRecord actual = serializeAndDeserializeAvro(ProtoConversionUtil.convertToAvro(convertedSchema, input), convertedSchema);
    Assertions.assertEquals(inputAndOutput.getRight(), actual);
    // assert that unsigned long is interpreted correctly
    Schema primitiveUnsignedLongSchema = convertedSchema.getField(PRIMITIVE_UNSIGNED_LONG_FIELD_NAME).schema();
    assertUnsignedLongCorrectness(primitiveUnsignedLongSchema, input.getPrimitiveUnsignedLong(), (GenericFixed) actual.get(PRIMITIVE_UNSIGNED_LONG_FIELD_NAME));
    Schema wrappedUnsignedLongSchema = convertedSchema.getField(WRAPPED_UNSIGNED_LONG_FIELD_NAME).schema().getTypes().get(1).getField("value").schema();
    assertUnsignedLongCorrectness(wrappedUnsignedLongSchema, input.getWrappedUnsignedLong().getValue(), (GenericFixed) ((GenericRecord) actual.get(WRAPPED_UNSIGNED_LONG_FIELD_NAME)).get("value"));
  }

  @Test
  public void noFieldsSet_wellKnownTypesAndTimestampsAsRecords() throws IOException {
    Sample sample = Sample.newBuilder().build();
    Schema.Parser parser = new Schema.Parser();
    Schema convertedSchema = parser.parse(getClass().getClassLoader().getResourceAsStream("schema-provider/proto/sample_schema_wrapped_and_timestamp_as_record.avsc"));
    GenericRecord actual = serializeAndDeserializeAvro(ProtoConversionUtil.convertToAvro(convertedSchema, sample), convertedSchema);
    Assertions.assertEquals(createDefaultOutput(convertedSchema), actual);
  }

  @Test
  public void allFieldsSet_defaultOptions() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema convertedSchema = parser.parse(getClass().getClassLoader().getResourceAsStream("schema-provider/proto/sample_schema_defaults.avsc"));
    Pair<Sample, GenericRecord> inputAndOutput = createInputOutputSampleWithRandomValues(convertedSchema, false);
    Sample input = inputAndOutput.getLeft();
    GenericRecord actual = serializeAndDeserializeAvro(ProtoConversionUtil.convertToAvro(convertedSchema, input), convertedSchema);
    Assertions.assertEquals(inputAndOutput.getRight(), actual);
    // assert that unsigned long is interpreted correctly
    Schema primitiveUnsignedLongSchema = convertedSchema.getField(PRIMITIVE_UNSIGNED_LONG_FIELD_NAME).schema();
    assertUnsignedLongCorrectness(primitiveUnsignedLongSchema, input.getPrimitiveUnsignedLong(), (GenericFixed) actual.get(PRIMITIVE_UNSIGNED_LONG_FIELD_NAME));
    Schema wrappedUnsignedLongSchema = convertedSchema.getField(WRAPPED_UNSIGNED_LONG_FIELD_NAME).schema().getTypes().get(1);
    assertUnsignedLongCorrectness(wrappedUnsignedLongSchema, input.getWrappedUnsignedLong().getValue(), (GenericFixed) actual.get(WRAPPED_UNSIGNED_LONG_FIELD_NAME));
  }

  @Test
  public void noFieldsSet_defaultOptions() throws IOException {
    Sample sample = Sample.newBuilder().build();
    Schema.Parser parser = new Schema.Parser();
    Schema convertedSchema = parser.parse(getClass().getClassLoader().getResourceAsStream("schema-provider/proto/sample_schema_defaults.avsc"));
    GenericRecord actual = serializeAndDeserializeAvro(ProtoConversionUtil.convertToAvro(convertedSchema, sample), convertedSchema);
    Assertions.assertEquals(createDefaultOutput(convertedSchema), actual);
  }

  @Test
  public void recursiveSchema_noOverflow() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema convertedSchema = parser.parse(getClass().getClassLoader().getResourceAsStream("schema-provider/proto/parent_schema_recursive_depth_2.avsc"));
    Pair<Parent, GenericRecord> inputAndOutput = createInputOutputForRecursiveSchemaNoOverflow(convertedSchema);
    GenericRecord actual = serializeAndDeserializeAvro(ProtoConversionUtil.convertToAvro(convertedSchema, inputAndOutput.getLeft()), convertedSchema);
    Assertions.assertEquals(inputAndOutput.getRight(), actual);
  }

  @Test
  public void recursiveSchema_withOverflow() throws Exception {
    Schema.Parser parser = new Schema.Parser();
    Schema convertedSchema = parser.parse(getClass().getClassLoader().getResourceAsStream("schema-provider/proto/parent_schema_recursive_depth_2.avsc"));
    Pair<Parent, GenericRecord> inputAndOutput = createInputOutputForRecursiveSchemaWithOverflow(convertedSchema);
    Parent input = inputAndOutput.getLeft();
    GenericRecord actual = serializeAndDeserializeAvro(ProtoConversionUtil.convertToAvro(convertedSchema, inputAndOutput.getLeft()), convertedSchema);
    Assertions.assertEquals(inputAndOutput.getRight(), actual);
    // assert that overflow data can be read back into proto class
    Child parsedSingleChildOverflow = Child.parseFrom(getOverflowBytesFromChildRecord((GenericRecord) actual.get("child")));
    Assertions.assertEquals(input.getChild().getRecurseField().getRecurseField(), parsedSingleChildOverflow);
    // Get children list
    GenericData.Array<GenericRecord> array = (GenericData.Array<GenericRecord>) actual.get("children");
    Child parsedChildren1Overflow = Child.parseFrom(getOverflowBytesFromChildRecord(array.get(0)));
    Assertions.assertEquals(input.getChildren(0).getRecurseField().getRecurseField(), parsedChildren1Overflow);
    Child parsedChildren2Overflow = Child.parseFrom(getOverflowBytesFromChildRecord(array.get(1)));
    Assertions.assertEquals(input.getChildren(1).getRecurseField().getRecurseField(), parsedChildren2Overflow);
  }

  @Test
  public void oneOfSchema() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema convertedSchema = parser.parse(getClass().getClassLoader().getResourceAsStream("schema-provider/proto/oneof_schema.avsc"));
    WithOneOf input = WithOneOf.newBuilder().setLong(32L).build();
    GenericRecord actual = serializeAndDeserializeAvro(ProtoConversionUtil.convertToAvro(convertedSchema, input), convertedSchema);

    GenericData.Record expectedRecord = new GenericData.Record(convertedSchema);
    expectedRecord.put("int", null);
    expectedRecord.put("long", 32L);
    expectedRecord.put("message", null);
    Assertions.assertEquals(expectedRecord, actual);
  }

  @Test
  public void longToUnsignedBigIntegerConversion() {
    Assertions.assertEquals("0", toUnsignedBigInteger(0).toString());
    Assertions.assertEquals(MAX_UNSIGNED_LONG, toUnsignedBigInteger(-1).toString());
    Assertions.assertEquals(String.valueOf(Long.MAX_VALUE), toUnsignedBigInteger(Long.MAX_VALUE).toString());
    Assertions.assertEquals("10", toUnsignedBigInteger(10L).toString());
    // equivalent of lower 32 bits all set to 0 and upper 32 bits alternating 10
    Assertions.assertEquals("12297829379609722880", toUnsignedBigInteger(-6148914694099828736L).toString());
  }

  private void assertUnsignedLongCorrectness(Schema fieldSchema, long expectedValue, GenericFixed actual) {
    BigDecimal actualPrimitiveUnsignedLong = DECIMAL_CONVERSION.fromFixed(actual, fieldSchema,
        fieldSchema.getLogicalType());
    Assertions.assertEquals(Long.toUnsignedString(expectedValue), actualPrimitiveUnsignedLong.toString());
  }

  private Pair<Sample, GenericRecord> createInputOutputSampleWithRandomValues(Schema schema, boolean wellKnownTypesAsRecords) {
    Schema nestedMessageSchema = schema.getField("nested_message").schema().getTypes().get(1);
    Schema listMessageSchema = schema.getField("repeated_message").schema().getElementType();
    Schema mapMessageSchema = schema.getField("map_message").schema().getElementType().getField("value").schema().getTypes().get(1);
    Schema unsignedLongSchema = schema.getField("primitive_unsigned_long").schema();

    // ensure at least one uint64 related value is above the signed long range
    boolean primitiveUnsignedLongInUnsignedRange = RANDOM.nextBoolean();

    double primitiveDouble = RANDOM.nextDouble();
    float primitiveFloat = RANDOM.nextFloat();
    int primitiveInt = RANDOM.nextInt();
    long primitiveLong = RANDOM.nextLong();
    int primitiveUnsignedInt = RANDOM.nextInt();
    long primitiveUnsignedLong = RANDOM.nextLong();
    int primitiveSignedInt = RANDOM.nextInt();
    long primitiveSignedLong = RANDOM.nextLong();
    int primitiveFixedInt = RANDOM.nextInt();
    long primitiveFixedLong = primitiveUnsignedLongInUnsignedRange ? Long.parseUnsignedLong(MAX_UNSIGNED_LONG) - RANDOM.nextInt(1000) : RANDOM.nextLong();
    int primitiveFixedSignedInt = RANDOM.nextInt();
    long primitiveFixedSignedLong = RANDOM.nextLong();
    boolean primitiveBoolean = RANDOM.nextBoolean();
    String primitiveString = randomString(10);
    byte[] primitiveBytes = randomString(10).getBytes();

    double wrappedDouble = RANDOM.nextDouble();
    float wrappedFloat = RANDOM.nextFloat();
    int wrappedInt = RANDOM.nextInt();
    long wrappedLong = RANDOM.nextLong();
    int wrappedUnsignedInt = RANDOM.nextInt();
    long wrappedUnsignedLong = primitiveUnsignedLongInUnsignedRange ? RANDOM.nextLong() : Long.parseUnsignedLong(MAX_UNSIGNED_LONG) - RANDOM.nextInt(1000);
    boolean wrappedBoolean = RANDOM.nextBoolean();
    String wrappedString = randomString(10);
    byte[] wrappedBytes = randomString(10).getBytes();
    SampleEnum enumValue = SampleEnum.forNumber(RANDOM.nextInt(1));

    List<Integer> primitiveList = Arrays.asList(RANDOM.nextInt(), RANDOM.nextInt(), RANDOM.nextInt());
    Map<String, Integer> primitiveMap = new HashMap<>();
    primitiveMap.put(randomString(5), RANDOM.nextInt());
    primitiveMap.put(randomString(5), RANDOM.nextInt());

    Nested nestedMessage = Nested.newBuilder().setNestedInt(RANDOM.nextInt()).build();
    List<Nested> nestedList = Arrays.asList(Nested.newBuilder().setNestedInt(RANDOM.nextInt()).build(), Nested.newBuilder().setNestedInt(RANDOM.nextInt()).build());
    Map<String, Nested> nestedMap = new HashMap<>();
    nestedMap.put(randomString(5), Nested.newBuilder().setNestedInt(RANDOM.nextInt()).build());
    nestedMap.put(randomString(5), Nested.newBuilder().setNestedInt(RANDOM.nextInt()).build());
    Timestamp time = Timestamps.fromMillis(System.currentTimeMillis());

    Sample input = Sample.newBuilder()
        .setPrimitiveDouble(primitiveDouble)
        .setPrimitiveFloat(primitiveFloat)
        .setPrimitiveInt(primitiveInt)
        .setPrimitiveLong(primitiveLong)
        .setPrimitiveUnsignedInt(primitiveUnsignedInt)
        .setPrimitiveUnsignedLong(primitiveUnsignedLong)
        .setPrimitiveSignedInt(primitiveSignedInt)
        .setPrimitiveSignedLong(primitiveSignedLong)
        .setPrimitiveFixedInt(primitiveFixedInt)
        .setPrimitiveFixedLong(primitiveFixedLong)
        .setPrimitiveFixedSignedInt(primitiveFixedSignedInt)
        .setPrimitiveFixedSignedLong(primitiveFixedSignedLong)
        .setPrimitiveBoolean(primitiveBoolean)
        .setPrimitiveString(primitiveString)
        .setPrimitiveBytes(ByteString.copyFrom(primitiveBytes))
        .addAllRepeatedPrimitive(primitiveList)
        .putAllMapPrimitive(primitiveMap)
        .setNestedMessage(nestedMessage)
        .addAllRepeatedMessage(nestedList)
        .putAllMapMessage(nestedMap)
        .setWrappedString(StringValue.of(wrappedString))
        .setWrappedInt(Int32Value.of(wrappedInt))
        .setWrappedLong(Int64Value.of(wrappedLong))
        .setWrappedUnsignedInt(UInt32Value.of(wrappedUnsignedInt))
        .setWrappedUnsignedLong(UInt64Value.of(wrappedUnsignedLong))
        .setWrappedDouble(DoubleValue.of(wrappedDouble))
        .setWrappedFloat(FloatValue.of(wrappedFloat))
        .setWrappedBoolean(BoolValue.of(wrappedBoolean))
        .setWrappedBytes(BytesValue.of(ByteString.copyFrom(wrappedBytes)))
        .setEnum(enumValue)
        .setTimestamp(time)
        .build();

    Object wrappedStringOutput;
    Object wrappedIntOutput;
    Object wrappedLongOutput;
    Object wrappedUIntOutput;
    Object wrappedULongOutput;
    Object wrappedDoubleOutput;
    Object wrappedFloatOutput;
    Object wrappedBooleanOutput;
    Object wrappedBytesOutput;
    Object timestampOutput;
    if (wellKnownTypesAsRecords) {
      wrappedStringOutput = getWrappedRecord(schema, "wrapped_string", wrappedString);
      wrappedIntOutput = getWrappedRecord(schema, "wrapped_int", wrappedInt);
      wrappedLongOutput = getWrappedRecord(schema, "wrapped_long", wrappedLong);
      wrappedUIntOutput = getWrappedRecord(schema, "wrapped_unsigned_int", (long) wrappedUnsignedInt);
      wrappedULongOutput = getWrappedRecord(schema, "wrapped_unsigned_long", unsignedLongAsGenericFixed(wrappedUnsignedLong, unsignedLongSchema));
      wrappedDoubleOutput = getWrappedRecord(schema, "wrapped_double", wrappedDouble);
      wrappedFloatOutput = getWrappedRecord(schema, "wrapped_float", wrappedFloat);
      wrappedBooleanOutput = getWrappedRecord(schema, "wrapped_boolean", wrappedBoolean);
      wrappedBytesOutput = getWrappedRecord(schema, "wrapped_bytes", ByteBuffer.wrap(wrappedBytes));
      timestampOutput = getTimestampRecord(schema, time);
    } else {
      wrappedStringOutput = wrappedString;
      wrappedIntOutput = wrappedInt;
      wrappedLongOutput = wrappedLong;
      wrappedUIntOutput = (long) wrappedUnsignedInt;
      wrappedULongOutput = unsignedLongAsGenericFixed(wrappedUnsignedLong, unsignedLongSchema);
      wrappedDoubleOutput = wrappedDouble;
      wrappedFloatOutput = wrappedFloat;
      wrappedBooleanOutput = wrappedBoolean;
      wrappedBytesOutput = ByteBuffer.wrap(wrappedBytes);
      timestampOutput = Timestamps.toMicros(time);
    }

    GenericData.Record expectedRecord = new GenericData.Record(schema);
    expectedRecord.put("primitive_double", primitiveDouble);
    expectedRecord.put("primitive_float", primitiveFloat);
    expectedRecord.put("primitive_int", primitiveInt);
    expectedRecord.put("primitive_long", primitiveLong);
    expectedRecord.put("primitive_unsigned_int", (long) primitiveUnsignedInt);
    expectedRecord.put("primitive_unsigned_long", unsignedLongAsGenericFixed(primitiveUnsignedLong, unsignedLongSchema));
    expectedRecord.put("primitive_signed_int", primitiveSignedInt);
    expectedRecord.put("primitive_signed_long", primitiveSignedLong);
    expectedRecord.put("primitive_fixed_int", primitiveFixedInt);
    expectedRecord.put("primitive_fixed_long", primitiveFixedLong);
    expectedRecord.put("primitive_fixed_signed_int", primitiveFixedSignedInt);
    expectedRecord.put("primitive_fixed_signed_long", primitiveFixedSignedLong);
    expectedRecord.put("primitive_boolean", primitiveBoolean);
    expectedRecord.put("primitive_string", primitiveString);
    expectedRecord.put("primitive_bytes", ByteBuffer.wrap(primitiveBytes));
    expectedRecord.put("repeated_primitive", primitiveList);
    expectedRecord.put("map_primitive", convertMapToList(schema, "map_primitive", primitiveMap));
    expectedRecord.put("nested_message", convertNestedMessage(nestedMessageSchema, nestedMessage));
    expectedRecord.put("repeated_message", nestedList.stream().map(m -> convertNestedMessage(listMessageSchema, m)).collect(Collectors.toList()));
    expectedRecord.put("map_message", convertMapToList(schema, "map_message", nestedMap, value -> convertNestedMessage(mapMessageSchema, value)));
    expectedRecord.put("wrapped_string", wrappedStringOutput);
    expectedRecord.put("wrapped_int", wrappedIntOutput);
    expectedRecord.put("wrapped_long", wrappedLongOutput);
    expectedRecord.put("wrapped_unsigned_int", wrappedUIntOutput);
    expectedRecord.put("wrapped_unsigned_long", wrappedULongOutput);
    expectedRecord.put("wrapped_double", wrappedDoubleOutput);
    expectedRecord.put("wrapped_float", wrappedFloatOutput);
    expectedRecord.put("wrapped_boolean", wrappedBooleanOutput);
    expectedRecord.put("wrapped_bytes", wrappedBytesOutput);
    expectedRecord.put("enum", enumValue.name());
    expectedRecord.put("timestamp", timestampOutput);

    return Pair.of(input, expectedRecord);
  }

  private GenericFixed unsignedLongAsGenericFixed(long unsignedLong, Schema unsignedLongSchema) {
    BigDecimal bigDecimal = new BigDecimal(toUnsignedBigInteger(unsignedLong));
    return DECIMAL_CONVERSION.toFixed(bigDecimal,
        unsignedLongSchema, unsignedLongSchema.getLogicalType());
  }

  private GenericRecord createDefaultOutput(Schema schema) {
    // all fields will have default values
    Schema unsignedLongSchema = schema.getField("primitive_unsigned_long").schema();
    GenericData.Record expectedRecord = new GenericData.Record(schema);
    expectedRecord.put("primitive_double", 0.0);
    expectedRecord.put("primitive_float", 0.0f);
    expectedRecord.put("primitive_int", 0);
    expectedRecord.put("primitive_long", 0L);
    expectedRecord.put("primitive_unsigned_int", 0L);
    expectedRecord.put("primitive_unsigned_long", unsignedLongAsGenericFixed(0L, unsignedLongSchema));
    expectedRecord.put("primitive_signed_int", 0);
    expectedRecord.put("primitive_signed_long", 0L);
    expectedRecord.put("primitive_fixed_int", 0);
    expectedRecord.put("primitive_fixed_long", 0L);
    expectedRecord.put("primitive_fixed_signed_int", 0);
    expectedRecord.put("primitive_fixed_signed_long", 0L);
    expectedRecord.put("primitive_boolean", false);
    expectedRecord.put("primitive_string", "");
    expectedRecord.put("primitive_bytes", ByteBuffer.wrap("".getBytes()));
    expectedRecord.put("repeated_primitive", Collections.emptyList());
    expectedRecord.put("map_primitive", Collections.emptyList());
    expectedRecord.put("nested_message", null);
    expectedRecord.put("repeated_message", Collections.emptyList());
    expectedRecord.put("map_message", Collections.emptyList());
    expectedRecord.put("wrapped_string", null);
    expectedRecord.put("wrapped_int", null);
    expectedRecord.put("wrapped_long", null);
    expectedRecord.put("wrapped_unsigned_int", null);
    expectedRecord.put("wrapped_unsigned_long", null);
    expectedRecord.put("wrapped_double", null);
    expectedRecord.put("wrapped_float", null);
    expectedRecord.put("wrapped_boolean", null);
    expectedRecord.put("wrapped_bytes", null);
    expectedRecord.put("enum", SampleEnum.FIRST.name());
    expectedRecord.put("timestamp", null);
    return expectedRecord;
  }

  public Pair<Parent, GenericRecord> createInputOutputForRecursiveSchemaNoOverflow(Schema schema) {
    Child singleChild = Child.newBuilder()
        .setBasicField(1)
        .setRecurseField(Child.newBuilder()
            .setBasicField(2)
            .build())
        .build();
    Child children1 = Child.newBuilder()
        .setBasicField(11)
        .setRecurseField(Child.newBuilder()
            .setBasicField(12)
            .build())
        .build();
    Child children2 = Child.newBuilder()
        .setBasicField(21)
        .setRecurseField(Child.newBuilder()
            .setBasicField(22)
            .build())
        .build();
    List<Child> childrenList = Arrays.asList(children1, children2);
    Parent input = Parent.newBuilder().setChild(singleChild).addAllChildren(childrenList).build();

    Schema childAvroSchema = schema.getField("child").schema().getTypes().get(1);
    Schema childLevel2AvroSchema = childAvroSchema.getField("recurse_field").schema().getTypes().get(1);

    Schema childrenAvroSchema = schema.getField("children").schema().getElementType();
    Schema childrenLevel2AvroSchema = childrenAvroSchema.getField("recurse_field").schema().getTypes().get(1);

    // setup the single child avro
    GenericData.Record singleChildLevel2Avro = new GenericData.Record(childLevel2AvroSchema);
    singleChildLevel2Avro.put("basic_field", 2);
    GenericData.Record singleChildAvro = new GenericData.Record(childAvroSchema);
    singleChildAvro.put("basic_field", 1);
    singleChildAvro.put("recurse_field", singleChildLevel2Avro);

    // setup list of children
    GenericData.Record children1Level2Avro = new GenericData.Record(childrenLevel2AvroSchema);
    children1Level2Avro.put("basic_field", 12);
    GenericData.Record children1Avro = new GenericData.Record(childrenAvroSchema);
    children1Avro.put("basic_field", 11);
    children1Avro.put("recurse_field", children1Level2Avro);

    GenericData.Record children2Level2Avro = new GenericData.Record(childrenLevel2AvroSchema);
    children2Level2Avro.put("basic_field", 22);
    GenericData.Record children2Avro = new GenericData.Record(childrenAvroSchema);
    children2Avro.put("basic_field", 21);
    children2Avro.put("recurse_field", children2Level2Avro);

    // setup expected parent record
    GenericData.Record expected = new GenericData.Record(schema);
    expected.put("child", singleChildAvro);
    expected.put("children", Arrays.asList(children1Avro, children2Avro));

    return Pair.of(input, expected);
  }

  public Pair<Parent, GenericRecord> createInputOutputForRecursiveSchemaWithOverflow(Schema schema) {
    Child singleChildOverflow = Child.newBuilder()
        .setBasicField(3)
        .setRecurseField(Child.newBuilder()
            .setBasicField(4)
            .build()).build();
    Child singleChild = Child.newBuilder()
        .setBasicField(1)
        .setRecurseField(Child.newBuilder()
            .setBasicField(2)
            .setRecurseField(singleChildOverflow)
            .build())
        .build();
    Child children1Overflow = Child.newBuilder()
        .setBasicField(13)
        .setRecurseField(Child.newBuilder()
            .setBasicField(14)
            .build()).build();
    Child children1 = Child.newBuilder()
        .setBasicField(11)
        .setRecurseField(Child.newBuilder()
            .setBasicField(12)
            .setRecurseField(children1Overflow)
            .build())
        .build();
    Child children2Overflow = Child.newBuilder()
        .setBasicField(23)
        .setRecurseField(Child.newBuilder()
            .setBasicField(24)
            .build()).build();
    Child children2 = Child.newBuilder()
        .setBasicField(21)
        .setRecurseField(Child.newBuilder()
            .setBasicField(22)
            .setRecurseField(children2Overflow)
            .build())
        .build();
    List<Child> childrenList = Arrays.asList(children1, children2);
    Parent input = Parent.newBuilder().setChild(singleChild).addAllChildren(childrenList).build();

    Schema childAvroSchema = schema.getField("child").schema().getTypes().get(1);
    Schema childLevel2AvroSchema = childAvroSchema.getField("recurse_field").schema().getTypes().get(1);
    Schema recursionOverflowSchema = childLevel2AvroSchema.getField("recurse_field").schema().getTypes().get(1);

    Schema childrenAvroSchema = schema.getField("children").schema().getElementType();
    Schema childrenLevel2AvroSchema = childrenAvroSchema.getField("recurse_field").schema().getTypes().get(1);

    // setup the single child avro
    GenericData.Record singleChildOverflowAvro = new GenericData.Record(recursionOverflowSchema);
    singleChildOverflowAvro.put("descriptor_full_name", "test.Child");
    singleChildOverflowAvro.put("proto_bytes", ByteBuffer.wrap(singleChildOverflow.toByteArray()));
    GenericData.Record singleChildLevel2Avro = new GenericData.Record(childLevel2AvroSchema);
    singleChildLevel2Avro.put("basic_field", 2);
    singleChildLevel2Avro.put("recurse_field", singleChildOverflowAvro);
    GenericData.Record singleChildAvro = new GenericData.Record(childAvroSchema);
    singleChildAvro.put("basic_field", 1);
    singleChildAvro.put("recurse_field", singleChildLevel2Avro);

    // setup list of children
    GenericData.Record children1OverflowAvro = new GenericData.Record(recursionOverflowSchema);
    children1OverflowAvro.put("descriptor_full_name", "test.Child");
    children1OverflowAvro.put("proto_bytes", ByteBuffer.wrap(children1Overflow.toByteArray()));
    GenericData.Record children1Level2Avro = new GenericData.Record(childrenLevel2AvroSchema);
    children1Level2Avro.put("basic_field", 12);
    children1Level2Avro.put("recurse_field", children1OverflowAvro);
    GenericData.Record children1Avro = new GenericData.Record(childrenAvroSchema);
    children1Avro.put("basic_field", 11);
    children1Avro.put("recurse_field", children1Level2Avro);

    GenericData.Record children2OverflowAvro = new GenericData.Record(recursionOverflowSchema);
    children2OverflowAvro.put("descriptor_full_name", "test.Child");
    children2OverflowAvro.put("proto_bytes", ByteBuffer.wrap(children2Overflow.toByteArray()));
    GenericData.Record children2Level2Avro = new GenericData.Record(childrenLevel2AvroSchema);
    children2Level2Avro.put("basic_field", 22);
    children2Level2Avro.put("recurse_field", children2OverflowAvro);
    GenericData.Record children2Avro = new GenericData.Record(childrenAvroSchema);
    children2Avro.put("basic_field", 21);
    children2Avro.put("recurse_field", children2Level2Avro);

    // setup expected parent record
    GenericData.Record expected = new GenericData.Record(schema);
    expected.put("child", singleChildAvro);
    expected.put("children", Arrays.asList(children1Avro, children2Avro));

    return Pair.of(input, expected);
  }

  private ByteBuffer getOverflowBytesFromChildRecord(GenericRecord record) {
    return (ByteBuffer) ((GenericRecord) ((GenericRecord) record.get("recurse_field")).get("recurse_field")).get("proto_bytes");
  }

  private GenericRecord serializeAndDeserializeAvro(GenericRecord input, Schema schema) {
    // serialize and deserialize the data to make sure the avro record can be persisted and then read back
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
      writer.write(input, encoder);
      encoder.flush();

      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
      GenericRecord transformedRec = reader.read(null, decoder);
      return transformedRec;
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  private GenericData.Record getTimestampRecord(Schema protoSchema, Timestamp time) {
    GenericData.Record timestampRecord = new GenericData.Record(protoSchema.getField("timestamp").schema().getTypes().get(1));
    timestampRecord.put("seconds", time.getSeconds());
    timestampRecord.put("nanos", time.getNanos());
    return timestampRecord;
  }

  private GenericData.Record getWrappedRecord(Schema protoSchema, String fieldName, Object value) {
    GenericData.Record wrappedRecord = new GenericData.Record(protoSchema.getField(fieldName).schema().getTypes().get(1));
    wrappedRecord.put("value", value);
    return wrappedRecord;
  }

  private GenericRecord convertNestedMessage(final Schema schema, Nested message) {
    GenericData.Record record = new GenericData.Record(schema);
    record.put("nested_int", message.getNestedInt());
    return record;
  }

  private static <K, V> List<GenericRecord> convertMapToList(final Schema protoSchema, final String fieldName, final Map<K, V> originalMap, final Function<V, ?> valueConverter) {
    return originalMap.entrySet().stream().map(entry -> {
      GenericData.Record record = new GenericData.Record(protoSchema.getField(fieldName).schema().getElementType());
      record.put("key", entry.getKey());
      record.put("value", valueConverter.apply(entry.getValue()));
      return record;
    }).collect(Collectors.toList());
  }

  private static <K, V> List<GenericRecord> convertMapToList(final Schema protoSchema, final String fieldName, final Map<K, V> originalMap) {
    return convertMapToList(protoSchema, fieldName, originalMap, Function.identity());
  }

  private static String randomString(int size) {
    byte[] bytes = new byte[size];
    RANDOM.nextBytes(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
