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

import org.apache.hudi.utilities.test.proto.Nested;
import org.apache.hudi.utilities.test.proto.Sample;
import org.apache.hudi.utilities.test.proto.SampleEnum;

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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import com.google.protobuf.util.Timestamps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TestProtoConversionUtil {
  @Test
  public void allFieldsSet_wellKnownTypesAreNested() {
    List<Integer> primitiveList = Arrays.asList(1, 2, 3);
    Map<String, Integer> primitiveMap = new HashMap<>();
    primitiveMap.put("key1", 1);
    primitiveMap.put("key2", 2);

    Nested nestedMessage = Nested.newBuilder().setNestedInt(1).build();
    List<Nested> nestedList = Arrays.asList(Nested.newBuilder().setNestedInt(2).build(), Nested.newBuilder().setNestedInt(3).build());
    Map<String, Nested> nestedMap = new HashMap<>();
    nestedMap.put("1Key", Nested.newBuilder().setNestedInt(123).build());
    nestedMap.put("2Key", Nested.newBuilder().setNestedInt(321).build());
    Timestamp time = Timestamps.fromMillis(System.currentTimeMillis());

    Sample sample = Sample.newBuilder()
        .setPrimitiveDouble(1.1)
        .setPrimitiveFloat(2.1f)
        .setPrimitiveInt(1)
        .setPrimitiveLong(2L)
        .setPrimitiveUnsignedInt(3)
        .setPrimitiveUnsignedLong(4L)
        .setPrimitiveSignedInt(5)
        .setPrimitiveSignedLong(6L)
        .setPrimitiveFixedInt(7)
        .setPrimitiveFixedLong(8L)
        .setPrimitiveFixedSignedInt(9)
        .setPrimitiveFixedSignedLong(10L)
        .setPrimitiveBoolean(true)
        .setPrimitiveString("I am a string!")
        .setPrimitiveBytes(ByteString.copyFrom("I am just bytes".getBytes()))
        .addAllRepeatedPrimitive(primitiveList)
        .putAllMapPrimitive(primitiveMap)
        .setNestedMessage(nestedMessage)
        .addAllRepeatedMessage(nestedList)
        .putAllMapMessage(nestedMap)
        .setWrappedString(StringValue.of("I am a wrapped string"))
        .setWrappedInt(Int32Value.of(11))
        .setWrappedLong(Int64Value.of(12L))
        .setWrappedUnsignedInt(UInt32Value.of(13))
        .setWrappedUnsignedLong(UInt64Value.of(14L))
        .setWrappedDouble(DoubleValue.of(15.5))
        .setWrappedFloat(FloatValue.of(16.6f))
        .setWrappedBoolean(BoolValue.of(true))
        .setWrappedBytes(BytesValue.of(ByteString.copyFrom("I am wrapped bytes".getBytes())))
        .setEnum(SampleEnum.SECOND)
        .setTimestamp(time)
        .build();
    Schema.Parser parser = new Schema.Parser();
    Schema protoSchema = parser.parse(getSchema("schema-provider/proto/sample_schema_nested.txt"));

    GenericRecord actual = ProtoConversionUtil.convertToAvro(protoSchema, sample);

    Schema nestedMessageSchema = protoSchema.getField("nested_message").schema().getTypes().get(1);

    GenericData.Record wrappedStringRecord = getWrappedRecord(protoSchema, "wrapped_string", "I am a wrapped string");
    GenericData.Record wrappedIntRecord = getWrappedRecord(protoSchema, "wrapped_int", 11);
    GenericData.Record wrappedLongRecord = getWrappedRecord(protoSchema, "wrapped_long", 12L);
    GenericData.Record wrappedUIntRecord = getWrappedRecord(protoSchema, "wrapped_unsigned_int", 13L);
    GenericData.Record wrappedULongRecord = getWrappedRecord(protoSchema, "wrapped_unsigned_long", 14L);
    GenericData.Record wrappedDoubleRecord = getWrappedRecord(protoSchema, "wrapped_double", 15.5);
    GenericData.Record wrappedFloatRecord = getWrappedRecord(protoSchema, "wrapped_float", 16.6f);
    GenericData.Record wrappedBooleanRecord = getWrappedRecord(protoSchema, "wrapped_boolean", true);
    GenericData.Record wrappedBytesRecord = getWrappedRecord(protoSchema, "wrapped_bytes", ByteBuffer.wrap("I am wrapped bytes".getBytes()));

    GenericData.Record expectedRecord = new GenericData.Record(protoSchema);
    expectedRecord.put("primitive_double", 1.1);
    expectedRecord.put("primitive_float", 2.1f);
    expectedRecord.put("primitive_int", 1);
    expectedRecord.put("primitive_long", 2L);
    expectedRecord.put("primitive_unsigned_int", 3L);
    expectedRecord.put("primitive_unsigned_long", 4L);
    expectedRecord.put("primitive_signed_int", 5);
    expectedRecord.put("primitive_signed_long", 6L);
    expectedRecord.put("primitive_fixed_int", 7);
    expectedRecord.put("primitive_fixed_long", 8L);
    expectedRecord.put("primitive_fixed_signed_int", 9);
    expectedRecord.put("primitive_fixed_signed_long", 10L);
    expectedRecord.put("primitive_boolean", true);
    expectedRecord.put("primitive_string", "I am a string!");
    expectedRecord.put("primitive_bytes", ByteBuffer.wrap("I am just bytes".getBytes()));
    expectedRecord.put("repeated_primitive", primitiveList);
    expectedRecord.put("map_primitive", convertMapToList(protoSchema, "map_primitive", primitiveMap));
    expectedRecord.put("nested_message", convertNestedMessage(nestedMessageSchema, nestedMessage));
    expectedRecord.put("repeated_message", nestedList.stream().map(m -> convertNestedMessage(nestedMessageSchema, m)).collect(Collectors.toList()));
    expectedRecord.put("map_message", convertMapToList(protoSchema, "map_message", nestedMap, value -> convertNestedMessage(nestedMessageSchema, value)));
    expectedRecord.put("wrapped_string", wrappedStringRecord);
    expectedRecord.put("wrapped_int", wrappedIntRecord);
    expectedRecord.put("wrapped_long", wrappedLongRecord);
    expectedRecord.put("wrapped_unsigned_int", wrappedUIntRecord);
    expectedRecord.put("wrapped_unsigned_long", wrappedULongRecord);
    expectedRecord.put("wrapped_double", wrappedDoubleRecord);
    expectedRecord.put("wrapped_float", wrappedFloatRecord);
    expectedRecord.put("wrapped_boolean", wrappedBooleanRecord);
    expectedRecord.put("wrapped_bytes", wrappedBytesRecord);
    expectedRecord.put("enum", SampleEnum.SECOND.name());
    expectedRecord.put("timestamp", getTimestampRecord(protoSchema, time));
    Assertions.assertEquals(expectedRecord, actual);
  }

  @Test
  public void noFieldsSet_wellKnownTypesAreNested() {
    Sample sample = Sample.newBuilder().build();
    Schema.Parser parser = new Schema.Parser();
    Schema protoSchema = parser.parse(getSchema("schema-provider/proto/sample_schema_nested.txt"));

    GenericRecord actual = ProtoConversionUtil.convertToAvro(protoSchema, sample);

    // all fields will have default values
    GenericData.Record expectedRecord = new GenericData.Record(protoSchema);
    expectedRecord.put("primitive_double", 0.0);
    expectedRecord.put("primitive_float", 0.0f);
    expectedRecord.put("primitive_int", 0);
    expectedRecord.put("primitive_long", 0L);
    expectedRecord.put("primitive_unsigned_int", 0L);
    expectedRecord.put("primitive_unsigned_long", 0L);
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
    Assertions.assertEquals(expectedRecord, actual);
  }

  @Test
  public void allFieldsSet_wellKnownTypesAreFlattened() {
    List<Integer> primitiveList = Arrays.asList(1, 2, 3);
    Map<String, Integer> primitiveMap = new HashMap<>();
    primitiveMap.put("key1", 1);
    primitiveMap.put("key2", 2);

    Nested nestedMessage = Nested.newBuilder().setNestedInt(1).build();
    List<Nested> nestedList = Arrays.asList(Nested.newBuilder().setNestedInt(2).build(), Nested.newBuilder().setNestedInt(3).build());
    Map<String, Nested> nestedMap = new HashMap<>();
    nestedMap.put("1Key", Nested.newBuilder().setNestedInt(123).build());
    nestedMap.put("2Key", Nested.newBuilder().setNestedInt(321).build());
    Timestamp time = Timestamps.fromMillis(System.currentTimeMillis());

    Sample sample = Sample.newBuilder()
        .setPrimitiveDouble(1.1)
        .setPrimitiveFloat(2.1f)
        .setPrimitiveInt(1)
        .setPrimitiveLong(2L)
        .setPrimitiveUnsignedInt(3)
        .setPrimitiveUnsignedLong(4L)
        .setPrimitiveSignedInt(5)
        .setPrimitiveSignedLong(6L)
        .setPrimitiveFixedInt(7)
        .setPrimitiveFixedLong(8L)
        .setPrimitiveFixedSignedInt(9)
        .setPrimitiveFixedSignedLong(10L)
        .setPrimitiveBoolean(true)
        .setPrimitiveString("I am a string!")
        .setPrimitiveBytes(ByteString.copyFrom("I am just bytes".getBytes()))
        .addAllRepeatedPrimitive(primitiveList)
        .putAllMapPrimitive(primitiveMap)
        .setNestedMessage(nestedMessage)
        .addAllRepeatedMessage(nestedList)
        .putAllMapMessage(nestedMap)
        .setWrappedString(StringValue.of("I am a wrapped string"))
        .setWrappedInt(Int32Value.of(11))
        .setWrappedLong(Int64Value.of(12L))
        .setWrappedUnsignedInt(UInt32Value.of(13))
        .setWrappedUnsignedLong(UInt64Value.of(14L))
        .setWrappedDouble(DoubleValue.of(15.5))
        .setWrappedFloat(FloatValue.of(16.6f))
        .setWrappedBoolean(BoolValue.of(true))
        .setWrappedBytes(BytesValue.of(ByteString.copyFrom("I am wrapped bytes".getBytes())))
        .setEnum(SampleEnum.SECOND)
        .setTimestamp(time)
        .build();
    Schema.Parser parser = new Schema.Parser();
    Schema protoSchema = parser.parse(getSchema("schema-provider/proto/sample_schema_flattened.txt"));

    GenericRecord actual = ProtoConversionUtil.convertToAvro(protoSchema, sample);

    Schema nestedMessageSchema = protoSchema.getField("nested_message").schema().getTypes().get(1);

    GenericData.Record expectedRecord = new GenericData.Record(protoSchema);
    expectedRecord.put("primitive_double", 1.1);
    expectedRecord.put("primitive_float", 2.1f);
    expectedRecord.put("primitive_int", 1);
    expectedRecord.put("primitive_long", 2L);
    expectedRecord.put("primitive_unsigned_int", 3L);
    expectedRecord.put("primitive_unsigned_long", 4L);
    expectedRecord.put("primitive_signed_int", 5);
    expectedRecord.put("primitive_signed_long", 6L);
    expectedRecord.put("primitive_fixed_int", 7);
    expectedRecord.put("primitive_fixed_long", 8L);
    expectedRecord.put("primitive_fixed_signed_int", 9);
    expectedRecord.put("primitive_fixed_signed_long", 10L);
    expectedRecord.put("primitive_boolean", true);
    expectedRecord.put("primitive_string", "I am a string!");
    expectedRecord.put("primitive_bytes", ByteBuffer.wrap("I am just bytes".getBytes()));
    expectedRecord.put("repeated_primitive", primitiveList);
    expectedRecord.put("map_primitive", convertMapToList(protoSchema, "map_primitive", primitiveMap));
    expectedRecord.put("nested_message", convertNestedMessage(nestedMessageSchema, nestedMessage));
    expectedRecord.put("repeated_message", nestedList.stream().map(m -> convertNestedMessage(nestedMessageSchema, m)).collect(Collectors.toList()));
    expectedRecord.put("map_message", convertMapToList(protoSchema, "map_message", nestedMap, value -> convertNestedMessage(nestedMessageSchema, value)));
    expectedRecord.put("wrapped_string", "I am a wrapped string");
    expectedRecord.put("wrapped_int", 11);
    expectedRecord.put("wrapped_long", 12L);
    expectedRecord.put("wrapped_unsigned_int", 13L);
    expectedRecord.put("wrapped_unsigned_long", 14L);
    expectedRecord.put("wrapped_double", 15.5);
    expectedRecord.put("wrapped_float", 16.6f);
    expectedRecord.put("wrapped_boolean", true);
    expectedRecord.put("wrapped_bytes", ByteBuffer.wrap("I am wrapped bytes".getBytes()));
    expectedRecord.put("enum", SampleEnum.SECOND.name());
    expectedRecord.put("timestamp", getTimestampRecord(protoSchema, time));
    Assertions.assertEquals(expectedRecord, actual);
  }

  @Test
  public void noFieldsSet_wellKnownTypesAreFlattened() {
    Sample sample = Sample.newBuilder().build();
    Schema.Parser parser = new Schema.Parser();
    Schema protoSchema = parser.parse(getSchema("schema-provider/proto/sample_schema_flattened.txt"));

    GenericRecord actual = ProtoConversionUtil.convertToAvro(protoSchema, sample);

    // all fields will have default values
    GenericData.Record expectedRecord = new GenericData.Record(protoSchema);
    expectedRecord.put("primitive_double", 0.0);
    expectedRecord.put("primitive_float", 0.0f);
    expectedRecord.put("primitive_int", 0);
    expectedRecord.put("primitive_long", 0L);
    expectedRecord.put("primitive_unsigned_int", 0L);
    expectedRecord.put("primitive_unsigned_long", 0L);
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
    Assertions.assertEquals(expectedRecord, actual);
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

  private String getSchema(String pathToSchema) {
    try (Scanner scanner = new Scanner(getClass().getClassLoader().getResourceAsStream(pathToSchema))) {
      return scanner.next();
    }
  }
}
