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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextTypeHandler;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestPartialUpdateStrategy {
  private static final String TRACK_EVENT_TIME_WATERMARK = "hoodie.write.track.event.time.watermark";
  private String schemaString = "{"
      + "\"type\": \"record\","
      + "\"name\": \"EventRecord\","
      + "\"namespace\": \"com.example.avro\","
      + "\"fields\": ["
      + "{\"name\": \"id\", \"type\": \"string\"},"
      + "{\"name\": \"ts\", \"type\": \"long\"},"
      + "{\"name\": \"op\", \"type\": \"string\"},"
      + "{\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\"}"
      + "]"
      + "}";
  private Schema schema = new Schema.Parser().parse(schemaString);

  @Test
  void testParseValidProperties() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.PARTIAL_UPDATE_PROPERTIES.key(), "a=1,b=2,c=3");
    Map<String, String> result = PartialUpdateStrategy.parsePartialUpdateProperties(props);

    assertEquals(3, result.size());
    assertEquals("1", result.get("a"));
    assertEquals("2", result.get("b"));
    assertEquals("3", result.get("c"));
  }

  @Test
  void testHandlesWhitespace() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.PARTIAL_UPDATE_PROPERTIES.key(), " a = 1 , b=  2 ,c=3 ");
    Map<String, String> result = PartialUpdateStrategy.parsePartialUpdateProperties(props);

    assertEquals(3, result.size());
    assertEquals("1", result.get("a"));
    assertEquals("2", result.get("b"));
    assertEquals("3", result.get("c"));
  }

  @Test
  void testIgnoresEmptyEntriesAndMissingEquals() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.PARTIAL_UPDATE_PROPERTIES.key(), ",a=1,,b,c=3");
    Map<String, String> result = PartialUpdateStrategy.parsePartialUpdateProperties(props);

    assertEquals(2, result.size());
    assertEquals("1", result.get("a"));
    assertEquals("3", result.get("c"));
    assertFalse(result.containsKey("b"));
  }

  @Test
  void testEmptyValueIsAccepted() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.PARTIAL_UPDATE_PROPERTIES.key(), "a=,b=2");
    assertThrows(
        IllegalArgumentException.class,
        () -> PartialUpdateStrategy.parsePartialUpdateProperties(props));
  }

  @Test
  void testEmptyInputReturnsEmptyMap() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.PARTIAL_UPDATE_PROPERTIES.key(), "");
    Map<String, String> result = PartialUpdateStrategy.parsePartialUpdateProperties(props);
    assertTrue(result.isEmpty());
  }

  @Test
  void testMissingKeyReturnsEmptyMap() {
    TypedProperties props = new TypedProperties(); // no property set
    Map<String, String> result = PartialUpdateStrategy.parsePartialUpdateProperties(props);
    assertTrue(result.isEmpty());
  }

  @Test
  void testDirectMatch() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    assertTrue(PartialUpdateStrategy.hasTargetType(stringSchema, Schema.Type.STRING));
  }

  @Test
  void testUnionWithTargetType() {
    Schema unionSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.STRING)
    );
    assertTrue(PartialUpdateStrategy.hasTargetType(unionSchema, Schema.Type.STRING));
  }

  @Test
  void testUnionWithoutTargetType() {
    Schema unionSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.INT)
    );
    assertFalse(PartialUpdateStrategy.hasTargetType(unionSchema, Schema.Type.STRING));
  }

  @Test
  void testNonUnionNonTargetType() {
    Schema intSchema = Schema.create(Schema.Type.INT);
    assertFalse(PartialUpdateStrategy.hasTargetType(intSchema, Schema.Type.STRING));
  }

  @Test
  void testShouldKeepEventTimeMetadataWithoutPropertySet() {
    TypedProperties props = new TypedProperties();
    assertFalse(PartialUpdateStrategy.shouldKeepEventTimeMetadata(props));
  }

  @Test
  void testShouldKeepEventTimeMetadataWithPropertySetFalse() {
    TypedProperties props = new TypedProperties();
    props.setProperty(TRACK_EVENT_TIME_WATERMARK, "false");
    assertFalse(PartialUpdateStrategy.shouldKeepEventTimeMetadata(props));
  }

  @Test
  void testShouldKeepEventTimeMetadataWithPropertySetTrue() {
    TypedProperties props = new TypedProperties();
    props.setProperty(TRACK_EVENT_TIME_WATERMARK, "true");
    assertTrue(PartialUpdateStrategy.shouldKeepEventTimeMetadata(props));
  }

  @Test
  void testPropertySetToTrue() {
    TypedProperties props = new TypedProperties();
    props.setProperty(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), "true");

    boolean result = PartialUpdateStrategy.shouldKeepConsistentLogicalTimestamp(props);
    assertTrue(result);
  }

  @Test
  void testPropertySetToFalse() {
    TypedProperties props = new TypedProperties();
    props.setProperty(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), "false");

    boolean result = PartialUpdateStrategy.shouldKeepConsistentLogicalTimestamp(props);
    assertFalse(result);
  }

  @Test
  void testPropertyMissingUsesDefault() {
    TypedProperties props = new TypedProperties();

    boolean result = PartialUpdateStrategy.shouldKeepConsistentLogicalTimestamp(props);
    boolean expected = Boolean.parseBoolean(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue());

    assertEquals(expected, result);
  }

  @Test
  void testPropertySetToInvalidBooleanFallsBackToFalse() {
    TypedProperties props = new TypedProperties();
    props.setProperty(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), "notABoolean");

    boolean result = PartialUpdateStrategy.shouldKeepConsistentLogicalTimestamp(props);
    assertFalse(result);
  }

  @Test
  void testExtractEventTimeWhenEventTimePresent() {
    Object rawEventTime = 18317L; // e.g. days since epoch
    Object convertedEventTime = "2020-02-01"; // after logical type conversion
    String eventTimeField = "ts";

    HoodieReaderContext mockContext = mock(HoodieReaderContext.class);
    ReaderContextTypeHandler mockTypeHandler = mock(ReaderContextTypeHandler.class);
    GenericRecord record = new GenericData.Record(schema);
    record.put(eventTimeField, rawEventTime);

    when(mockContext.getEventTime(record, schema, Option.of(eventTimeField)))
        .thenReturn(Option.of(rawEventTime));
    when(mockContext.getEventTime(any(), any(), any())).thenReturn(Option.of(18317L));
    when(mockContext.getTypeHandler()).thenReturn(mockTypeHandler);
    when(mockTypeHandler.convertValueForAvroLogicalTypes(any(), eq(rawEventTime), eq(true)))
        .thenReturn(convertedEventTime);

    TypedProperties props = new TypedProperties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "ts");
    props.setProperty("hoodie.write.track.event.time.watermark", "true");
    PartialUpdateStrategy partialUpdateStrategy =
        new PartialUpdateStrategy(mockContext, PartialUpdateMode.KEEP_VALUES, props);
    Option<Object> result = partialUpdateStrategy.extractEventTime(record, schema);

    assertTrue(result.isPresent());
    assertEquals(convertedEventTime, result.get());
  }

  @Test
  void testExtractEventTime_whenEventTimeMissing() {
    GenericRecord record = new GenericData.Record(schema);
    HoodieReaderContext mockContext = mock(HoodieReaderContext.class);
    when(mockContext.getEventTime(record, schema, Option.of("ts")))
        .thenReturn(Option.empty());

    TypedProperties props = new TypedProperties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "ts");
    props.setProperty("hoodie.write.track.event.time.watermark", "true");
    PartialUpdateStrategy partialUpdateStrategy =
        new PartialUpdateStrategy(mockContext, PartialUpdateMode.KEEP_VALUES, props);
    Option<Object> result = partialUpdateStrategy.extractEventTime(record, schema);

    assertTrue(result.isEmpty());
  }
}
