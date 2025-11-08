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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.exception.HoodieRecordCreationException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.schema.SimpleSchemaProvider;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;

/**
 * Tests {@link HoodieStreamerUtils}.
 */
public class TestHoodieStreamerUtils extends UtilitiesTestBase {
  private static final String SCHEMA_STRING = "{\"type\": \"record\"," + "\"name\": \"rec\"," + "\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"long\"}," + "{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"partition_path\", \"type\": [\"null\", \"string\"], \"default\": null },"
      + "{\"name\": \"rider\", \"type\": \"string\"}," + "{\"name\": \"driver\", \"type\": \"string\"}]}";

  @BeforeAll
  public static void setupOnce() throws Exception {
    initTestServices();
  }

  private static Stream<Arguments> testCreateHoodieRecords() {
    HoodieRecordType[] recordTypes = {HoodieRecordType.SPARK, HoodieRecordType.AVRO};
    Boolean[] booleanValues = {true, false};
    String[] recordKeyFields = {"_row_key", "rider"};

    return Stream.of(recordTypes)
        .flatMap(recordType ->
            Stream.of(booleanValues)
                .flatMap(booleanValue ->
                    Stream.of(recordKeyFields)
                        .map(recordKeyField -> Arguments.of(recordType, booleanValue, recordKeyField))));
  }

  @ParameterizedTest
  @MethodSource("testCreateHoodieRecords")
  void testCreateValidHoodieRecords(
      HoodieRecordType recordType, boolean enableErrorTableWriter, String recordKeyField) {
    Schema schema = new Schema.Parser().parse(SCHEMA_STRING);
    JavaRDD<GenericRecord> recordRdd = jsc.parallelize(Collections.singletonList(1)).map(i -> {
      GenericRecord genericRecord = new GenericData.Record(schema);
      genericRecord.put(0, i * 1000L);
      genericRecord.put(1, "key" + i);
      genericRecord.put(2, "path" + i);
      genericRecord.put(3, "rider1");
      genericRecord.put(4, "driver1");
      return genericRecord;
    });
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.payloadClassName = DefaultHoodieRecordPayload.class.getName();
    TypedProperties props = new TypedProperties();
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), recordKeyField);
    Option<BaseErrorTableWriter> errorTableWriter = enableErrorTableWriter
        ? Option.of(Mockito.mock(BaseErrorTableWriter.class)) : Option.empty();
    ArgumentCaptor<JavaRDD<?>> errorEventCaptor = ArgumentCaptor.forClass(JavaRDD.class);
    if (errorTableWriter.isPresent()) {
      doNothing().when(errorTableWriter.get()).addErrorEvents(errorEventCaptor.capture());
    }
    Option<JavaRDD<HoodieRecord>> recordOpt = HoodieStreamerUtils.createHoodieRecords(cfg, props, Option.of(recordRdd),
        new SimpleSchemaProvider(jsc, schema, props), recordType, false, "000", errorTableWriter, new HoodieTableConfig());

    if (errorTableWriter.isPresent()) {
      assertEquals(0, errorEventCaptor.getValue().collect().size());
    }
    assertTrue(recordOpt.isPresent());
    List<HoodieRecord> records = recordOpt.get().collect();
    assertEquals(1, records.size());
    Object[] columnValues = records.get(0).getColumnValues(
        schema, new String[] {"timestamp", "_row_key", "partition_path", "rider", "driver"}, false);
    Object[] expectedValues = recordType == HoodieRecordType.SPARK
        ? new Object[] {1000L, UTF8String.fromString("key1"), UTF8String.fromString("path1"),
        UTF8String.fromString("rider1"), UTF8String.fromString("driver1")}
        : new Object[] {1000L, new Utf8("key1"), new Utf8("path1"), new Utf8("rider1"), new Utf8("driver1")};
    assertArrayEquals(expectedValues, columnValues);
  }

  @ParameterizedTest
  @MethodSource("testCreateHoodieRecords")
  void testCreateHoodieRecordsWithError(
      HoodieRecordType recordType, boolean enableErrorTableWriter, String recordKeyField) {
    Schema schema = new Schema.Parser().parse(SCHEMA_STRING);
    JavaRDD<GenericRecord> recordRdd = jsc.parallelize(Collections.singletonList(1)).map(i -> {
      GenericRecord genericRecord = new GenericData.Record(schema);
      genericRecord.put(0, i * 1000L);
      genericRecord.put(1, "key" + i);
      genericRecord.put(2, "path" + i);
      // The field is non-null in schema but the value is null, so this fails the Hudi record creation
      genericRecord.put(3, null);
      genericRecord.put(4, "driver");
      return genericRecord;
    });
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.payloadClassName = DefaultHoodieRecordPayload.class.getName();
    TypedProperties props = new TypedProperties();
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), recordKeyField);
    SchemaProvider schemaProvider = new SimpleSchemaProvider(jsc, schema, props);
    Option<BaseErrorTableWriter> errorTableWriter = enableErrorTableWriter
        ? Option.of(Mockito.mock(BaseErrorTableWriter.class)) : Option.empty();
    ArgumentCaptor<JavaRDD<?>> errorEventCaptor = ArgumentCaptor.forClass(JavaRDD.class);
    if (errorTableWriter.isPresent()) {
      doNothing().when(errorTableWriter.get()).addErrorEvents(errorEventCaptor.capture());
    }
    Option<JavaRDD<HoodieRecord>> records = HoodieStreamerUtils.createHoodieRecords(cfg, props, Option.of(recordRdd),
        schemaProvider, recordType, false, "000", errorTableWriter, new HoodieTableConfig());
    assertTrue(records.isPresent());

    if (enableErrorTableWriter) {
      List<ErrorEvent<String>> actualErrorEvents = (List<ErrorEvent<String>>) errorEventCaptor.getValue().collect();
      ErrorEvent<String> expectedErrorEvent = new ErrorEvent<>("{\"timestamp\": 1000, \"_row_key\": \"key1\", \"partition_path\": \"path1\", \"rider\": null, \"driver\": \"driver\"}",
          ErrorEvent.ErrorReason.RECORD_CREATION);
      assertEquals(Collections.singletonList(expectedErrorEvent), actualErrorEvents);
    } else {
      SparkException sparkException = assertThrows(SparkException.class, () -> records.get().collect());
      if (recordType == HoodieRecordType.SPARK) {
        // With SPARK record type, the exception should be thrown from HoodieStreamerUtils.createHoodieRecords
        assertEquals(HoodieRecordCreationException.class, sparkException.getCause().getClass());
        assertEquals("Failed to create Hoodie Record", sparkException.getCause().getMessage());
        assertEquals(
            recordKeyField.equals("rider") ? IllegalArgumentException.class : NullPointerException.class,
            sparkException.getCause().getCause().getClass());
        assertEquals(
            recordKeyField.equals("rider")
                ? "Found null value for the field that is declared as non-nullable: StructField(rider,StringType,false)"
                : null,
            sparkException.getCause().getCause().getMessage());
      } else {
        // With AVRO record type, the HoodieAvroIndexedRecord is successfully created
        // by HoodieStreamerUtils.createHoodieRecords because of lazy Avro encoding;
        // when collecting the record to the driver, the Avro encoding is triggered,
        // which is expected to throw an exception
        assertEquals(
            recordKeyField.equals("rider") ? HoodieKeyException.class : NullPointerException.class,
            sparkException.getCause().getClass());
        assertEquals(
            recordKeyField.equals("rider")
                ? "recordKey value: \"null\" for field: \"rider\" cannot be null or empty."
                : "null value for (non-nullable) string at rec.rider",
            sparkException.getCause().getMessage());
      }
    }
  }
}
