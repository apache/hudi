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
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.schema.SimpleSchemaProvider;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

  @ParameterizedTest
  @EnumSource(HoodieRecordType.class)
  public void testCreateHoodieRecordsWithError(HoodieRecordType recordType) {
    Schema schema = new Schema.Parser().parse(SCHEMA_STRING);
    JavaRDD<GenericRecord> recordRdd = jsc.parallelize(Collections.singletonList(1)).map(i -> {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i * 1000L);
      record.put(1, "key" + i);
      record.put(2, "path" + i);
      // The field is non-null in schema but the value is null, so this fails the Hudi record creation
      record.put(3, null);
      record.put(4, "driver");
      return record;
    });
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    TypedProperties props = new TypedProperties();
    SchemaProvider schemaProvider = new SimpleSchemaProvider(jsc, schema, props);
    BaseErrorTableWriter errorTableWriter = Mockito.mock(BaseErrorTableWriter.class);
    ArgumentCaptor<JavaRDD<?>> errorEventCaptor = ArgumentCaptor.forClass(JavaRDD.class);
    doNothing().when(errorTableWriter).addErrorEvents(errorEventCaptor.capture());
    HoodieStreamerUtils.createHoodieRecords(cfg, props, Option.of(recordRdd),
                schemaProvider, recordType, false, "000", Option.of(errorTableWriter));
    List<ErrorEvent<String>> actualErrorEvents = (List<ErrorEvent<String>>) errorEventCaptor.getValue().collect();
    ErrorEvent<String> expectedErrorEvent = new ErrorEvent<>("{\"timestamp\": 1000, \"_row_key\": \"key1\", \"partition_path\": \"path1\", \"rider\": null, \"driver\": \"driver\"}",
        ErrorEvent.ErrorReason.RECORD_CREATION);
    assertEquals(Collections.singletonList(expectedErrorEvent), actualErrorEvents);
  }
}
