/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utils;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.RowDataToAvroConverters;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;

class TestRowDataToAvroConverters {

  DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  @Test
  void testRowDataToAvroStringToRowDataWithLocalTimezone() throws JsonProcessingException {
    String timestampFromLocal = "2021-03-30 07:44:29";

    DataType rowDataType = ROW(FIELD("timestamp_from_local", TIMESTAMP()));
    JsonToRowDataConverters.JsonToRowDataConverter jsonToRowDataConverter =
            new JsonToRowDataConverters(true, true, TimestampFormat.SQL)
                    .createConverter(rowDataType.getLogicalType());
    Object rowData = jsonToRowDataConverter.convert(new ObjectMapper().readTree("{\"timestamp_from_local\":\"" + timestampFromLocal + "\"}"));

    RowType rowType = (RowType) DataTypes.ROW(DataTypes.FIELD("f_timestamp", DataTypes.TIMESTAMP(3))).getLogicalType();
    RowDataToAvroConverters.RowDataToAvroConverter converter =
            RowDataToAvroConverters.createConverter(rowType, false);
    GenericRecord avroRecord =
            (GenericRecord) converter.convert(HoodieSchemaConverter.convertToSchema(rowType), rowData);
    Assertions.assertEquals(timestampFromLocal, formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) avroRecord.get(0)), ZoneId.systemDefault())));
  }

  @Test
  void testRowDataToAvroStringToRowDataWithUtcTimezone() throws JsonProcessingException {
    String timestampFromUtc0 = "2021-03-30 07:44:29";

    DataType rowDataType = ROW(FIELD("timestamp_from_utc_0", TIMESTAMP()));
    JsonToRowDataConverters.JsonToRowDataConverter jsonToRowDataConverter =
            new JsonToRowDataConverters(true, true, TimestampFormat.SQL)
                    .createConverter(rowDataType.getLogicalType());
    Object rowData = jsonToRowDataConverter.convert(new ObjectMapper().readTree("{\"timestamp_from_utc_0\":\"" + timestampFromUtc0 + "\"}"));

    RowType rowType = (RowType) DataTypes.ROW(DataTypes.FIELD("f_timestamp", DataTypes.TIMESTAMP(3))).getLogicalType();
    RowDataToAvroConverters.RowDataToAvroConverter converter =
            RowDataToAvroConverters.createConverter(rowType);
    GenericRecord avroRecord =
            (GenericRecord) converter.convert(HoodieSchemaConverter.convertToSchema(rowType), rowData);
    Assertions.assertEquals(timestampFromUtc0, formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) avroRecord.get(0)), ZoneId.of("UTC"))));
    Assertions.assertEquals("2021-03-30 08:44:29", formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) avroRecord.get(0)), ZoneId.of("UTC+1"))));
    Assertions.assertEquals("2021-03-30 15:44:29", formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) avroRecord.get(0)), ZoneId.of("Asia/Shanghai"))));
  }

  @Test
  void testRowDataToAvroVectorEncoding() {
    HoodieSchema schema = HoodieSchema.createRecord(
        "vector_record",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("embedding", HoodieSchema.createVector(2)),
            HoodieSchemaField.of("features", HoodieSchema.createVector(2, HoodieSchema.Vector.VectorElementType.DOUBLE)),
            HoodieSchemaField.of("codes", HoodieSchema.createVector(3, HoodieSchema.Vector.VectorElementType.INT8))));
    RowType rowType = (RowType) HoodieSchemaConverter.convertToDataType(schema).getLogicalType();
    RowDataToAvroConverters.RowDataToAvroConverter converter =
        RowDataToAvroConverters.createConverter(rowType);

    GenericRowData rowData = GenericRowData.of(
        1,
        new GenericArrayData(new float[] {1.0f, 2.0f}),
        new GenericArrayData(new double[] {3.0d, 4.0d}),
        new GenericArrayData(new byte[] {5, 6, 7}));
    GenericRecord avroRecord = (GenericRecord) converter.convert(schema, rowData);

    Assertions.assertArrayEquals(vectorBytes(1.0f, 2.0f),
        ((GenericData.Fixed) avroRecord.get("embedding")).bytes());
    Assertions.assertArrayEquals(vectorBytes(3.0d, 4.0d),
        ((GenericData.Fixed) avroRecord.get("features")).bytes());
    Assertions.assertArrayEquals(new byte[] {5, 6, 7},
        ((GenericData.Fixed) avroRecord.get("codes")).bytes());
  }

  @Test
  void testRowDataToAvroVectorValidation() {
    HoodieSchema schema = HoodieSchema.createRecord(
        "vector_record",
        null,
        null,
        Arrays.asList(HoodieSchemaField.of("embedding", HoodieSchema.createVector(2))));
    RowType rowType = (RowType) HoodieSchemaConverter.convertToDataType(schema).getLogicalType();
    RowDataToAvroConverters.RowDataToAvroConverter converter =
        RowDataToAvroConverters.createConverter(rowType);

    Assertions.assertThrows(IllegalArgumentException.class,
        () -> converter.convert(schema, GenericRowData.of(new GenericArrayData(new float[] {1.0f}))));
  }

  private static byte[] vectorBytes(float... values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * Float.BYTES)
        .order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    for (float value : values) {
      buffer.putFloat(value);
    }
    return buffer.array();
  }

  private static byte[] vectorBytes(double... values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * Double.BYTES)
        .order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    for (double value : values) {
      buffer.putDouble(value);
    }
    return buffer.array();
  }
}
