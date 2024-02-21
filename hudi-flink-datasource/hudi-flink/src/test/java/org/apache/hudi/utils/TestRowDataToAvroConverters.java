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

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.RowDataToAvroConverters;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.FIELD;
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
            (GenericRecord) converter.convert(AvroSchemaConverter.convertToSchema(rowType), rowData);
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
            (GenericRecord) converter.convert(AvroSchemaConverter.convertToSchema(rowType), rowData);
    Assertions.assertEquals(timestampFromUtc0, formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) avroRecord.get(0)), ZoneId.of("UTC"))));
    Assertions.assertEquals("2021-03-30 08:44:29", formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) avroRecord.get(0)), ZoneId.of("UTC+1"))));
    Assertions.assertEquals("2021-03-30 15:44:29", formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) avroRecord.get(0)), ZoneId.of("Asia/Shanghai"))));
  }
}