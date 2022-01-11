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

import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StringToRowDataConverter;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoField;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Test cases for {@link StringToRowDataConverter}.
 */
public class TestStringToRowDataConverter {
  @Test
  void testConvert() {
    String[] fields = new String[] {"1.1", "3.4", "2021-03-30", "56669000", "1617119069000", "1617119069666111", "12345.67"};
    LogicalType[] fieldTypes = new LogicalType[] {
        DataTypes.FLOAT().getLogicalType(),
        DataTypes.DOUBLE().getLogicalType(),
        DataTypes.DATE().getLogicalType(),
        DataTypes.TIME(3).getLogicalType(),
        DataTypes.TIMESTAMP(3).getLogicalType(),
        DataTypes.TIMESTAMP(6).getLogicalType(),
        DataTypes.DECIMAL(7, 2).getLogicalType()
    };
    StringToRowDataConverter converter = new StringToRowDataConverter(fieldTypes);
    Object[] converted = converter.convert(fields);
    Object[] expected = new Object[] {
        1.1f, 3.4D, (int) LocalDate.parse("2021-03-30").toEpochDay(),
        LocalTime.parse("15:44:29").get(ChronoField.MILLI_OF_DAY),
        TimestampData.fromInstant(Instant.parse("2021-03-30T15:44:29Z")),
        TimestampData.fromInstant(Instant.parse("2021-03-30T15:44:29.666111Z")),
        DecimalData.fromBigDecimal(new BigDecimal("12345.67"), 7, 2)
    };
    assertArrayEquals(expected, converted);
  }

  @Test
  void testRowDataToAvroStringToRowData() {
    GenericRowData rowData = new GenericRowData(7);
    rowData.setField(0, 1.1f);
    rowData.setField(1, 3.4D);
    rowData.setField(2, (int) LocalDate.parse("2021-03-30").toEpochDay());
    rowData.setField(3, LocalTime.parse("15:44:29").get(ChronoField.MILLI_OF_DAY));
    rowData.setField(4, TimestampData.fromInstant(Instant.parse("2021-03-30T15:44:29Z")));
    rowData.setField(5, TimestampData.fromInstant(Instant.parse("2021-03-30T15:44:29.666111Z")));
    rowData.setField(6, DecimalData.fromBigDecimal(new BigDecimal("12345.67"), 7, 2));

    DataType dataType = DataTypes.ROW(
        DataTypes.FIELD("f_float", DataTypes.FLOAT()),
        DataTypes.FIELD("f_double", DataTypes.DOUBLE()),
        DataTypes.FIELD("f_date", DataTypes.DATE()),
        DataTypes.FIELD("f_time", DataTypes.TIME(3)),
        DataTypes.FIELD("f_timestamp", DataTypes.TIMESTAMP(3)),
        DataTypes.FIELD("f_timestamp_micros", DataTypes.TIMESTAMP(6)),
        DataTypes.FIELD("f_decimal", DataTypes.DECIMAL(7, 2))
    );
    RowType rowType = (RowType) dataType.getLogicalType();
    RowDataToAvroConverters.RowDataToAvroConverter converter =
        RowDataToAvroConverters.createConverter(rowType);
    GenericRecord avroRecord =
        (GenericRecord) converter.convert(AvroSchemaConverter.convertToSchema(rowType), rowData);
    StringToRowDataConverter stringToRowDataConverter =
        new StringToRowDataConverter(rowType.getChildren().toArray(new LogicalType[0]));
    final String recordKey = KeyGenUtils.getRecordKey(avroRecord, rowType.getFieldNames(), false);
    final String[] recordKeys = KeyGenUtils.extractRecordKeys(recordKey);
    Object[] convertedKeys = stringToRowDataConverter.convert(recordKeys);

    GenericRowData converted = new GenericRowData(7);
    for (int i = 0; i < 7; i++) {
      converted.setField(i, convertedKeys[i]);
    }
    assertThat(converted, is(rowData));
  }
}
