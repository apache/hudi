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
import org.apache.avro.util.Utf8;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
  void testRowDataToAvroBlobTypeFieldWritesEnumSymbol() {
    // Flink models the BLOB `type` discriminator as STRING, but its Avro encoding is an ENUM
    // (blob_storage_type). The converter must emit a GenericData.EnumSymbol, not a plain Utf8,
    // otherwise Avro log-block writes (MOR) fail with "value OUT_OF_LINE (a Utf8) is not a
    // blob_storage_type".
    DataType blobRow = DataTypes.ROW(
        DataTypes.FIELD(HoodieSchema.Blob.TYPE, DataTypes.STRING().notNull()),
        DataTypes.FIELD(HoodieSchema.Blob.INLINE_DATA_FIELD, DataTypes.BYTES().nullable()),
        DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE, DataTypes.ROW(
            DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH, DataTypes.STRING().notNull()),
            DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET, DataTypes.BIGINT().nullable()),
            DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH, DataTypes.BIGINT().nullable()),
            DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED, DataTypes.BOOLEAN().notNull())
        ).nullable()));
    RowType rowType = (RowType) DataTypes.ROW(DataTypes.FIELD("blob_col", blobRow)).getLogicalType();

    GenericRowData reference = new GenericRowData(4);
    reference.setField(0, StringData.fromString("file1.bin"));
    reference.setField(1, 0L);
    reference.setField(2, 100L);
    reference.setField(3, false);

    GenericRowData blob = new GenericRowData(3);
    blob.setField(0, StringData.fromString(HoodieSchema.Blob.OUT_OF_LINE));
    blob.setField(1, null);
    blob.setField(2, reference);

    GenericRowData top = new GenericRowData(1);
    top.setField(0, blob);

    RowDataToAvroConverters.RowDataToAvroConverter converter =
        RowDataToAvroConverters.createConverter(rowType);
    GenericRecord avroRecord =
        (GenericRecord) converter.convert(HoodieSchemaConverter.convertToSchema(rowType), top);

    GenericRecord blobRecord = (GenericRecord) avroRecord.get(0);
    Object typeValue = blobRecord.get(HoodieSchema.Blob.TYPE);
    Assertions.assertInstanceOf(GenericData.EnumSymbol.class, typeValue,
        "BLOB `type` must be written as an Avro EnumSymbol, found: "
            + (typeValue == null ? "null" : typeValue.getClass().getName()));
    Assertions.assertEquals(HoodieSchema.Blob.OUT_OF_LINE, typeValue.toString());
  }

  /**
   * Regression for the {@code createBlobConverter} approach (old design): when a ROW has the same
   * field-name shape as a BLOB but its {@link HoodieSchema} carries a plain {@code STRING} for
   * {@code type} (not an {@code ENUM}), the converter must not crash and must write a plain
   * {@link Utf8} — not a {@link GenericData.EnumSymbol}.
   *
   * <p>The old approach selected {@code createBlobConverter} at creation time via
   * {@code isBlobStructure}, then called
   * {@code new GenericData.EnumSymbol(stringAvroSchema, ...)} at runtime — which either threw or
   * wrote the wrong representation.  With Option-1 the ENUM branch is gated on the runtime
   * {@code HoodieSchema}, so this scenario falls through to a plain {@link Utf8}.
   */
  @Test
  void testBlobShapedRowWithPlainStringSchemaWritesUtf8() {
    DataType blobShapedRow = DataTypes.ROW(
        DataTypes.FIELD(HoodieSchema.Blob.TYPE, DataTypes.STRING().notNull()),
        DataTypes.FIELD(HoodieSchema.Blob.INLINE_DATA_FIELD, DataTypes.BYTES().nullable()),
        DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE, DataTypes.ROW(
            DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH, DataTypes.STRING().notNull()),
            DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET, DataTypes.BIGINT().nullable()),
            DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH, DataTypes.BIGINT().nullable()),
            DataTypes.FIELD(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED, DataTypes.BOOLEAN().notNull())
        ).nullable()));
    RowType outerRowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("blob_col", blobShapedRow)).getLogicalType();

    // Plain RECORD schema: field[0] is STRING (not ENUM) — mimics a non-BLOB record whose
    // shape happens to match the BLOB structure.
    HoodieSchema refSchema = HoodieSchema.createRecord("reference", null, null, Arrays.asList(
        HoodieSchemaField.of(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH,
            HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET,
            HoodieSchema.createNullable(HoodieSchemaType.LONG)),
        HoodieSchemaField.of(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH,
            HoodieSchema.createNullable(HoodieSchemaType.LONG)),
        HoodieSchemaField.of(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED,
            HoodieSchema.create(HoodieSchemaType.BOOLEAN))
    ));
    HoodieSchema plainBlobShapedSchema = HoodieSchema.createRecord("blob_col", null, null, Arrays.asList(
        HoodieSchemaField.of(HoodieSchema.Blob.TYPE, HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of(HoodieSchema.Blob.INLINE_DATA_FIELD,
            HoodieSchema.createNullable(HoodieSchemaType.BYTES)),
        HoodieSchemaField.of(HoodieSchema.Blob.EXTERNAL_REFERENCE,
            HoodieSchema.createNullable(refSchema))
    ));
    HoodieSchema outerSchema = HoodieSchema.createRecord("outer", null, null, Arrays.asList(
        HoodieSchemaField.of("blob_col", plainBlobShapedSchema)
    ));

    GenericRowData reference = new GenericRowData(4);
    reference.setField(0, StringData.fromString("file1.bin"));
    reference.setField(1, 0L);
    reference.setField(2, 100L);
    reference.setField(3, false);

    GenericRowData blobRow = new GenericRowData(3);
    blobRow.setField(0, StringData.fromString("OUT_OF_LINE"));
    blobRow.setField(1, null);
    blobRow.setField(2, reference);

    GenericRowData top = new GenericRowData(1);
    top.setField(0, blobRow);

    RowDataToAvroConverters.RowDataToAvroConverter converter =
        RowDataToAvroConverters.createConverter(outerRowType);
    GenericRecord avroRecord = (GenericRecord) converter.convert(outerSchema, top);

    GenericRecord blobRecord = (GenericRecord) avroRecord.get(0);
    Object typeValue = blobRecord.get(HoodieSchema.Blob.TYPE);
    Assertions.assertInstanceOf(Utf8.class, typeValue,
        "STRING field must write as Utf8 (not EnumSymbol) when HoodieSchema is not ENUM; found: "
            + (typeValue == null ? "null" : typeValue.getClass().getName()));
    Assertions.assertEquals("OUT_OF_LINE", typeValue.toString());
  }
}