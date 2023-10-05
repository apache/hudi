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

package org.apache.hudi.gcp.bigquery;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.hudi.gcp.bigquery.BigQuerySchemaResolver.schemaToSqlString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestBigQuerySchemaResolver {
  private static final com.google.cloud.bigquery.Schema PRIMITIVE_TYPES_BQ_SCHEMA = com.google.cloud.bigquery.Schema.of(
      Field.newBuilder("requiredBoolean", StandardSQLTypeName.BOOL).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("optionalBoolean", StandardSQLTypeName.BOOL).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("requiredInt", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("optionalInt", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("requiredLong", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("optionalLong", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("requiredDouble", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("optionalDouble", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("requiredFloat", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("optionalFloat", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("requiredString", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("optionalString", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("requiredBytes", StandardSQLTypeName.BYTES).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("optionalBytes", StandardSQLTypeName.BYTES).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("requiredEnum", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("optionalEnum", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
  private static final Schema PRIMITIVE_TYPES = SchemaBuilder.record("testRecord")
      .fields()
      .requiredBoolean("requiredBoolean")
      .optionalBoolean("optionalBoolean")
      .requiredInt("requiredInt")
      .optionalInt("optionalInt")
      .requiredLong("requiredLong")
      .optionalLong("optionalLong")
      .requiredDouble("requiredDouble")
      .optionalDouble("optionalDouble")
      .requiredFloat("requiredFloat")
      .optionalFloat("optionalFloat")
      .requiredString("requiredString")
      .optionalString("optionalString")
      .requiredBytes("requiredBytes")
      .optionalBytes("optionalBytes")
      .name("requiredEnum").type().enumeration("REQUIRED_ENUM").symbols("ONE", "TWO").enumDefault("ONE")
      .name("optionalEnum").type().optional().enumeration("OPTIONAL_ENUM").symbols("ONE", "TWO")
      .endRecord();
  private static final Schema NESTED_FIELDS = SchemaBuilder.record("testRecord")
      .fields()
      .name("nestedOne")
      .type()
      .optional()
      .record("nestedOneType").fields()
      .optionalInt("nestedOptionalInt")
      .requiredDouble("nestedRequiredDouble")
      .name("nestedTwo")
      .type(SchemaBuilder.record("nestedTwoType").fields()
          .optionalString("doublyNestedString").endRecord()).noDefault()
      .endRecord()
      .endRecord();
  private static final Schema LISTS = SchemaBuilder.record("testRecord")
      .fields()
      .name("intList")
      .type()
      .array()
      .items()
      .intType().noDefault()
      .name("recordList")
      .type()
      .nullable()
      .array()
      .items(SchemaBuilder.record("randomname").fields().requiredDouble("requiredDouble").optionalString("optionalString").endRecord())
      .noDefault()
      .endRecord();
  private static final BigQuerySchemaResolver SCHEMA_RESOLVER = BigQuerySchemaResolver.getInstance();

  @Test
  void convertSchema_primitiveFields() {
    Assertions.assertEquals(PRIMITIVE_TYPES_BQ_SCHEMA, SCHEMA_RESOLVER.convertSchema(PRIMITIVE_TYPES));
  }

  @Test
  void convertSchemaToString_primitiveTypes() {
    String expectedSqlSchema = "`requiredBoolean` BOOL NOT NULL, "
        + "`optionalBoolean` BOOL, "
        + "`requiredInt` INT64 NOT NULL, "
        + "`optionalInt` INT64, "
        + "`requiredLong` INT64 NOT NULL, "
        + "`optionalLong` INT64, "
        + "`requiredDouble` FLOAT64 NOT NULL, "
        + "`optionalDouble` FLOAT64, "
        + "`requiredFloat` FLOAT64 NOT NULL, "
        + "`optionalFloat` FLOAT64, "
        + "`requiredString` STRING NOT NULL, "
        + "`optionalString` STRING, "
        + "`requiredBytes` BYTES NOT NULL, "
        + "`optionalBytes` BYTES, "
        + "`requiredEnum` STRING NOT NULL, "
        + "`optionalEnum` STRING";
    Assertions.assertEquals(expectedSqlSchema, schemaToSqlString(SCHEMA_RESOLVER.convertSchema(PRIMITIVE_TYPES)));
  }

  @Test
  void convertSchema_nestedFields() {
    com.google.cloud.bigquery.Schema expected = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("nestedOne", StandardSQLTypeName.STRUCT,
                Field.newBuilder("nestedOptionalInt", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("nestedRequiredDouble", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("nestedTwo", StandardSQLTypeName.STRUCT,
                    Field.newBuilder("doublyNestedString", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()).setMode(Field.Mode.REQUIRED).build())
            .setMode(Field.Mode.NULLABLE).build());

    Assertions.assertEquals(expected, SCHEMA_RESOLVER.convertSchema(NESTED_FIELDS));
  }

  @Test
  void convertSchemaToString_nestedFields() {
    String expectedSqlSchema = "`nestedOne` STRUCT<"
        + "`nestedOptionalInt` INT64, "
        + "`nestedRequiredDouble` FLOAT64 NOT NULL, "
        + "`nestedTwo` STRUCT<`doublyNestedString` STRING> NOT NULL>";
    Assertions.assertEquals(expectedSqlSchema, schemaToSqlString(SCHEMA_RESOLVER.convertSchema(NESTED_FIELDS)));
  }

  @Test
  void convertSchema_lists() {
    Field intListField = Field.newBuilder("intList", StandardSQLTypeName.INT64).setMode(Field.Mode.REPEATED).build();

    Field requiredDoubleField = Field.newBuilder("requiredDouble", StandardSQLTypeName.FLOAT64)
        .setMode(Field.Mode.REQUIRED)
        .build();
    Field optionalStringField = Field.newBuilder("optionalString", StandardSQLTypeName.STRING)
        .setMode(Field.Mode.NULLABLE)
        .build();
    Field recordListField = Field.newBuilder("recordList", StandardSQLTypeName.STRUCT,
        requiredDoubleField, optionalStringField).setMode(Field.Mode.REPEATED).build();


    com.google.cloud.bigquery.Schema expected =
        com.google.cloud.bigquery.Schema.of(intListField, recordListField);
    Assertions.assertEquals(expected, SCHEMA_RESOLVER.convertSchema(LISTS));
  }

  @Test
  void convertSchemaToString_lists() {
    String expectedSqlSchema = "`intList` ARRAY<INT64>, "
        + "`recordList` ARRAY<STRUCT<`requiredDouble` FLOAT64 NOT NULL, `optionalString` STRING>>";
    Assertions.assertEquals(expectedSqlSchema, schemaToSqlString(SCHEMA_RESOLVER.convertSchema(LISTS)));
  }

  @Test
  void convertSchemaListOfNullableRecords() {
    Schema nestedRecordType = SchemaBuilder.record("nested_record").fields().optionalString("inner_string_field").endRecord();
    Schema input = SchemaBuilder.record("top_level_schema")
        .fields().name("top_level_schema_field")
        .type()
        .nullable()
        .array()
        .items(SchemaBuilder.unionOf().nullType().and().type(nestedRecordType).endUnion())
        .noDefault()
        .endRecord();

    Field innerStringField = Field.newBuilder("inner_string_field", StandardSQLTypeName.STRING)
        .setMode(Field.Mode.NULLABLE)
        .build();
    Field topLevelSchemaField = Field.newBuilder("top_level_schema_field", StandardSQLTypeName.STRUCT,
        innerStringField).setMode(Field.Mode.REPEATED).build();

    com.google.cloud.bigquery.Schema expected = com.google.cloud.bigquery.Schema.of(topLevelSchemaField);
    Assertions.assertEquals(expected, SCHEMA_RESOLVER.convertSchema(input));
  }

  @Test
  void convertSchema_logicalTypes() {
    String schemaString = "{\"type\":\"record\",\"name\":\"logicalTypes\",\"fields\":[{\"name\":\"int_date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
        + "{\"name\":\"int_time_millis\",\"type\":{\"type\":\"int\",\"logicalType\":\"time-millis\"}},{\"name\":\"long_time_micros\",\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}},"
        + "{\"name\":\"long_timestamp_millis\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
        + "{\"name\":\"long_timestamp_micros\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
        + "{\"name\":\"long_timestamp_millis_local\",\"type\":{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}},"
        + "{\"name\":\"long_timestamp_micros_local\",\"type\":{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}},"
        + "{\"name\":\"bytes_decimal\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\", \"precision\": 4, \"scale\": 2}}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema input = parser.parse(schemaString);

    com.google.cloud.bigquery.Schema expected = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("int_date", StandardSQLTypeName.DATE).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("int_time_millis", StandardSQLTypeName.TIME).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("long_time_micros", StandardSQLTypeName.TIME).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("long_timestamp_millis", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("long_timestamp_micros", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("long_timestamp_millis_local", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("long_timestamp_micros_local", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("bytes_decimal", StandardSQLTypeName.NUMERIC).setMode(Field.Mode.REQUIRED).build());

    Assertions.assertEquals(expected, SCHEMA_RESOLVER.convertSchema(input));
  }

  @Test
  void convertSchema_maps() {
    Schema input = SchemaBuilder.record("testRecord")
        .fields()
        .name("intMap")
        .type()
        .map()
        .values()
        .intType().noDefault()
        .name("recordMap")
        .type()
        .nullable()
        .map()
        .values(SchemaBuilder.record("element").fields().requiredDouble("requiredDouble").optionalString("optionalString").endRecord())
        .noDefault()
        .endRecord();


    com.google.cloud.bigquery.Schema expected = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("intMap", StandardSQLTypeName.STRUCT,
                Field.newBuilder("key_value", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("key", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                        Field.newBuilder("value", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build())
                    .setMode(Field.Mode.REPEATED).build())
            .setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("recordMap", StandardSQLTypeName.STRUCT,
                Field.newBuilder("key_value", StandardSQLTypeName.STRUCT,
                    Field.newBuilder("key", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                    Field.newBuilder("value", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("requiredDouble", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REQUIRED).build(),
                        Field.newBuilder("optionalString", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
                    ).setMode(Field.Mode.REQUIRED).build()).setMode(Field.Mode.REPEATED).build())
            .setMode(Field.Mode.NULLABLE).build());

    Assertions.assertEquals(expected, SCHEMA_RESOLVER.convertSchema(input));
  }

  @Test
  void getTableSchema_withPartitionFields() throws Exception {
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    TableSchemaResolver mockTableSchemaResolver = mock(TableSchemaResolver.class);
    when(mockTableSchemaResolver.getTableAvroSchema()).thenReturn(PRIMITIVE_TYPES);
    BigQuerySchemaResolver resolver = new BigQuerySchemaResolver(metaClient -> mockTableSchemaResolver);

    com.google.cloud.bigquery.Schema expected = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("requiredBoolean", StandardSQLTypeName.BOOL).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("optionalBoolean", StandardSQLTypeName.BOOL).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("requiredInt", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("optionalInt", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("requiredLong", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("optionalLong", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("requiredDouble", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("optionalDouble", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("requiredFloat", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("optionalFloat", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("optionalString", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("requiredBytes", StandardSQLTypeName.BYTES).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("optionalBytes", StandardSQLTypeName.BYTES).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("requiredEnum", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("optionalEnum", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());

    // expect 'requiredString' field to be removed
    Assertions.assertEquals(expected, resolver.getTableSchema(mockMetaClient, Collections.singletonList("requiredString")));
  }

  @Test
  void getTableSchema_withoutPartitionFields() throws Exception {
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    TableSchemaResolver mockTableSchemaResolver = mock(TableSchemaResolver.class);
    when(mockTableSchemaResolver.getTableAvroSchema()).thenReturn(PRIMITIVE_TYPES);
    when(mockTableSchemaResolver.getTableAvroSchema()).thenReturn(PRIMITIVE_TYPES);
    BigQuerySchemaResolver resolver = new BigQuerySchemaResolver(metaClient -> mockTableSchemaResolver);
    Assertions.assertEquals(PRIMITIVE_TYPES_BQ_SCHEMA, resolver.getTableSchema(mockMetaClient, Collections.emptyList()));
  }
}
