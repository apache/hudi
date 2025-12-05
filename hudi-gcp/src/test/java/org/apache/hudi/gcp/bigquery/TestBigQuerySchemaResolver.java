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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.hudi.gcp.bigquery.BigQuerySchemaResolver.schemaToSqlString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestBigQuerySchemaResolver {
  private static final Schema PRIMITIVE_TYPES_BQ_SCHEMA = Schema.of(
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
  private static final HoodieSchema PRIMITIVE_TYPES = HoodieSchema.createRecord("testRecord", null, null, false, Arrays.asList(
      HoodieSchemaField.of("requiredBoolean", HoodieSchema.create(HoodieSchemaType.BOOLEAN)),
      HoodieSchemaField.of("optionalBoolean", HoodieSchema.createNullable(HoodieSchemaType.BOOLEAN)),
      HoodieSchemaField.of("requiredInt", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("optionalInt", HoodieSchema.createNullable(HoodieSchemaType.INT)),
        HoodieSchemaField.of("requiredLong", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("optionalLong", HoodieSchema.createNullable(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("requiredDouble", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
        HoodieSchemaField.of("optionalDouble", HoodieSchema.createNullable(HoodieSchemaType.DOUBLE)),
        HoodieSchemaField.of("requiredFloat", HoodieSchema.create(HoodieSchemaType.FLOAT)),
        HoodieSchemaField.of("optionalFloat", HoodieSchema.createNullable(HoodieSchemaType.FLOAT)),
        HoodieSchemaField.of("requiredString", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("optionalString", HoodieSchema.createNullable(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("requiredBytes", HoodieSchema.create(HoodieSchemaType.BYTES)),
        HoodieSchemaField.of("optionalBytes", HoodieSchema.createNullable(HoodieSchemaType.BYTES)),
        HoodieSchemaField.of("requiredEnum", HoodieSchema.createEnum("REQUIRED_ENUM", null, null, Arrays.asList("ONE", "TWO"))),
        HoodieSchemaField.of("optionalEnum", HoodieSchema.createNullable(HoodieSchema.createEnum("OPTIONAL_ENUM", null, null, Arrays.asList("ONE", "TWO")))
    )));
  private static final HoodieSchema NESTED_FIELDS = HoodieSchema.createRecord("testRecord", null, null, false, Collections.singletonList(
      HoodieSchemaField.of("nestedOne", HoodieSchema.createNullable(HoodieSchema.createRecord("nestedOneType", null, null, false, Arrays.asList(
          HoodieSchemaField.of("nestedOptionalInt", HoodieSchema.createNullable(HoodieSchemaType.INT)),
          HoodieSchemaField.of("nestedRequiredDouble", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
          HoodieSchemaField.of("nestedTwo", HoodieSchema.createRecord("nestedTwoType", null, null, false, Collections.singletonList(
              HoodieSchemaField.of("doublyNestedString", HoodieSchema.createNullable(HoodieSchemaType.STRING))
          )))
      ))))
  ));
  private static final HoodieSchema LISTS = HoodieSchema.createRecord("testRecord", null, null, false, Arrays.asList(
      HoodieSchemaField.of("intList", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT))),
      HoodieSchemaField.of("recordList", HoodieSchema.createNullable(HoodieSchema.createArray(
          HoodieSchema.createRecord("randomname", null, null, false, Arrays.asList(
              HoodieSchemaField.of("requiredDouble", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
              HoodieSchemaField.of("optionalString", HoodieSchema.createNullable(HoodieSchemaType.STRING))
          ))
      )))
  ));
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
    Schema expected = Schema.of(
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


    Schema expected =
        Schema.of(intListField, recordListField);
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
    HoodieSchema nestedRecordType = 
        HoodieSchema.createRecord("nested_record", null, null, false,
            Collections.singletonList(HoodieSchemaField.of("inner_string_field", HoodieSchema.createNullable(HoodieSchemaType.STRING))));
    HoodieSchema input = HoodieSchema.createRecord("top_level_schema", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("top_level_schema_field", HoodieSchema.createNullable(HoodieSchema.createArray(HoodieSchema.createNullable(nestedRecordType))))));

    Field innerStringField = Field.newBuilder("inner_string_field", StandardSQLTypeName.STRING)
        .setMode(Field.Mode.NULLABLE)
        .build();
    Field topLevelSchemaField = Field.newBuilder("top_level_schema_field", StandardSQLTypeName.STRUCT,
        innerStringField).setMode(Field.Mode.REPEATED).build();

    Schema expected = Schema.of(topLevelSchemaField);
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
    HoodieSchema input = HoodieSchema.parse(schemaString);

    Schema expected = Schema.of(
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
    HoodieSchema input = HoodieSchema.createRecord("testRecord", null, null, false, Arrays.asList(
        HoodieSchemaField.of("intMap", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.INT))),
        HoodieSchemaField.of("recordMap", HoodieSchema.createNullable(HoodieSchema.createMap(
            HoodieSchema.createRecord("element", null, null, false, Arrays.asList(
                HoodieSchemaField.of("requiredDouble", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
                HoodieSchemaField.of("optionalString", HoodieSchema.createNullable(HoodieSchemaType.STRING))
            ))
        )))
    ));

    Schema expected = Schema.of(
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
    when(mockTableSchemaResolver.getTableSchema()).thenReturn(PRIMITIVE_TYPES);
    BigQuerySchemaResolver resolver = new BigQuerySchemaResolver(metaClient -> mockTableSchemaResolver);

    Schema expected = Schema.of(
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
    when(mockTableSchemaResolver.getTableSchema()).thenReturn(PRIMITIVE_TYPES);
    when(mockTableSchemaResolver.getTableSchema()).thenReturn(PRIMITIVE_TYPES);
    BigQuerySchemaResolver resolver = new BigQuerySchemaResolver(metaClient -> mockTableSchemaResolver);
    Assertions.assertEquals(PRIMITIVE_TYPES_BQ_SCHEMA, resolver.getTableSchema(mockMetaClient, Collections.emptyList()));
  }
}
