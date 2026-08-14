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

package org.apache.hudi.table.catalog;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the Flink and Hive schema conversions used by the Hive catalog.
 */
class TestHiveSchemaUtils {

  @ParameterizedTest
  @MethodSource("flinkToHiveTypes")
  void testFlinkToHiveTypeConversion(DataType flinkType, String expectedHiveType) {
    assertEquals(
        expectedHiveType,
        HiveSchemaUtils.toHiveTypeInfo(flinkType.getLogicalType()).getTypeName());
  }

  private static Stream<Arguments> flinkToHiveTypes() {
    return Stream.of(
        Arguments.of(DataTypes.CHAR(4), "string"),
        Arguments.of(DataTypes.VARCHAR(12), "string"),
        Arguments.of(DataTypes.BOOLEAN(), "boolean"),
        Arguments.of(DataTypes.BYTES(), "binary"),
        Arguments.of(DataTypes.DECIMAL(12, 3), "decimal(12,3)"),
        Arguments.of(DataTypes.TINYINT(), "int"),
        Arguments.of(DataTypes.SMALLINT(), "int"),
        Arguments.of(DataTypes.INT(), "int"),
        Arguments.of(DataTypes.BIGINT(), "bigint"),
        Arguments.of(DataTypes.FLOAT(), "float"),
        Arguments.of(DataTypes.DOUBLE(), "double"),
        Arguments.of(DataTypes.DATE(), "date"),
        Arguments.of(DataTypes.TIMESTAMP(6), "timestamp"),
        Arguments.of(DataTypes.TIMESTAMP(9), "bigint"),
        Arguments.of(DataTypes.ARRAY(DataTypes.INT()), "array<int>"),
        Arguments.of(
            DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.BIGINT())),
            "map<string,array<bigint>>"),
        Arguments.of(
            DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))),
            "struct<id:int,attributes:map<string,string>>"));
  }

  @ParameterizedTest
  @MethodSource("hiveToFlinkTypes")
  void testHiveToFlinkTypeConversion(String hiveType, DataType expectedFlinkType) {
    assertEquals(
        expectedFlinkType,
        HiveSchemaUtils.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(hiveType)));
  }

  private static Stream<Arguments> hiveToFlinkTypes() {
    return Stream.of(
        Arguments.of("char(4)", DataTypes.CHAR(4)),
        Arguments.of("varchar(12)", DataTypes.VARCHAR(12)),
        Arguments.of("string", DataTypes.STRING()),
        Arguments.of("boolean", DataTypes.BOOLEAN()),
        Arguments.of("tinyint", DataTypes.TINYINT()),
        Arguments.of("smallint", DataTypes.SMALLINT()),
        Arguments.of("int", DataTypes.INT()),
        Arguments.of("bigint", DataTypes.BIGINT()),
        Arguments.of("float", DataTypes.FLOAT()),
        Arguments.of("double", DataTypes.DOUBLE()),
        Arguments.of("date", DataTypes.DATE()),
        Arguments.of("timestamp", DataTypes.TIMESTAMP(6)),
        Arguments.of("binary", DataTypes.BYTES()),
        Arguments.of("decimal(12,3)", DataTypes.DECIMAL(12, 3)),
        Arguments.of("array<int>", DataTypes.ARRAY(DataTypes.INT())),
        Arguments.of(
            "map<string,array<bigint>>",
            DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.BIGINT()))),
        Arguments.of(
            "struct<id:int,attributes:map<string,string>>",
            DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())))));
  }

  @Test
  void testUnsupportedTypeConversions() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> HiveSchemaUtils.toHiveTypeInfo(DataTypes.VARBINARY(4).getLogicalType()));
    assertThrows(
        UnsupportedOperationException.class,
        () -> HiveSchemaUtils.toHiveTypeInfo(DataTypes.TIME().getLogicalType()));
    assertThrows(
        UnsupportedOperationException.class,
        () -> HiveSchemaUtils.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString("void")));
    assertThrows(
        UnsupportedOperationException.class,
        () -> HiveSchemaUtils.toFlinkType(
            TypeInfoUtils.getTypeInfoFromTypeString("uniontype<int,string>")));
    assertThrows(NullPointerException.class, () -> HiveSchemaUtils.toFlinkType(null));
    assertThrows(NullPointerException.class, () -> HiveSchemaUtils.toHiveTypeInfo(null));

    assertEquals("void", HiveSchemaUtils.toHiveTypeInfo(new NullType()).getTypeName());
    assertEquals(
        "int",
        DataTypes.INT().getLogicalType().accept(new TypeInfoLogicalTypeVisitor(DataTypes.INT())).getTypeName());
  }

  @Test
  void testHiveTableSchemaRoundTrip() {
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(Arrays.asList(
        new FieldSchema(HoodieRecord.COMMIT_TIME_METADATA_FIELD, "string", null),
        new FieldSchema("id", "int", null),
        new FieldSchema("payload", "struct<name:string,scores:array<int>>", null)));

    Table hiveTable = new Table();
    hiveTable.setSd(storageDescriptor);
    hiveTable.setPartitionKeys(Collections.singletonList(new FieldSchema("part", "string", null)));
    Map<String, String> parameters = new HashMap<>();
    parameters.put(FlinkOptions.RECORD_KEY_FIELD.key(), "id");
    parameters.put(TableOptionProperties.PK_CONSTRAINT_NAME, "pk_hms");
    parameters.put(TableOptionProperties.METADATA_COLUMNS, HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    hiveTable.setParameters(parameters);

    Schema flinkSchema = HiveSchemaUtils.convertTableSchema(hiveTable);
    assertEquals(
        Arrays.asList("id", "payload", "part", HoodieRecord.COMMIT_TIME_METADATA_FIELD),
        flinkSchema.getColumns().stream()
            .map(Schema.UnresolvedColumn::getName)
            .collect(Collectors.toList()));
    assertEquals(
        Collections.singletonList("id"),
        flinkSchema.getPrimaryKey().get().getColumnNames());
    assertEquals("pk_hms", flinkSchema.getPrimaryKey().get().getConstraintName());
    assertTrue(flinkSchema.getColumns().get(0).toString().contains("NOT NULL"));
    assertTrue(
        flinkSchema.getColumns().get(3) instanceof Schema.UnresolvedMetadataColumn);

    List<FieldSchema> roundTrip = HiveSchemaUtils.toHiveFieldSchema(flinkSchema, true);
    assertEquals(
        HoodieRecord.HOODIE_META_COLUMNS.size() + 1 + 3,
        roundTrip.size());
    assertTrue(
        HiveSchemaUtils.getFieldNames(roundTrip)
            .contains(HoodieRecord.OPERATION_METADATA_FIELD));
    assertEquals("int", fieldType(roundTrip, "id"));
    assertEquals("struct<name:string,scores:array<int>>", fieldType(roundTrip, "payload"));
  }

  @Test
  void testSplitSchemaByPartitionKeys() {
    List<FieldSchema> fields = Arrays.asList(
        new FieldSchema("id", "int", null),
        new FieldSchema("region", "string", null),
        new FieldSchema("day", "date", null));

    Pair<List<FieldSchema>, List<FieldSchema>> split =
        HiveSchemaUtils.splitSchemaByPartitionKeys(fields, Arrays.asList("day", "region"));

    assertEquals(Collections.singletonList("id"), HiveSchemaUtils.getFieldNames(split.getLeft()));
    assertEquals(Arrays.asList("region", "day"), HiveSchemaUtils.getFieldNames(split.getRight()));
    assertFalse(split.getLeft().isEmpty());
  }

  private static String fieldType(List<FieldSchema> fields, String name) {
    return fields.stream()
        .filter(field -> field.getName().equals(name))
        .findFirst()
        .orElseThrow(AssertionError::new)
        .getType();
  }
}
