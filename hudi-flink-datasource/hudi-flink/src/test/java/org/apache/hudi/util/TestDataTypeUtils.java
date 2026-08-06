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

package org.apache.hudi.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.exception.HoodieCatalogException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DataTypeUtils}.
 */
public class TestDataTypeUtils {

  @Test
  public void testToHoodieSchema() {
    HoodieSchema tableSchema = HoodieSchema.createRecord(
        "test_record",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("amount", HoodieSchema.create(HoodieSchemaType.LONG)),
            HoodieSchemaField.of("embedding", HoodieSchema.createVector(
                128, HoodieSchema.Vector.VectorElementType.DOUBLE)),
            HoodieSchemaField.of("payload", HoodieSchema.createVariant())));

    RowType requiredRowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("amount", DataTypes.INT().nullable()),
        DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT().notNull()).notNull()),
        DataTypes.FIELD("payload", DataTypes.ROW(
            DataTypes.FIELD("value", DataTypes.BYTES().notNull()),
            DataTypes.FIELD("metadata", DataTypes.BYTES().notNull())).notNull()),
        DataTypes.FIELD("missing", DataTypes.STRING().nullable()))
        .notNull()
        .getLogicalType();

    HoodieSchema requiredSchema = DataTypeUtils.toHoodieSchema(requiredRowType, tableSchema);

    assertEquals(Arrays.asList("amount", "embedding", "payload", "missing"),
        Arrays.asList(
            requiredSchema.getFields().get(0).name(),
            requiredSchema.getFields().get(1).name(),
            requiredSchema.getFields().get(2).name(),
            requiredSchema.getFields().get(3).name()));
    assertEquals(HoodieSchemaType.UNION, requiredSchema.getField("amount").get().schema().getType());
    assertEquals(HoodieSchemaType.INT, requiredSchema.getField("amount").get().schema().getNonNullType().getType());

    HoodieSchema embeddingSchema = requiredSchema.getField("embedding").get().schema().getNonNullType();
    assertEquals(HoodieSchemaType.VECTOR, embeddingSchema.getType());
    assertInstanceOf(HoodieSchema.Vector.class, embeddingSchema);
    assertEquals(128, ((HoodieSchema.Vector) embeddingSchema).getDimension());
    assertEquals(HoodieSchema.Vector.VectorElementType.DOUBLE,
        ((HoodieSchema.Vector) embeddingSchema).getVectorElementType());

    assertEquals(HoodieSchemaType.RECORD,
        requiredSchema.getField("payload").get().schema().getNonNullType().getType());
    assertEquals(HoodieSchemaType.STRING,
        requiredSchema.getField("missing").get().schema().getNonNullType().getType());
  }

  @Test
  void testTypePredicatesAndPrecision() {
    assertTrue(DataTypeUtils.isTimestampType(DataTypes.TIMESTAMP(3)));
    assertFalse(DataTypeUtils.isTimestampType(DataTypes.TIMESTAMP_LTZ(3)));
    assertTrue(DataTypeUtils.isDateType(DataTypes.DATE()));
    assertTrue(DataTypeUtils.isDatetimeType(DataTypes.DATE()));
    assertTrue(DataTypeUtils.isDatetimeType(DataTypes.TIMESTAMP(3)));
    assertFalse(DataTypeUtils.isDatetimeType(DataTypes.STRING()));
    assertEquals(3, DataTypeUtils.precision(DataTypes.TIMESTAMP(3).getLogicalType()));
    assertEquals(6, DataTypeUtils.precision(DataTypes.TIMESTAMP_LTZ(6).getLogicalType()));
    assertThrows(AssertionError.class,
        () -> DataTypeUtils.precision(DataTypes.STRING().getLogicalType()));
    assertTrue(DataTypeUtils.isFamily(
        DataTypes.INT().getLogicalType(), LogicalTypeFamily.NUMERIC));
  }

  @Test
  void testRowTypeProjectionUtilities() {
    Schema schema = Schema.newBuilder()
        .column("id", DataTypes.INT())
        .column("name", DataTypes.STRING())
        .columnByExpression("computed", "id + 1")
        .build();
    RowType rowType = DataTypeUtils.toRowType(schema);

    assertEquals(Arrays.asList("id", "name"), rowType.getFieldNames());
    RowType projected = (RowType) DataTypes.ROW(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("id", DataTypes.INT()))
        .getLogicalType();
    assertArrayEquals(new int[] {1, 0}, DataTypeUtils.projectOrdinals(rowType, projected));
    assertEquals(Arrays.asList("name", "id"), Arrays.asList(
        DataTypeUtils.projectRowFields(rowType, new String[] {"name", "id"})[0].getName(),
        DataTypeUtils.projectRowFields(rowType, new String[] {"name", "id"})[1].getName()));
  }

  @Test
  void testResolvePartitionValues() {
    assertEquals("value", DataTypeUtils.resolvePartition("value", DataTypes.STRING()));
    assertEquals(true, DataTypeUtils.resolvePartition("true", DataTypes.BOOLEAN()));
    assertEquals((byte) 1, DataTypeUtils.resolvePartition("1", DataTypes.TINYINT()));
    assertEquals((short) 2, DataTypeUtils.resolvePartition("2", DataTypes.SMALLINT()));
    assertEquals(3, DataTypeUtils.resolvePartition("3", DataTypes.INT()));
    assertEquals(4L, DataTypeUtils.resolvePartition("4", DataTypes.BIGINT()));
    assertEquals(1.5F, DataTypeUtils.resolvePartition("1.5", DataTypes.FLOAT()));
    assertEquals(2.5D, DataTypeUtils.resolvePartition("2.5", DataTypes.DOUBLE()));
    assertEquals(LocalDate.of(2026, 8, 6),
        DataTypeUtils.resolvePartition("2026-08-06", DataTypes.DATE()));
    assertEquals(LocalDateTime.of(2026, 8, 6, 12, 30),
        DataTypeUtils.resolvePartition("2026-08-06T12:30:00", DataTypes.TIMESTAMP()));
    assertEquals(new BigDecimal("12.30"),
        DataTypeUtils.resolvePartition("12.30", DataTypes.DECIMAL(10, 2)));
    assertNull(DataTypeUtils.resolvePartition(null, DataTypes.STRING()));
    assertThrows(RuntimeException.class,
        () -> DataTypeUtils.resolvePartition("00:00:00", DataTypes.TIME()));
  }

  @Test
  void testEnsureColumnsAsNonNullable() {
    DataType row = DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT()),
        DataTypes.FIELD("name", DataTypes.STRING().notNull()));

    assertSame(row, DataTypeUtils.ensureColumnsAsNonNullable(row, null));
    assertSame(row, DataTypeUtils.ensureColumnsAsNonNullable(row, Collections.emptyList()));
    assertSame(row, DataTypeUtils.ensureColumnsAsNonNullable(row, Collections.singletonList("name")));

    DataType converted = DataTypeUtils.ensureColumnsAsNonNullable(
        row, Collections.singletonList("id"));
    RowType convertedRowType = (RowType) converted.getLogicalType();
    assertFalse(convertedRowType.isNullable());
    assertFalse(convertedRowType.getTypeAt(0).isNullable());
    assertFalse(convertedRowType.getTypeAt(1).isNullable());
    assertThrows(RuntimeException.class,
        () -> DataTypeUtils.ensureColumnsAsNonNullable(
            DataTypes.STRING(), Collections.singletonList("id")));
  }

  @Test
  void testAddMetadataFields() {
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT()))
        .getLogicalType();

    RowType withoutOperation = DataTypeUtils.addMetadataFields(rowType, false);
    RowType withOperation = DataTypeUtils.addMetadataFields(rowType, true);

    assertEquals(6, withoutOperation.getFieldCount());
    assertEquals(7, withOperation.getFieldCount());
    assertEquals("_hoodie_commit_time", withOperation.getFieldNames().get(0));
    assertEquals("_hoodie_operation", withOperation.getFieldNames().get(5));
    assertEquals("id", withOperation.getFieldNames().get(6));
  }

  @Test
  void testGetMetadataColumnsValidation() {
    Schema validSchema = Schema.newBuilder()
        .column("id", DataTypes.INT())
        .columnByMetadata("_hoodie_commit_time", DataTypes.STRING(), true)
        .build();
    assertEquals(Collections.singletonList("_hoodie_commit_time"),
        DataTypeUtils.getMetadataColumns(validSchema));

    Schema invalidName = Schema.newBuilder()
        .columnByMetadata("unknown", DataTypes.STRING(), true)
        .build();
    assertThrows(HoodieCatalogException.class,
        () -> DataTypeUtils.getMetadataColumns(invalidName));

    Schema persisted = Schema.newBuilder()
        .columnByMetadata("_hoodie_commit_time", DataTypes.STRING(), false)
        .build();
    assertThrows(HoodieCatalogException.class,
        () -> DataTypeUtils.getMetadataColumns(persisted));

    Schema withKey = Schema.newBuilder()
        .columnByMetadata("_hoodie_commit_time", DataTypes.STRING(), "metadata-key", true)
        .build();
    assertThrows(HoodieCatalogException.class,
        () -> DataTypeUtils.getMetadataColumns(withKey));
  }
}
