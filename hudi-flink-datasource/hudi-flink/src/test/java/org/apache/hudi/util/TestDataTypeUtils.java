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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Tests for {@link DataTypeUtils}.
 */
public class TestDataTypeUtils {

  @Test
  public void testCreateRequiredSchemaUsesTableSchemaOnlyForHudiLogicalTypes() {
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

    HoodieSchema requiredSchema = DataTypeUtils.createRequiredSchema(tableSchema, requiredRowType);

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

    assertEquals(HoodieSchemaType.VARIANT,
        requiredSchema.getField("payload").get().schema().getNonNullType().getType());
    assertEquals(HoodieSchemaType.STRING,
        requiredSchema.getField("missing").get().schema().getNonNullType().getType());
  }
}
