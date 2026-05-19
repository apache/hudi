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

package org.apache.hudi.internal.schema.convert;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createArrayField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createMapField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createNestedField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createNullablePrimitiveField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createNullableRecord;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createPrimitiveField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestInternalSchemaConverter {

  public static HoodieSchema getSimpleSchema() {
    return createRecord("simpleSchema",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        createPrimitiveField("field2", HoodieSchemaType.STRING));
  }

  public static List<String> getSimpleSchemaExpectedColumnNames() {
    return Arrays.asList("field1", "field2");
  }

  public static HoodieSchema getSimpleSchemaWithNullable() {
    return createRecord("simpleSchemaWithNullable",
        createNullablePrimitiveField("field1", HoodieSchemaType.INT),
        createPrimitiveField("field2", HoodieSchemaType.STRING));
  }

  public static HoodieSchema getComplexSchemaSingleLevel() {
    return createRecord("complexSchemaSingleLevel",
        createNestedField("field1", HoodieSchemaType.INT),
        createArrayField("field2", HoodieSchemaType.STRING),
        createMapField("field3", HoodieSchemaType.DOUBLE));
  }

  public static List<String> getComplexSchemaSingleLevelExpectedColumnNames() {
    return Arrays.asList("field1.nested", "field2.element", "field3.key", "field3.value");
  }

  public static HoodieSchema getDeeplyNestedFieldSchema() {
    return createRecord("deeplyNestedFieldSchema",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        HoodieSchemaField.of("field2",
            createRecord("field2nest",
                createArrayField("field2nestarray",
                    createNullableRecord("field2nestarraynest",
                        createNullablePrimitiveField("field21", HoodieSchemaType.INT),
                        createNullablePrimitiveField("field22", HoodieSchemaType.INT)))), null, null),
        createNullablePrimitiveField("field3", HoodieSchemaType.INT));
  }

  public static List<String> getDeeplyNestedFieldSchemaExpectedColumnNames() {
    return Arrays.asList("field1", "field2.field2nestarray.element.field21",
        "field2.field2nestarray.element.field22", "field3");
  }

  @Test
  public void testVariantConvertToInternalType() {
    // Create a schema with a variant field
    HoodieSchema variantSchema = createRecord("variantRecord",
        createPrimitiveField("id", HoodieSchemaType.INT),
        HoodieSchemaField.of("data", HoodieSchema.createVariant(), null, null));

    // Convert HoodieSchema -> InternalSchema, exercising the VARIANT case in visitPrimitiveToBuildInternalType
    Type internalType = InternalSchemaConverter.convertToField(variantSchema);
    assertInstanceOf(Types.RecordType.class, internalType);
    Types.RecordType recordType = (Types.RecordType) internalType;

    // The record should have 2 fields: id and data
    assertEquals(2, recordType.fields().size());
    Types.Field dataField = recordType.fields().get(1);
    assertEquals("data", dataField.name());

    // The variant field should be represented as a RecordType with sentinel negative IDs
    assertInstanceOf(Types.RecordType.class, dataField.type());
    Types.RecordType variantRecordType = (Types.RecordType) dataField.type();
    List<Types.Field> variantFields = variantRecordType.fields();
    assertEquals(2, variantFields.size());
    assertEquals(HoodieSchema.Variant.VARIANT_METADATA_FIELD, variantFields.get(0).name());
    assertEquals(HoodieSchema.Variant.VARIANT_VALUE_FIELD, variantFields.get(1).name());
    assertEquals(InternalSchemaConverter.VARIANT_METADATA_FIELD_ID, variantFields.get(0).fieldId());
    assertEquals(InternalSchemaConverter.VARIANT_VALUE_FIELD_ID, variantFields.get(1).fieldId());
    assertFalse(variantFields.get(0).isOptional());
    assertFalse(variantFields.get(1).isOptional());
    assertEquals(Type.TypeID.BINARY, variantFields.get(0).type().typeId());
    assertEquals(Type.TypeID.BINARY, variantFields.get(1).type().typeId());

    // Round-trip: convert back to HoodieSchema and verify it's a Variant
    HoodieSchema roundTripped = InternalSchemaConverter.convert(recordType, "variantRecord");
    HoodieSchemaField roundTrippedDataField = roundTripped.getField("data").get();
    assertEquals(HoodieSchemaType.VARIANT, roundTrippedDataField.schema().getType());
    assertInstanceOf(HoodieSchema.Variant.class, roundTrippedDataField.schema());
  }

  @Test
  public void testVariantDetectionValueFirstOrder() {
    // value first, metadata second — covers lines 464-465 and line 471
    List<Types.Field> variantFields = Arrays.asList(
        Types.Field.get(InternalSchemaConverter.VARIANT_VALUE_FIELD_ID, false,
            HoodieSchema.Variant.VARIANT_VALUE_FIELD, Types.BinaryType.get(), null),
        Types.Field.get(InternalSchemaConverter.VARIANT_METADATA_FIELD_ID, false,
            HoodieSchema.Variant.VARIANT_METADATA_FIELD, Types.BinaryType.get(), null));
    Types.RecordType variantRecord = Types.RecordType.get(variantFields);

    HoodieSchema result = InternalSchemaConverter.convert(variantRecord, "test");
    assertInstanceOf(HoodieSchema.Variant.class, result);
  }

  @Test
  public void testVariantDetectionMetadataFirstOrder() {
    // metadata first, value second — covers lines 466-467
    List<Types.Field> variantFields = Arrays.asList(
        Types.Field.get(InternalSchemaConverter.VARIANT_METADATA_FIELD_ID, false,
            HoodieSchema.Variant.VARIANT_METADATA_FIELD, Types.BinaryType.get(), null),
        Types.Field.get(InternalSchemaConverter.VARIANT_VALUE_FIELD_ID, false,
            HoodieSchema.Variant.VARIANT_VALUE_FIELD, Types.BinaryType.get(), null));
    Types.RecordType variantRecord = Types.RecordType.get(variantFields);

    HoodieSchema result = InternalSchemaConverter.convert(variantRecord, "test");
    assertInstanceOf(HoodieSchema.Variant.class, result);
  }

  @Test
  public void testVariantDetectionNegativeIdsWrongNames() {
    // Negative IDs but wrong field names — hasVariantFields is false, should produce a regular record
    List<Types.Field> fields = Arrays.asList(
        Types.Field.get(-1, false, "foo", Types.BinaryType.get(), null),
        Types.Field.get(-2, false, "bar", Types.BinaryType.get(), null));
    Types.RecordType record = Types.RecordType.get(fields);

    HoodieSchema result = InternalSchemaConverter.convert(record, "test");
    assertFalse(result instanceof HoodieSchema.Variant);
    assertEquals(HoodieSchemaType.RECORD, result.getType());
  }

  @Test
  public void testVariantDetectionCorrectNamesPositiveIds() {
    // Correct variant field names but positive IDs — hasNegativeIds is false, should produce a regular record
    List<Types.Field> fields = Arrays.asList(
        Types.Field.get(0, false,
            HoodieSchema.Variant.VARIANT_VALUE_FIELD, Types.BinaryType.get(), null),
        Types.Field.get(1, false,
            HoodieSchema.Variant.VARIANT_METADATA_FIELD, Types.BinaryType.get(), null));
    Types.RecordType record = Types.RecordType.get(fields);

    HoodieSchema result = InternalSchemaConverter.convert(record, "test");
    assertFalse(result instanceof HoodieSchema.Variant);
    assertEquals(HoodieSchemaType.RECORD, result.getType());
  }

  @Test
  public void testCollectColumnNames() {
    HoodieSchema simpleSchema =  getSimpleSchema();
    List<String> fieldNames =  InternalSchemaConverter.collectColNamesFromSchema(simpleSchema);
    List<String> expectedOutput = getSimpleSchemaExpectedColumnNames();
    assertEquals(expectedOutput.size(), fieldNames.size());
    assertTrue(fieldNames.containsAll(expectedOutput));


    HoodieSchema simpleSchemaWithNullable = getSimpleSchemaWithNullable();
    fieldNames =  InternalSchemaConverter.collectColNamesFromSchema(simpleSchemaWithNullable);
    expectedOutput = getSimpleSchemaExpectedColumnNames();
    assertEquals(expectedOutput.size(), fieldNames.size());
    assertTrue(fieldNames.containsAll(expectedOutput));

    HoodieSchema complexSchemaSingleLevel = getComplexSchemaSingleLevel();
    fieldNames =  InternalSchemaConverter.collectColNamesFromSchema(complexSchemaSingleLevel);
    expectedOutput = getComplexSchemaSingleLevelExpectedColumnNames();
    assertEquals(expectedOutput.size(), fieldNames.size());
    assertTrue(fieldNames.containsAll(expectedOutput));

    HoodieSchema deeplyNestedFieldSchema = getDeeplyNestedFieldSchema();
    fieldNames =  InternalSchemaConverter.collectColNamesFromSchema(deeplyNestedFieldSchema);
    expectedOutput = getDeeplyNestedFieldSchemaExpectedColumnNames();
    assertEquals(expectedOutput.size(), fieldNames.size());
    assertTrue(fieldNames.containsAll(expectedOutput));
  }

  @Test
  public void testVectorTypeRoundTrip() {
    // Create a HoodieSchema with a VECTOR field
    HoodieSchema vectorField = HoodieSchema.createVector(128, HoodieSchema.Vector.VectorElementType.FLOAT);
    HoodieSchema recordSchema = HoodieSchema.createRecord("vectorRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("embedding", vectorField, null, null)
    ));

    // HoodieSchema → InternalSchema
    InternalSchema internalSchema = InternalSchemaConverter.convert(recordSchema);

    // Verify the InternalSchema has a VectorType
    Types.VectorType internalVectorType = (Types.VectorType) internalSchema.findType("embedding");
    assertEquals(128, internalVectorType.getDimension());
    assertEquals("FLOAT", internalVectorType.getElementType());
    assertEquals("FIXED_BYTES", internalVectorType.getStorageBacking());

    // InternalSchema → HoodieSchema
    HoodieSchema roundTripped = InternalSchemaConverter.convert(internalSchema, "vectorRecord");

    // Verify vector properties survived the round-trip
    HoodieSchemaField embeddingField = roundTripped.getFields().stream()
        .filter(f -> f.name().equals("embedding"))
        .findFirst().get();
    HoodieSchema embeddingSchema = embeddingField.schema().getNonNullType();
    assertEquals(HoodieSchemaType.VECTOR, embeddingSchema.getType());
    HoodieSchema.Vector roundTrippedVector = (HoodieSchema.Vector) embeddingSchema;
    assertEquals(128, roundTrippedVector.getDimension());
    assertEquals(HoodieSchema.Vector.VectorElementType.FLOAT, roundTrippedVector.getVectorElementType());
    assertEquals(HoodieSchema.Vector.StorageBacking.FIXED_BYTES, roundTrippedVector.getStorageBacking());
  }

  @Test
  public void testVectorTypeRoundTripDouble() {
    HoodieSchema vectorField = HoodieSchema.createVector(64, HoodieSchema.Vector.VectorElementType.DOUBLE);
    HoodieSchema recordSchema = HoodieSchema.createRecord("vectorDoubleRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("vec", vectorField, null, null)
    ));

    InternalSchema internalSchema = InternalSchemaConverter.convert(recordSchema);
    Types.VectorType internalVec = (Types.VectorType) internalSchema.findType("vec");
    assertEquals(64, internalVec.getDimension());
    assertEquals("DOUBLE", internalVec.getElementType());

    HoodieSchema roundTripped = InternalSchemaConverter.convert(internalSchema, "vectorDoubleRecord");
    HoodieSchemaField vecField = roundTripped.getFields().stream()
        .filter(f -> f.name().equals("vec"))
        .findFirst().get();
    HoodieSchema.Vector rtVec = (HoodieSchema.Vector) vecField.schema().getNonNullType();
    assertEquals(64, rtVec.getDimension());
    assertEquals(HoodieSchema.Vector.VectorElementType.DOUBLE, rtVec.getVectorElementType());
    assertEquals(HoodieSchema.Vector.StorageBacking.FIXED_BYTES, rtVec.getStorageBacking());
  }
}
