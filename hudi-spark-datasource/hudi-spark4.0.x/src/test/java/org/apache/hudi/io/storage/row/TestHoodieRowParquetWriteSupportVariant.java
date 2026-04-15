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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Variant type support in {@link HoodieRowParquetWriteSupport}.
 * Verifies that Spark VariantType data is correctly written to Parquet groups with 'metadata' and 'value' binary fields,
 * respecting shredded vs unshredded schemas.
 */
public class TestHoodieRowParquetWriteSupportVariant {

  @TempDir
  public File tempDir;

  /**
   * Tests that an Unshredded Variant (defined by HoodieSchema) is written as:
   * <pre>
   * group {
   * required binary metadata;
   * required binary value;
   * }
   * </pre>
   */
  @Test
  public void testWriteUnshreddedVariant() throws IOException {

    // Setup Hoodie Schema: Unshredded
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariant("v", null, "Unshredded variant");
    HoodieSchema recordSchema = HoodieSchema.createRecord("record", null, null,
        Collections.singletonList(HoodieSchemaField.of("v", variantSchema, null, null)));

    // Spark Schema: Uses actual VariantType to trigger the correct path in WriteSupport
    StructType sparkSchema = new StructType(new StructField[] {
        new StructField("v", DataTypes.VariantType, false, Metadata.empty())
    });

    // Setup Data
    // Note: This relies on the test environment having a constructible VariantVal
    VariantVal variantVal = getSampleVariantVal();
    InternalRow row = new GenericInternalRow(new Object[] {variantVal});

    // Write to Parquet
    File outputFile = new File(tempDir, "unshredded.parquet");
    writeRows(outputFile, sparkSchema, recordSchema, row);

    // Verify Parquet Structure
    MessageType parquetSchema = readParquetSchema(outputFile);
    GroupType vGroup = parquetSchema.getType("v").asGroupType();

    assertEquals(2, vGroup.getFieldCount(), "Unshredded variant should have exactly 2 fields");

    // Metadata (Required)
    Type metaField = vGroup.getType("metadata");
    assertEquals(BINARY, metaField.asPrimitiveType().getPrimitiveTypeName());
    assertEquals(REQUIRED, metaField.getRepetition());

    // Value (Required for Unshredded)
    Type valueField = vGroup.getType("value");
    assertEquals(BINARY, valueField.asPrimitiveType().getPrimitiveTypeName());
    assertEquals(REQUIRED, valueField.getRepetition(), "Unshredded variant value must be REQUIRED");
  }

  /**
   * Tests that a Shredded Variant (defined by HoodieSchema) is written as:
   * <pre>
   *   group {
   *     required binary metadata;
   *     optional binary value;
   *   }
   * </pre>
   * <p>
   * The schema that we are using here is as such:
   * <pre>
   *   root
   *    |-- v: struct (nullable = true)
   *    |    |-- metadata: binary (nullable = true)
   *    |    |-- value: binary (nullable = true)
   *    |    |-- typed_value: struct (nullable = true)
   *    |    |    |-- a: struct (nullable = true)
   *    |    |    |    |-- value: binary (nullable = true)
   *    |    |    |    |-- typed_value: integer (nullable = true)
   *    |    |    |-- b: struct (nullable = true)
   *    |    |    |    |-- value: binary (nullable = true)
   *    |    |    |    |-- typed_value: string (nullable = true)
   *    |    |    |-- c: struct (nullable = true)
   *    |    |    |    |-- value: binary (nullable = true)
   *    |    |    |    |-- typed_value: decimal(15,1) (nullable = true)
   * </pre>
   * Note: Even though typed_value is not populated, the schema must indicate 'value' is OPTIONAL.
   */
  @Test
  public void testWriteShreddedVariant() throws IOException {
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariantShreddedObject("v", null, "Shredded variant",
        // These does not need to be nullable, #createVariantShreddedObject will handle the nullable coercion
        Map.of("a", HoodieSchema.create(HoodieSchemaType.INT),
            "b", HoodieSchema.create(HoodieSchemaType.STRING),
            "c", HoodieSchema.createDecimal(15, 1)));
    HoodieSchema recordSchema = HoodieSchema.createRecord("record", null, null,
        Collections.singletonList(HoodieSchemaField.of("v", variantSchema, null, null)));

    // Spark Schema: Uses actual VariantType
    StructType sparkSchema = new StructType(new StructField[] {
        new StructField("v", DataTypes.VariantType, false, Metadata.empty())
    });

    // Setup Data
    VariantVal variantVal = getSampleVariantVal();
    InternalRow row = new GenericInternalRow(new Object[] {variantVal});

    // Write to Parquet
    File outputFile = new File(tempDir, "shredded.parquet");
    writeRows(outputFile, sparkSchema, recordSchema, row);

    // Verify Parquet Structure
    MessageType parquetSchema = readParquetSchema(outputFile);
    GroupType vGroup = parquetSchema.getType("v").asGroupType();

    // Metadata (Required)
    Type metaField = vGroup.getType("metadata");
    assertEquals(BINARY, metaField.asPrimitiveType().getPrimitiveTypeName());
    assertEquals(REQUIRED, metaField.getRepetition());

    // Value (OPTIONAL for Shredded)
    Type valueField = vGroup.getType("value");
    assertEquals(BINARY, valueField.asPrimitiveType().getPrimitiveTypeName());
    assertEquals(OPTIONAL, valueField.getRepetition(), "Shredded variant value must be OPTIONAL");

    boolean hasTypedValue = vGroup.getFields().stream().anyMatch(f -> f.getName().equals("typed_value"));
    assertTrue(hasTypedValue, "typed_value field should be present in shredded variant");

    // Check parquet metadata in the footer that shredded schemas are created
    ParquetMetadata fileFooter = ParquetFileReader.readFooter(new Configuration(), new Path(outputFile.toURI()), ParquetMetadataConverter.NO_FILTER);
    MessageType footerSchema = fileFooter.getFileMetaData().getSchema();
    assertEquals(INT32, footerSchema.getType("v", "typed_value", "a", "typed_value").asPrimitiveType().getPrimitiveTypeName());
    assertEquals(LogicalTypeAnnotation.stringType(), footerSchema.getType("v", "typed_value", "b", "typed_value").getLogicalTypeAnnotation());
    assertEquals(LogicalTypeAnnotation.decimalType(1, 15), footerSchema.getType("v", "typed_value", "c", "typed_value").getLogicalTypeAnnotation());
  }

  /**
   * Tests that a root-level scalar shredded Variant is written as:
   * <pre>
   *   group {
   *     required binary metadata;
   *     optional binary value;
   *     optional int64 typed_value;
   *   }
   * </pre>
   * This verifies that scalar typed_value (e.g. LONG) is correctly shredded at the root level,
   * as opposed to object shredding where typed_value is a nested struct.
   */
  @Test
  public void testWriteShreddedVariantRootLevelScalar() throws IOException {
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariantShredded("v", null, "Scalar shredded variant",
        HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema recordSchema = HoodieSchema.createRecord("record", null, null,
        Collections.singletonList(HoodieSchemaField.of("v", variantSchema, null, null)));

    // Spark Schema: Uses actual VariantType
    StructType sparkSchema = new StructType(new StructField[] {
        new StructField("v", DataTypes.VariantType, false, Metadata.empty())
    });

    // Setup Data: integer variant value 100
    VariantVal variantVal = getSampleVariantValWithRootLevelScalar();
    InternalRow row = new GenericInternalRow(new Object[] {variantVal});

    // Write to Parquet
    File outputFile = new File(tempDir, "shredded_scalar.parquet");
    writeRows(outputFile, sparkSchema, recordSchema, row);

    // Verify Parquet Structure
    ParquetMetadata fileFooter = ParquetFileReader.readFooter(new Configuration(), new Path(outputFile.toURI()), ParquetMetadataConverter.NO_FILTER);
    MessageType footerSchema = fileFooter.getFileMetaData().getSchema();
    GroupType vGroup = footerSchema.getType("v").asGroupType();

    // Should have 3 fields: metadata, value, typed_value
    assertEquals(3, vGroup.getFieldCount(), "Scalar shredded variant should have 3 fields");

    // Metadata (Required)
    Type metaField = vGroup.getType("metadata");
    assertEquals(BINARY, metaField.asPrimitiveType().getPrimitiveTypeName());
    assertEquals(REQUIRED, metaField.getRepetition());

    // Value (OPTIONAL for Shredded)
    Type valueField = vGroup.getType("value");
    assertEquals(BINARY, valueField.asPrimitiveType().getPrimitiveTypeName());
    assertEquals(OPTIONAL, valueField.getRepetition(), "Shredded variant value must be OPTIONAL");

    // typed_value (OPTIONAL, INT64 for LONG)
    Type typedValueField = vGroup.getType("typed_value");
    assertEquals(INT64, typedValueField.asPrimitiveType().getPrimitiveTypeName());
    assertEquals(OPTIONAL, typedValueField.getRepetition(), "Scalar typed_value must be OPTIONAL");
  }

  private void writeRows(File outputFile, StructType sparkSchema, HoodieSchema recordSchema, InternalRow row) throws IOException {
    Configuration conf = new Configuration();
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.AVRO_SCHEMA_STRING.key(), recordSchema.toString());

    // Create config and write support
    HoodieConfig hoodieConfig = new HoodieConfig(props);
    HoodieRowParquetWriteSupport writeSupport = new HoodieRowParquetWriteSupport(
        conf, sparkSchema, Option.empty(), hoodieConfig);

    try (ParquetWriter<InternalRow> writer = new ParquetWriter<>(new Path(outputFile.getAbsolutePath()), writeSupport)) {
      writer.write(row);
    }
  }

  private MessageType readParquetSchema(File file) throws IOException {
    Configuration conf = new Configuration();
    ParquetMetadata metadata = ParquetFileReader.readFooter(conf, new Path(file.getAbsolutePath()));
    return metadata.getFileMetaData().getSchema();
  }

  /**
   * Tests that a Variant nested inside a struct field is correctly shredded.
   * <pre>
   * record {
   *   id: int,
   *   nested: struct {
   *     variant_field: variant (shredded)
   *   }
   * }
   * </pre>
   */
  @Test
  public void testWriteNestedVariantInStruct() throws IOException {
    // Setup: Create a shredded variant schema
    LinkedHashMap<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.INT));
    shreddedFields.put("b", HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariantShreddedObject(shreddedFields);

    // Create a nested struct containing the variant
    HoodieSchema nestedStructSchema = HoodieSchema.createRecord("NestedStruct", null, null,
        Collections.singletonList(HoodieSchemaField.of("variant_field", variantSchema, null, null)));

    // Create the top-level record schema
    HoodieSchema recordSchema = HoodieSchema.createRecord("record", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("nested", nestedStructSchema, null, null)
    ));

    // Create Spark schema
    StructType nestedSparkStruct = new StructType(new StructField[] {
        new StructField("variant_field", DataTypes.VariantType, false, Metadata.empty())
    });
    StructType sparkSchema = new StructType(new StructField[] {
        new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("nested", nestedSparkStruct, false, Metadata.empty())
    });

    // Write data
    File outputFile = new File(tempDir, "nested_variant_struct.parquet");

    // Create nested struct with variant
    GenericInternalRow nestedRow = new GenericInternalRow(1);
    nestedRow.update(0, getSampleVariantVal());

    // Create top-level row
    GenericInternalRow row = new GenericInternalRow(2);
    row.update(0, 123);
    row.update(1, nestedRow);

    writeRows(outputFile, sparkSchema, recordSchema, row);

    // Verify the Parquet schema has shredded structure in the nested field
    MessageType schema = readParquetSchema(outputFile);

    // Verify nested.variant_field has shredded structure
    GroupType nestedGroup = schema.getType("nested").asGroupType();
    GroupType variantGroup = nestedGroup.getType("variant_field").asGroupType();

    assertNotNull(variantGroup.getType("typed_value"), "Nested variant should have typed_value field (shredded)");
    assertEquals(Type.Repetition.OPTIONAL, variantGroup.getType("value").getRepetition(),
        "Shredded nested variant value should be OPTIONAL");
  }

  /**
   * Tests that a Variant nested deeply (struct -> struct -> variant) is correctly shredded.
   */
  @Test
  public void testWriteDeeplyNestedVariant() throws IOException {
    // Setup: Create shredded variant
    LinkedHashMap<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("x", HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariantShreddedObject(shreddedFields);

    // Create deeply nested structure: level2 -> level1 -> variant
    HoodieSchema level2Schema = HoodieSchema.createRecord("Level2", null, null,
        Collections.singletonList(HoodieSchemaField.of("variant_field", variantSchema, null, null)));

    HoodieSchema level1Schema = HoodieSchema.createRecord("Level1", null, null,
        Collections.singletonList(HoodieSchemaField.of("level2", level2Schema, null, null)));

    HoodieSchema recordSchema = HoodieSchema.createRecord("record", null, null,
        Collections.singletonList(HoodieSchemaField.of("level1", level1Schema, null, null)));

    // Create corresponding Spark schema
    StructType level2Spark = new StructType(new StructField[] {
        new StructField("variant_field", DataTypes.VariantType, false, Metadata.empty())
    });
    StructType level1Spark = new StructType(new StructField[] {
        new StructField("level2", level2Spark, false, Metadata.empty())
    });
    StructType sparkSchema = new StructType(new StructField[] {
        new StructField("level1", level1Spark, false, Metadata.empty())
    });

    // Write data
    File outputFile = new File(tempDir, "deeply_nested_variant.parquet");

    // Create level2 with variant
    GenericInternalRow level2Row = new GenericInternalRow(1);
    level2Row.update(0, getSampleVariantVal());

    // Create level1 with level2
    GenericInternalRow level1Row = new GenericInternalRow(1);
    level1Row.update(0, level2Row);

    // Create top-level row
    GenericInternalRow row = new GenericInternalRow(1);
    row.update(0, level1Row);

    writeRows(outputFile, sparkSchema, recordSchema, row);

    // Verify shredding at depth
    MessageType schema = readParquetSchema(outputFile);

    GroupType level1 = schema.getType("level1").asGroupType();
    GroupType level2 = level1.getType("level2").asGroupType();
    GroupType variantGroup = level2.getType("variant_field").asGroupType();

    assertNotNull(variantGroup.getType("typed_value"),
        "Deeply nested variant should have typed_value field (shredded)");
  }

  /**
   * Tests that Variants in array elements are correctly handled.
   * Note: Arrays of Variants themselves can contain shredded or unshredded variants.
   */
  @Test
  public void testWriteVariantInArray() throws IOException {
    // Setup: Array of shredded variants
    LinkedHashMap<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("item", HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariantShreddedObject(shreddedFields);

    HoodieSchema arraySchema = HoodieSchema.createArray(variantSchema);
    HoodieSchema recordSchema = HoodieSchema.createRecord("record", null, null,
        Collections.singletonList(HoodieSchemaField.of("items", arraySchema, null, null)));

    // Spark schema
    StructType sparkSchema = new StructType(new StructField[] {
        new StructField("items", DataTypes.createArrayType(DataTypes.VariantType, false),
            false, Metadata.empty())
    });

    // Write data
    File outputFile = new File(tempDir, "variant_array.parquet");

    VariantVal[] variants = new VariantVal[] {getSampleVariantVal(), getSampleVariantVal()};
    GenericInternalRow row = new GenericInternalRow(1);
    row.update(0, new org.apache.spark.sql.catalyst.util.GenericArrayData(variants));

    writeRows(outputFile, sparkSchema, recordSchema, row);

    // Verify array element has shredded structure
    MessageType schema = readParquetSchema(outputFile);

    // Navigate Parquet LIST structure: items -> list -> element
    GroupType itemsGroup = schema.getType("items").asGroupType();
    GroupType listGroup = itemsGroup.getType("list").asGroupType();
    GroupType elementGroup = listGroup.getType("element").asGroupType();

    assertNotNull(elementGroup.getType("typed_value"),
        "Array element variant should have typed_value field (shredded)");
  }

  /**
   * Tests mixed scenario: struct containing array of structs, each with a Variant field.
   */
  @Test
  public void testWriteMixedNestedVariant() throws IOException {
    // Setup: Variant in struct, inside array, inside struct
    LinkedHashMap<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("data", HoodieSchema.create(HoodieSchemaType.INT));
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariantShreddedObject(shreddedFields);

    // Inner struct with variant
    HoodieSchema innerStructSchema = HoodieSchema.createRecord("InnerStruct", null, null,
        Collections.singletonList(HoodieSchemaField.of("v", variantSchema, null, null)));

    // Array of inner structs
    HoodieSchema arraySchema = HoodieSchema.createArray(innerStructSchema);

    // Outer struct with array
    HoodieSchema outerStructSchema = HoodieSchema.createRecord("OuterStruct", null, null,
        Collections.singletonList(HoodieSchemaField.of("items", arraySchema, null, null)));

    HoodieSchema recordSchema = HoodieSchema.createRecord("record", null, null,
        Collections.singletonList(HoodieSchemaField.of("outer", outerStructSchema, null, null)));

    // Spark schema
    StructType innerSparkStruct = new StructType(new StructField[] {
        new StructField("v", DataTypes.VariantType, false, Metadata.empty())
    });
    StructType outerSparkStruct = new StructType(new StructField[] {
        new StructField("items", DataTypes.createArrayType(innerSparkStruct, false),
            false, Metadata.empty())
    });
    StructType sparkSchema = new StructType(new StructField[] {
        new StructField("outer", outerSparkStruct, false, Metadata.empty())
    });

    // Write data
    File outputFile = new File(tempDir, "mixed_nested_variant.parquet");

    // Create inner struct with variant
    GenericInternalRow innerRow = new GenericInternalRow(1);
    innerRow.update(0, getSampleVariantVal());

    // Create array of inner structs
    GenericInternalRow[] array = new GenericInternalRow[] {innerRow};

    // Create outer struct with array
    GenericInternalRow outerRow = new GenericInternalRow(1);
    outerRow.update(0, new org.apache.spark.sql.catalyst.util.GenericArrayData(array));

    // Create top-level row
    GenericInternalRow row = new GenericInternalRow(1);
    row.update(0, outerRow);

    writeRows(outputFile, sparkSchema, recordSchema, row);

    // Verify complex nested shredding
    MessageType schema = readParquetSchema(outputFile);

    // Navigate: outer -> items (LIST) -> list -> element (struct) -> v (variant)
    GroupType outer = schema.getType("outer").asGroupType();
    GroupType items = outer.getType("items").asGroupType();
    GroupType list = items.getType("list").asGroupType();
    GroupType element = list.getType("element").asGroupType();
    GroupType variantGroup = element.getType("v").asGroupType();

    assertNotNull(variantGroup.getType("typed_value"),
        "Variant in complex nested structure should have typed_value field (shredded)");
  }

  /**
   * Tests that a shredded Variant inside a nullable map value is correctly handled.
   * This verifies that nullable unions in HoodieSchema are properly unwrapped via getNonNullType()
   * when processing map value schemas.
   */
  @Test
  public void testWriteVariantInNullableMapValue() throws IOException {
    // Setup: Map with nullable shredded variant values
    LinkedHashMap<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("score", HoodieSchema.create(HoodieSchemaType.INT));
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariantShreddedObject(shreddedFields);

    // Wrap the variant in a nullable union, as would occur in real schemas
    HoodieSchema nullableVariantSchema = HoodieSchema.createNullable(variantSchema);
    HoodieSchema mapSchema = HoodieSchema.createMap(nullableVariantSchema);
    HoodieSchema recordSchema = HoodieSchema.createRecord("record", null, null,
        Collections.singletonList(HoodieSchemaField.of("data", mapSchema, null, null)));

    // Spark schema: Map<String, Variant>
    StructType sparkSchema = new StructType(new StructField[] {
        new StructField("data", DataTypes.createMapType(DataTypes.StringType, DataTypes.VariantType, true),
            false, Metadata.empty())
    });

    // Write data
    File outputFile = new File(tempDir, "variant_nullable_map.parquet");

    ArrayData keyArray = new GenericArrayData(new Object[] {UTF8String.fromString("key1")});
    ArrayData valueArray = new GenericArrayData(new Object[] {getSampleVariantVal()});
    ArrayBasedMapData sparkMap = new ArrayBasedMapData(keyArray, valueArray);

    GenericInternalRow row = new GenericInternalRow(new Object[] {sparkMap});
    writeRows(outputFile, sparkSchema, recordSchema, row);

    // Verify map value has shredded structure
    MessageType schema = readParquetSchema(outputFile);

    // Navigate Parquet MAP structure: data -> key_value -> value (variant group)
    GroupType dataGroup = schema.getType("data").asGroupType();
    GroupType keyValueGroup = dataGroup.getType("key_value").asGroupType();
    GroupType valueGroup = keyValueGroup.getType("value").asGroupType();

    assertNotNull(valueGroup.getType("typed_value"),
        "Map value variant with nullable schema should have typed_value field (shredded)");
  }

  /**
   * Tests that a shredded Variant inside a nullable array element is correctly handled.
   * This verifies that nullable unions in HoodieSchema are properly unwrapped via getNonNullType()
   * when processing array element schemas.
   */
  @Test
  public void testWriteVariantInNullableArrayElement() throws IOException {
    // Setup: Array with nullable shredded variant elements
    LinkedHashMap<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("tag", HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariantShreddedObject(shreddedFields);

    // Wrap the variant in a nullable union
    HoodieSchema nullableVariantSchema = HoodieSchema.createNullable(variantSchema);
    HoodieSchema arraySchema = HoodieSchema.createArray(nullableVariantSchema);
    HoodieSchema recordSchema = HoodieSchema.createRecord("record", null, null,
        Collections.singletonList(HoodieSchemaField.of("items", arraySchema, null, null)));

    // Spark schema: Array<Variant> (nullable elements)
    StructType sparkSchema = new StructType(new StructField[] {
        new StructField("items", DataTypes.createArrayType(DataTypes.VariantType, true),
            false, Metadata.empty())
    });

    // Write data
    File outputFile = new File(tempDir, "variant_nullable_array.parquet");

    VariantVal[] variants = new VariantVal[] {getSampleVariantVal(), getSampleVariantVal()};
    GenericInternalRow row = new GenericInternalRow(1);
    row.update(0, new org.apache.spark.sql.catalyst.util.GenericArrayData(variants));

    writeRows(outputFile, sparkSchema, recordSchema, row);

    // Verify array element has shredded structure
    MessageType schema = readParquetSchema(outputFile);

    // Navigate Parquet LIST structure: items -> list -> element
    GroupType itemsGroup = schema.getType("items").asGroupType();
    GroupType listGroup = itemsGroup.getType("list").asGroupType();
    GroupType elementGroup = listGroup.getType("element").asGroupType();

    assertNotNull(elementGroup.getType("typed_value"),
        "Array element variant with nullable schema should have typed_value field (shredded)");
  }

  // ==================== Negative / Failure Mode Tests ====================

  /**
   * Tests that a null Variant value is correctly handled (skipped during write).
   * The Parquet file should be written successfully with the null field omitted.
   */
  @Test
  public void testWriteNullVariantValue() throws IOException {
    // Setup: Unshredded variant
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariant("v", null, "Nullable variant");
    HoodieSchema recordSchema = HoodieSchema.createRecord("record", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("v", variantSchema, null, null)
    ));

    // Spark schema: nullable variant
    StructType sparkSchema = new StructType(new StructField[] {
        new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("v", DataTypes.VariantType, true, Metadata.empty())
    });

    // Row with null variant
    GenericInternalRow row = new GenericInternalRow(new Object[] {1, null});

    File outputFile = new File(tempDir, "null_variant.parquet");
    writeRows(outputFile, sparkSchema, recordSchema, row);

    // Verify file was written and schema is correct
    MessageType parquetSchema = readParquetSchema(outputFile);
    GroupType vGroup = parquetSchema.getType("v").asGroupType();
    assertEquals(OPTIONAL, vGroup.getRepetition(), "Nullable variant should be OPTIONAL");
    assertEquals(2, vGroup.getFieldCount(), "Unshredded variant should have 2 fields");
  }

  /**
   * Tests that a null shredded Variant value is correctly handled.
   */
  @Test
  public void testWriteNullShreddedVariantValue() throws IOException {
    // Setup: Shredded variant
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariantShredded("v", null, "Nullable shredded variant",
        HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema recordSchema = HoodieSchema.createRecord("record", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("v", variantSchema, null, null)
    ));

    // Spark schema: nullable variant
    StructType sparkSchema = new StructType(new StructField[] {
        new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("v", DataTypes.VariantType, true, Metadata.empty())
    });

    // Row with null variant
    GenericInternalRow row = new GenericInternalRow(new Object[] {1, null});

    File outputFile = new File(tempDir, "null_shredded_variant.parquet");
    writeRows(outputFile, sparkSchema, recordSchema, row);

    // Verify file was written and schema has shredded structure
    MessageType parquetSchema = readParquetSchema(outputFile);
    GroupType vGroup = parquetSchema.getType("v").asGroupType();
    assertEquals(OPTIONAL, vGroup.getRepetition(), "Nullable variant should be OPTIONAL");
    assertTrue(vGroup.getFields().stream().anyMatch(f -> f.getName().equals("typed_value")),
        "Shredded variant schema should still contain typed_value even when value is null");
  }

  /**
   * Tests that when HoodieSchema says shredded but Spark schema doesn't have VariantType,
   * the shredding is not applied (graceful fallback).
   */
  @Test
  public void testSchemaMismatchNonVariantSparkType() throws IOException {
    // Setup: HoodieSchema says shredded variant, but Spark schema uses StringType instead
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariantShredded("v", null, "Shredded variant",
        HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema recordSchema = HoodieSchema.createRecord("record", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("v", variantSchema, null, null)
    ));

    // Spark schema uses StringType instead of VariantType — mismatch
    StructType sparkSchema = new StructType(new StructField[] {
        new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("v", DataTypes.StringType, false, Metadata.empty())
    });

    // Row with string value
    GenericInternalRow row = new GenericInternalRow(new Object[] {1, UTF8String.fromString("hello")});

    File outputFile = new File(tempDir, "mismatch_variant.parquet");
    writeRows(outputFile, sparkSchema, recordSchema, row);

    // Verify: shredding was NOT applied since Spark type is not VariantType
    MessageType parquetSchema = readParquetSchema(outputFile);
    Type vField = parquetSchema.getType("v");
    // Should be a primitive string, not a group with typed_value
    assertTrue(vField.isPrimitive(), "Non-variant Spark type should remain primitive even if HoodieSchema says VARIANT");
  }

  /**
   * Tests that createVariantShreddedObject rejects an empty shredded fields map.
   */
  @Test
  public void testEmptyShreddedFieldsMapThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> HoodieSchema.createVariantShreddedObject(Collections.emptyMap()),
        "Empty shredded fields map should throw IllegalArgumentException");
  }

  /**
   * Helper method to create a VariantVal with sample data derived from Spark 4.0 investigation.
   * Represents JSON:
   * <pre>{"a":1,"b":"2","c":3.3,"d":4.4}</pre>
   * <p>
   * The values are obtained by following these steps:
   * <a href="https://github.com/apache/hudi/issues/17744#issuecomment-3822180385">how to obtain raw bytes</a>
   */
  private static VariantVal getSampleVariantVal() {
    byte[] metadata = new byte[] {0x01, 0x04, 0x00, 0x01, 0x02, 0x03, 0x04, 0x61, 0x62, 0x63, 0x64};
    byte[] value = new byte[] {0x02, 0x04, 0x00, 0x01, 0x02, 0x03, 0x00, 0x02, 0x04, 0x0a, 0x10, 0x0c,
        0x01, 0x05, 0x32, 0x20, 0x01, 0x21, 0x00, 0x00, 0x00, 0x20, 0x01, 0x2c, 0x00, 0x00, 0x00};
    return new VariantVal(value, metadata);
  }

  /**
   * Helper method to create a VariantVal with sample data derived from Spark 4.0 investigation.
   * Represents JSON:
   * <pre>100</pre>
   * <p>
   * The values are obtained by following these steps:
   * <a href="https://github.com/apache/hudi/issues/17744#issuecomment-3822180385">how to obtain raw bytes</a>
   */
  private static VariantVal getSampleVariantValWithRootLevelScalar() {
    byte[] metadata = new byte[] {0x01, 0x00, 0x00};
    byte[] value = new byte[] {0x0c, 0x64};
    return new VariantVal(value, metadata);
  }
}
