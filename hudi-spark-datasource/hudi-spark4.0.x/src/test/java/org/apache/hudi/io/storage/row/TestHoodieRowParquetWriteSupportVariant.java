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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    assertTrue(hasTypedValue, "typed_value field should be omitted in this writer implementation");

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
