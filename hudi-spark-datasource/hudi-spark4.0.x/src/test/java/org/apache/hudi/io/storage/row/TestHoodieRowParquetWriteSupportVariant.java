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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
   * group {
   * required binary metadata;
   * optional binary value;
   * }
   * </pre>
   * Note: Even though typed_value is not populated, the schema must indicate 'value' is OPTIONAL.
   */
  @Test
  public void testWriteShreddedVariant() throws IOException {
    // Setup Hoodie Schema: Shredded
    HoodieSchema.Variant variantSchema = HoodieSchema.createVariantShredded("v", null, "Shredded variant", null);
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

    // Verify typed_value is omitted (as implementation skips it)
    boolean hasTypedValue = vGroup.getFields().stream().anyMatch(f -> f.getName().equals("typed_value"));
    assertFalse(hasTypedValue, "typed_value field should be omitted in this writer implementation");
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
   * Represents JSON: {"a":1,"b":"2","c":3.3,"d":4.4}
   */
  private static VariantVal getSampleVariantVal() {
    // Metadata: [01 04 00 01 02 03 04 61 62 63 64]
    byte[] metadata = new byte[] {0x01, 0x04, 0x00, 0x01, 0x02, 0x03, 0x04, 0x61, 0x62, 0x63, 0x64};
    // Value: [02 01 03 00 06 20 01 2C 00 00 00]
    byte[] value = new byte[] {0x02, 0x01, 0x03, 0x00, 0x06, 0x20, 0x01, 0x2C, 0x00, 0x00, 0x00};
    return new VariantVal(value, metadata);
  }
}
