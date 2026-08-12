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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.common.avro.VariantShreddingProvider;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieVariantReconstruction {

  private static HoodieSchema recordWithVariant(HoodieSchema variantSchema) {
    return HoodieSchema.createRecord("test_record", "org.apache.hudi.test", null,
        Collections.singletonList(HoodieSchemaField.of("v", variantSchema)));
  }

  private static HoodieStorage storageWithReadingShredded(Path tmp, boolean enabled) {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmp.toString());
    storage.getConf().set(HoodieStorageConfig.PARQUET_VARIANT_ALLOW_READING_SHREDDED.key(),
        Boolean.toString(enabled));
    return storage;
  }

  @Test
  void failsFastWhenShreddedColumnPresentButReadingDisabled(@TempDir Path tmp) {
    HoodieSchema fileSchema = recordWithVariant(
        HoodieSchema.createVariantShredded(HoodieSchema.create(HoodieSchemaType.INT)));
    HoodieSchema requestedSchema = recordWithVariant(HoodieSchema.createVariant());

    HoodieException ex = assertThrows(HoodieException.class, () ->
        HoodieVariantReconstruction.create(fileSchema, requestedSchema,
            storageWithReadingShredded(tmp, false)));
    // The flag check must run after shredded-column detection: dropping typed_value silently would
    // corrupt rows whose payload lives there, so a disabled read of a shredded file fails fast.
    assertTrue(ex.getMessage().contains("reading shredded variants is disabled"), ex.getMessage());
  }

  @Test
  void returnsNullWhenNoShreddedColumnEvenIfReadingDisabled(@TempDir Path tmp) {
    HoodieSchema schema = recordWithVariant(HoodieSchema.createVariant());
    // No shredded variant column to reconstruct: nothing to do regardless of the flag.
    assertNull(HoodieVariantReconstruction.create(schema, schema,
        storageWithReadingShredded(tmp, false)));
  }

  @Test
  void returnsNullForNonRecordSchemas(@TempDir Path tmp) {
    HoodieStorage storage = storageWithReadingShredded(tmp, true);
    assertNull(HoodieVariantReconstruction.create(
        HoodieSchema.create(HoodieSchemaType.STRING), recordWithVariant(HoodieSchema.createVariant()), storage));
    assertNull(HoodieVariantReconstruction.create(
        recordWithVariant(HoodieSchema.createVariant()), HoodieSchema.create(HoodieSchemaType.STRING), storage));
  }

  @Test
  void failsFastWhenProviderIsUnavailable(@TempDir Path tmp) {
    HoodieSchema fileSchema = recordWithVariant(
        HoodieSchema.createVariantShredded(HoodieSchema.create(HoodieSchemaType.INT)));
    HoodieSchema requestedSchema = recordWithVariant(HoodieSchema.createVariant());

    HoodieException exception = assertThrows(HoodieException.class, () ->
        HoodieVariantReconstruction.create(fileSchema, requestedSchema,
            storageWithReadingShredded(tmp, true)));
    assertTrue(exception.getMessage().contains("no VariantShreddingProvider is available"));
  }

  @Test
  void reconstructsTargetFieldsAndPassesThroughOtherValues(@TempDir Path tmp) {
    HoodieSchema fileSchema = recordWithIdAndVariant(
        HoodieSchema.createVariantShredded(HoodieSchema.create(HoodieSchemaType.INT)));
    HoodieSchema requestedSchema = recordWithIdAndVariant(HoodieSchema.createVariant());
    HoodieStorage storage = storageWithReadingShredded(tmp, true);
    storage.getConf().set(HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_PROVIDER_CLASS.key(),
        TestVariantShreddingProvider.class.getName());

    HoodieVariantReconstruction reconstruction = HoodieVariantReconstruction.create(
        fileSchema, requestedSchema, storage);
    assertNotNull(reconstruction);
    assertTrue(reconstruction.intermediateSchema().getField("v").get().schema().getNonNullType()
        instanceof HoodieSchema.Variant);
    assertTrue(((HoodieSchema.Variant) reconstruction.intermediateSchema().getField("v").get()
        .schema().getNonNullType()).isShredded());

    GenericRecord shredded = new GenericData.Record(
        reconstruction.intermediateSchema().getField("v").get().schema().getNonNullType().toAvroSchema());
    shredded.put("metadata", ByteBuffer.wrap(new byte[] {1}));
    shredded.put("value", null);
    shredded.put("typed_value", 42);
    GenericRecord input = new GenericData.Record(reconstruction.intermediateSchema().toAvroSchema());
    input.put("id", "record-1");
    input.put("v", shredded);

    IndexedRecord output = reconstruction.reconstruct(input);
    assertEquals("record-1", output.get(0).toString());
    GenericRecord variant = (GenericRecord) output.get(1);
    assertEquals(ByteBuffer.wrap(new byte[] {1}), variant.get("metadata"));
    assertEquals(ByteBuffer.wrap(new byte[] {42}), variant.get("value"));

    input.put("v", null);
    assertNull(reconstruction.reconstruct(input).get(1));
  }

  @Test
  void engagesOnFooterDerivedPlainShreddedShape(@TempDir Path tmp) {
    // #19567: a real file schema comes from converting the parquet footer MessageType, which
    // loses the variant logical type, so the shredded column arrives as a PLAIN record of
    // {metadata, value, typed_value}. Detection must anchor on the requested column being a
    // variant and match the file side by shape, then reconstruction proceeds as usual.
    HoodieSchema fileSchema = recordWithIdAndVariant(footerStylePlainShreddedSchema());
    HoodieSchema requestedSchema = recordWithIdAndVariant(HoodieSchema.createVariant());
    HoodieStorage storage = storageWithReadingShredded(tmp, true);
    storage.getConf().set(HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_PROVIDER_CLASS.key(),
        TestVariantShreddingProvider.class.getName());

    HoodieVariantReconstruction reconstruction = HoodieVariantReconstruction.create(
        fileSchema, requestedSchema, storage);
    assertNotNull(reconstruction, "Plain-record shredded shape with a variant requested column must engage");

    GenericRecord shredded = new GenericData.Record(
        reconstruction.intermediateSchema().getField("v").get().schema().getNonNullType().toAvroSchema());
    shredded.put("metadata", ByteBuffer.wrap(new byte[] {1}));
    shredded.put("value", null);
    shredded.put("typed_value", 42);
    GenericRecord input = new GenericData.Record(reconstruction.intermediateSchema().toAvroSchema());
    input.put("id", "record-1");
    input.put("v", shredded);

    IndexedRecord output = reconstruction.reconstruct(input);
    assertEquals("record-1", output.get(0).toString());
    GenericRecord variant = (GenericRecord) output.get(1);
    assertEquals(ByteBuffer.wrap(new byte[] {1}), variant.get("metadata"));
    assertEquals(ByteBuffer.wrap(new byte[] {42}), variant.get("value"));
  }

  @Test
  void returnsNullForFooterDerivedPlainUnshreddedShape(@TempDir Path tmp) {
    // The ordinary unshredded layout after footer conversion: a plain {metadata, value} record
    // with the variant logical type lost. There is no typed_value to reconstruct, so detection
    // must stay disengaged (the column reads directly at the requested schema) even with
    // shredded reading disabled.
    HoodieSchema fileSchema = recordWithIdAndVariant(footerStylePlainUnshreddedSchema());
    HoodieSchema requestedSchema = recordWithIdAndVariant(HoodieSchema.createVariant());
    assertNull(HoodieVariantReconstruction.create(fileSchema, requestedSchema,
        storageWithReadingShredded(tmp, false)));
  }

  @Test
  void ignoresShreddedShapeWhenRequestedColumnIsNotVariant(@TempDir Path tmp) {
    // A user struct that merely has the {metadata, value, typed_value} shape must not be
    // treated as a shredded variant: without the requested-side variant anchor there is
    // nothing to reconstruct, so create() returns null even with shredded reading disabled.
    HoodieSchema fileSchema = recordWithIdAndVariant(footerStylePlainShreddedSchema());
    HoodieSchema requestedSchema = recordWithIdAndVariant(footerStylePlainShreddedSchema());
    assertNull(HoodieVariantReconstruction.create(fileSchema, requestedSchema,
        storageWithReadingShredded(tmp, false)));
  }

  /**
   * The shape a shredded variant column has after the parquet footer MessageType is converted
   * back to a schema: a plain record of {metadata, value, typed_value} with no variant logical
   * type attached.
   */
  private static HoodieSchema footerStylePlainShreddedSchema() {
    return HoodieSchema.createRecord("v", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("metadata", HoodieSchema.create(HoodieSchemaType.BYTES)),
        HoodieSchemaField.of("value", HoodieSchema.createNullable(HoodieSchemaType.BYTES)),
        HoodieSchemaField.of("typed_value", HoodieSchema.createNullable(HoodieSchemaType.INT))));
  }

  /**
   * The shape an unshredded variant column has after the parquet footer MessageType is converted
   * back to a schema: a plain record of {metadata, value} with no variant logical type attached.
   */
  private static HoodieSchema footerStylePlainUnshreddedSchema() {
    return HoodieSchema.createRecord("v", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("metadata", HoodieSchema.create(HoodieSchemaType.BYTES)),
        HoodieSchemaField.of("value", HoodieSchema.createNullable(HoodieSchemaType.BYTES))));
  }

  private static HoodieSchema recordWithIdAndVariant(HoodieSchema variantSchema) {
    return HoodieSchema.createRecord("test_record", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("v", variantSchema)));
  }

  public static class TestVariantShreddingProvider implements VariantShreddingProvider {
    @Override
    public GenericRecord shredVariantRecord(
        GenericRecord unshreddedVariant, Schema shreddedSchema, HoodieSchema.Variant variantSchema) {
      throw new UnsupportedOperationException("Not used by the reconstruction test");
    }

    @Override
    public GenericRecord rebuildVariantRecord(
        GenericRecord shreddedVariant, Schema shreddedSchema, Schema unshreddedSchema) {
      GenericRecord rebuilt = new GenericData.Record(unshreddedSchema);
      rebuilt.put("metadata", shreddedVariant.get("metadata"));
      rebuilt.put("value", ByteBuffer.wrap(new byte[] {
          ((Number) shreddedVariant.get("typed_value")).byteValue()}));
      return rebuilt;
    }
  }
}
