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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.variant.Spark4VariantShreddingProvider;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Round-trip coverage for the successful {@code create} -> {@code reconstruct} path of
 * {@link HoodieVariantReconstruction}: the AVRO read-path orchestration (isTarget alignment,
 * shredded/unshredded sub-schema indexing, and per-record field mapping).
 *
 * <p>Runs in hudi-spark4-common so the real {@link Spark4VariantShreddingProvider} is auto-detected
 * on the classpath; the package is {@code org.apache.hudi.io.storage.hadoop} to reach the
 * package-private class. Spark compaction never reaches this path (it reads base files via the
 * InternalRow reader), so this is the only place the alignment is exercised end to end.
 * The null/throw guards of {@code create} are covered by {@code TestHoodieVariantReconstruction}
 * (hudi-hadoop-common); the provider's shred/rebuild in isolation by {@code TestSpark4VariantShreddingProvider}.
 */
class TestHoodieVariantReconstructionRoundTrip {

  @Test
  void createThenReconstructRebuildsVariantAndPassesThroughNonVariant(@TempDir Path tmp) throws Exception {
    // A shredded variant column "v" alongside a non-variant column "id", to exercise field alignment.
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.STRING));
    shreddedFields.put("b", HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema.Variant shreddedVariant = HoodieSchema.createVariantShreddedObject(shreddedFields);
    HoodieSchema.Variant unshreddedVariant = HoodieSchema.createVariant();

    HoodieSchema fileSchema = HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("v", shreddedVariant)));
    HoodieSchema requestedSchema = HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("v", unshreddedVariant)));

    HoodieStorage storage = HoodieTestUtils.getStorage(tmp.toString()); // allow.reading.shredded defaults true
    HoodieVariantReconstruction reconstruction =
        HoodieVariantReconstruction.create(fileSchema, requestedSchema, storage);
    assertNotNull(reconstruction, "create should build a reconstruction for a shredded variant column");

    // Build the input the parquet-avro reader would produce: a record at the intermediate (shredded)
    // schema, i.e. {id, v=<shredded {metadata, value, typed_value}>}.
    Spark4VariantShreddingProvider provider = new Spark4VariantShreddingProvider();
    Variant original = VariantBuilder.parseJson("{\"a\":\"x\",\"b\":5}", false);
    GenericRecord unshreddedV = new GenericData.Record(unshreddedVariant.getAvroSchema());
    unshreddedV.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(original.getMetadata()));
    unshreddedV.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, ByteBuffer.wrap(original.getValue()));
    GenericRecord shreddedV =
        provider.shredVariantRecord(unshreddedV, shreddedVariant.getAvroSchema(), shreddedVariant);

    GenericRecord input = new GenericData.Record(reconstruction.intermediateSchema().getAvroSchema());
    input.put("id", 7L);
    input.put("v", shreddedV);

    IndexedRecord out = reconstruction.reconstruct(input);

    // Non-variant column passes through unchanged at its position; the variant column is rebuilt unshredded.
    assertEquals(7L, out.get(0));
    GenericRecord rebuiltV = (GenericRecord) out.get(1);
    Variant rebuilt = new Variant(
        toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD)),
        toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD)));
    assertEquals(original.toJson(ZoneOffset.UTC), rebuilt.toJson(ZoneOffset.UTC));
  }

  @Test
  void createThenReconstructRebuildsAValueLessShreddedGroup(@TempDir Path tmp) throws Exception {
    // The shredding spec lets a writer omit `value` when nothing is left over. TestHoodieVariantReconstruction
    // pins that Hudi detects the resulting two-field group, but its stub provider ignores the shredded
    // schema, so this is the only place the real provider sees the shape: buildVariantSchema assigns
    // variantIdx = -1 there, which shifts every other ordinal and is what ShreddingUtils.rebuild reads to
    // decide there is no residual. Get the mapping wrong and the rebuild reads the wrong Avro field.
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.STRING));
    shreddedFields.put("b", HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema.Variant shreddedVariant = HoodieSchema.createVariantShreddedObject(shreddedFields);
    HoodieSchema.Variant unshreddedVariant = HoodieSchema.createVariant();

    // Shred with the real provider first, then drop the (null) top-level residual, which yields exactly
    // what a writer that omits `value` would have put on disk.
    Spark4VariantShreddingProvider provider = new Spark4VariantShreddingProvider();
    Variant original = VariantBuilder.parseJson("{\"a\":\"x\",\"b\":5}", false);
    GenericRecord unshreddedV = new GenericData.Record(unshreddedVariant.getAvroSchema());
    unshreddedV.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(original.getMetadata()));
    unshreddedV.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, ByteBuffer.wrap(original.getValue()));
    GenericRecord shreddedV =
        provider.shredVariantRecord(unshreddedV, shreddedVariant.getAvroSchema(), shreddedVariant);
    assertNull(shreddedV.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD),
        "both fields matched the shredding schema, so there is no residual and dropping `value` loses nothing");

    // The file side is a plain {metadata, typed_value} record: a real footer conversion loses the variant
    // logical type, so this is also what shape detection has to recognize.
    HoodieSchema valueLessShredded = HoodieSchema.createRecord("v", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of(HoodieSchema.Variant.VARIANT_METADATA_FIELD, HoodieSchema.create(HoodieSchemaType.BYTES)),
        HoodieSchemaField.of(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD,
            shreddedVariant.getTypedValueField().get())));
    HoodieSchema fileSchema = HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("v", valueLessShredded)));
    HoodieSchema requestedSchema = HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("v", unshreddedVariant)));

    HoodieStorage storage = HoodieTestUtils.getStorage(tmp.toString()); // allow.reading.shredded defaults true
    HoodieVariantReconstruction reconstruction =
        HoodieVariantReconstruction.create(fileSchema, requestedSchema, storage);
    assertNotNull(reconstruction, "a shredded group with no value column must still engage");

    GenericRecord valueLessV = new GenericData.Record(
        reconstruction.intermediateSchema().getAvroSchema().getField("v").schema());
    valueLessV.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD,
        shreddedV.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD));
    valueLessV.put(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD,
        shreddedV.get(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD));

    GenericRecord input = new GenericData.Record(reconstruction.intermediateSchema().getAvroSchema());
    input.put("id", 7L);
    input.put("v", valueLessV);

    IndexedRecord out = reconstruction.reconstruct(input);

    assertEquals(7L, out.get(0));
    GenericRecord rebuiltV = (GenericRecord) out.get(1);
    Variant rebuilt = new Variant(
        toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD)),
        toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD)));
    assertEquals(original.toJson(ZoneOffset.UTC), rebuilt.toJson(ZoneOffset.UTC));
  }

  private static byte[] toBytes(Object byteBuffer) {
    ByteBuffer buf = ((ByteBuffer) byteBuffer).duplicate();
    byte[] out = new byte[buf.remaining()];
    buf.get(out);
    return out;
  }
}
