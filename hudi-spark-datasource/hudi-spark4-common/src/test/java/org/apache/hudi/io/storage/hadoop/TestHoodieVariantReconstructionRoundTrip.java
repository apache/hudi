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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.core.io.storage.HoodieAvroBootstrapFileReader;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.variant.Spark4VariantShreddingProvider;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.avro.AvroParquetWriter;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  /**
   * What every bootstrap leg expects of the joined row read at {@code requestedSchema}: the
   * variant column survives the join, rebuilds to {@code original}, and the data column below it
   * keeps its value rather than shifting.
   */
  private static void assertBootstrapRoundTrip(IndexedRecord out, HoodieSchema requestedSchema, Variant original) {
    GenericRecord rebuiltV = (GenericRecord) out.get(requestedSchema.getAvroSchema().getField("v").pos());
    assertNotNull(rebuiltV, "the variant column must survive the bootstrap join");
    Variant rebuilt = new Variant(
        toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD)),
        toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD)));
    assertEquals(original.toJson(ZoneOffset.UTC), rebuilt.toJson(ZoneOffset.UTC),
        "the shredded payload must be reconstructed, not dropped");
    assertEquals(7L, out.get(requestedSchema.getAvroSchema().getField("id").pos()),
        "the data column must keep its value");
  }

  @Test
  void bootstrapReaderReconstructsShreddedDataFileUsingTableSchema(@TempDir java.nio.file.Path tmp) throws Exception {
    // HoodieMergeHelper's bootstrap branch uses the one-argument getRecordIterator overload,
    // which used to hand the DATA file its own footer schema: for a shredded variant that is a
    // plain {metadata, value, typed_value} record with the logical type lost, so reconstruction
    // never engaged and the later rewrite to the writer schema silently dropped typed_value.
    // The overload now requests the caller's schema (minus meta fields), which anchors
    // HoodieVariantReconstruction just like the two-argument form.
    //
    // AVRO bootstrap reader only, deliberately: the changed line lives on the shared base
    // class, but the SPARK twin reads through HoodieSparkParquetReader (whose getSchema returns
    // a nullable union, not a record) and would need a Spark session plus InternalRow
    // assertions that do not fit this unit test. The non-variant SPARK bootstrap path is
    // covered end-to-end by TestBootstrap.
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.STRING));
    shreddedFields.put("b", HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema.Variant shreddedVariant = HoodieSchema.createVariantShreddedObject(shreddedFields);
    HoodieSchema.Variant unshreddedVariant = HoodieSchema.createVariant();
    // Data fields are nullable with null defaults, as Hudi table schemas declare them; the
    // skeleton read requests the full schema and fills the data columns with the defaults.
    HoodieSchema dataFileSchema = HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.createNullable(HoodieSchemaType.LONG), null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("v", HoodieSchema.createNullable(shreddedVariant), null, HoodieSchema.NULL_VALUE)));

    // The shredded data file, as an external bootstrap source would carry it.
    Spark4VariantShreddingProvider provider = new Spark4VariantShreddingProvider();
    Variant original = VariantBuilder.parseJson("{\"a\":\"x\",\"b\":5}", false);
    GenericRecord unshreddedV = new GenericData.Record(unshreddedVariant.getAvroSchema());
    unshreddedV.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(original.getMetadata()));
    unshreddedV.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, ByteBuffer.wrap(original.getValue()));
    GenericRecord shreddedV =
        provider.shredVariantRecord(unshreddedV, shreddedVariant.getAvroSchema(), shreddedVariant);
    java.nio.file.Path dataFile = tmp.resolve("data.parquet");
    try (AvroParquetWriter<GenericRecord> writer =
             new AvroParquetWriter<>(new org.apache.hadoop.fs.Path(dataFile.toString()), dataFileSchema.getAvroSchema())) {
      GenericRecord record = new GenericData.Record(dataFileSchema.getAvroSchema());
      record.put("id", 7L);
      record.put("v", shreddedV);
      writer.write(record);
    }

    // The skeleton file: just the meta columns for the row.
    HoodieSchema skeletonSchema = HoodieSchemaUtils.addMetadataFields(
        HoodieSchema.createRecord("r", "org.apache.hudi.test", null, java.util.Collections.emptyList()));
    java.nio.file.Path skeletonFile = tmp.resolve("skeleton.parquet");
    try (AvroParquetWriter<GenericRecord> writer =
             new AvroParquetWriter<>(new org.apache.hadoop.fs.Path(skeletonFile.toString()), skeletonSchema.getAvroSchema())) {
      GenericRecord meta = new GenericData.Record(skeletonSchema.getAvroSchema());
      meta.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, "001");
      meta.put(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, "001_0_1");
      meta.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, "key-7");
      meta.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, "");
      meta.put(HoodieRecord.FILENAME_METADATA_FIELD, "data.parquet");
      writer.write(meta);
    }

    HoodieStorage storage = HoodieTestUtils.getStorage(tmp.toString());
    HoodieAvroFileReaderFactory readerFactory = new HoodieAvroFileReaderFactory(storage);
    HoodieFileReader skeletonReader =
        readerFactory.getFileReader(new HoodieConfig(), new StoragePath(skeletonFile.toUri().toString()));
    HoodieFileReader dataReader =
        readerFactory.getFileReader(new HoodieConfig(), new StoragePath(dataFile.toUri().toString()));
    HoodieAvroBootstrapFileReader bootstrapReader = (HoodieAvroBootstrapFileReader)
        readerFactory.newBootstrapFileReader(skeletonReader, dataReader, Option.empty(), new Object[0]);

    // The merge helper requests the writer schema with meta fields: an UNSHREDDED variant.
    HoodieSchema tableSchema = HoodieSchemaUtils.addMetadataFields(
        HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.createNullable(HoodieSchemaType.LONG), null, HoodieSchema.NULL_VALUE),
            HoodieSchemaField.of("v", HoodieSchema.createNullable(unshreddedVariant), null, HoodieSchema.NULL_VALUE))));
    try (ClosableIterator<HoodieRecord<IndexedRecord>> iterator =
             (ClosableIterator) bootstrapReader.getRecordIterator(tableSchema)) {
      assertTrue(iterator.hasNext(), "the joined bootstrap row must come back");
      IndexedRecord out = (IndexedRecord) iterator.next().getData();
      assertBootstrapRoundTrip(out, tableSchema, original);
    }

    // The alignment only bites when the requested schema holds a column the external data file
    // does not: a partitioned bootstrap table's partition column (the source is read with
    // basePath, so the table schema gains it while the file lacks it) or a column added after
    // bootstrap. Reading the data file at its own footer schema also mis-sizes the meta-field
    // prefix in HoodieAvroIndexedRecord.prependMetaFields, which shifts every data column.
    // HUDI-5392 (#7461) pinned the same "request the caller's schema" alignment for arrays, which
    // were read at the wrong LIST level when the request came from Hudi instead of the file.
    HoodieSchema partitionedTableSchema = HoodieSchemaUtils.addMetadataFields(
        HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.createNullable(HoodieSchemaType.LONG), null, HoodieSchema.NULL_VALUE),
            HoodieSchemaField.of("v", HoodieSchema.createNullable(unshreddedVariant), null, HoodieSchema.NULL_VALUE),
            HoodieSchemaField.of("part", HoodieSchema.createNullable(HoodieSchemaType.STRING), null, HoodieSchema.NULL_VALUE))));
    // Partition values reach the reader through its constructor - HoodieBootstrapRecordIterator
    // reads them from there and never parses them out of the file path - so `part` comes back as
    // the value handed in here, not as null and not from a part=p1 directory.
    HoodieAvroBootstrapFileReader partitionedReader = (HoodieAvroBootstrapFileReader)
        readerFactory.newBootstrapFileReader(
            readerFactory.getFileReader(new HoodieConfig(), new StoragePath(skeletonFile.toUri().toString())),
            readerFactory.getFileReader(new HoodieConfig(), new StoragePath(dataFile.toUri().toString())),
            Option.of(new String[] {"part"}), new Object[] {"p1"});
    try (ClosableIterator<HoodieRecord<IndexedRecord>> iterator =
             (ClosableIterator) partitionedReader.getRecordIterator(partitionedTableSchema)) {
      assertTrue(iterator.hasNext(), "the joined bootstrap row must come back");
      IndexedRecord out = (IndexedRecord) iterator.next().getData();
      assertEquals(partitionedTableSchema.getAvroSchema(), out.getSchema(),
          "the record must carry exactly the requested schema's fields");
      assertBootstrapRoundTrip(out, partitionedTableSchema, original);
      assertEquals("p1", String.valueOf(out.get(partitionedTableSchema.getAvroSchema().getField("part").pos())),
          "the partition column, absent from the data file, is filled from the partition values");
    }
  }
}
