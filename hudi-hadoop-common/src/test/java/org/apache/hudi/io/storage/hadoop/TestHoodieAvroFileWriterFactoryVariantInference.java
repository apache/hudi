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

import org.apache.hudi.common.avro.VariantSchemaUtils;
import org.apache.hudi.common.avro.VariantShreddingRuntime;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Pins the no-inferrer degradation of shredding-schema inference in
 * {@link HoodieAvroFileWriterFactory}. Inference is on by default, so this gate is reached by every
 * engine's Avro write path with a stock config; a classpath without a Spark 4.1+ version module
 * (Flink, Java, and Spark 3.x and 4.0 in production) must degrade to the plain unshredded writer.
 * This module's classpath carries no Spark version module, so
 * {@link VariantShreddingRuntime#lookupInferrer()} is empty here, which is what those engines see.
 */
public class TestHoodieAvroFileWriterFactoryVariantInference {

  @TempDir
  java.nio.file.Path tmpDir;

  @Test
  public void testDefaultInferenceWithoutInferrerWritesPlainUnshreddedFile() throws Exception {
    assumeFalse(VariantShreddingRuntime.lookupInferrer().isPresent(),
        "this test pins the fallback for classpaths without a shredding-schema inferrer");

    HoodieSchema schema = HoodieSchema.createRecord("rec", "ns", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("v", HoodieSchema.createNullable(HoodieSchema.createVariant()))));
    HoodieConfig config = new HoodieConfig();
    assertTrue(config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_SCHEMA_INFERENCE_ENABLED),
        "inference is on by default since #19690; this test pins what that default does without an inferrer");
    config.setValue(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, "zstd");
    // Name a provider explicitly: the factory also declines when no shredding provider is available,
    // and this module ships none, so without this the inferrer gate (the one under test) would never
    // be reached. The class is never loaded: the write support only resolves it for shredded schemas.
    config.setValue(HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_PROVIDER_CLASS,
        "org.apache.hudi.variant.Spark4VariantShreddingProvider");
    // Inference applies as far as the config is concerned...
    assertEquals(singletonList("v"), VariantSchemaUtils.getInferableVariantColumns(config, schema));

    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    StoragePath path = new StoragePath(tmpDir.resolve("f1_1-0-1_000.parquet").toAbsolutePath().toString());
    HoodieFileWriter writer = new HoodieAvroFileWriterFactory(storage)
        .newParquetFileWriter("000", path, config, schema, new LocalTaskContextSupplier());
    // ...but with no inferrer on the classpath the factory must hand out the plain writer, not the
    // inference decorator, and the write must go through unshredded rather than fail.
    assertInstanceOf(HoodieAvroParquetWriter.class, writer);

    HoodieSchema variantSchema = schema.getField("v").get().schema().getNonNullType();
    GenericRecord variant = new GenericData.Record(variantSchema.toAvroSchema());
    variant.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(new byte[] {1, 0, 0}));
    variant.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, ByteBuffer.wrap(new byte[] {0}));
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "r1");
    record.put("v", variant);
    ((HoodieAvroParquetWriter) writer).writeAvro("r1", record);
    writer.close();
    assertTrue(storage.exists(path));

    MessageType footer;
    try (ParquetFileReader reader = ParquetFileReader.open(new Configuration(), new Path(path.toUri()))) {
      footer = reader.getFooter().getFileMetaData().getSchema();
    }
    GroupType variantGroup = footer.getType("v").asGroupType();
    assertFalse(variantGroup.containsField("typed_value"),
        "no inferrer: the variant must be written unshredded, got " + variantGroup);
    assertTrue(variantGroup.containsField("metadata") && variantGroup.containsField("value"));
  }
}
