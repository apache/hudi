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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.Version;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Physical page bounds for inline prepared images through the embedding table's real writer factory. */
public class TestEmbeddingPageBound {

  /** Each image fits below the 1 MiB inline threshold and just below the default page target. */
  private static final int IMAGE_BYTES = 1_000_000;
  private static final String INLINE_DATA = EmbeddingTableSchema.PREPARED_IMAGE + "." + HoodieSchema.Blob.INLINE_DATA_FIELD;

  @TempDir
  Path tempDir;

  /** The single-row case controls the setup; a supported builder must flush the larger batch during the write. */
  @ParameterizedTest
  @ValueSource(ints = {1, 24})
  public void inlineImagePagesFollowTheWritersSupportedCadence(int rows) throws Exception {
    Schema blobSchema = HoodieSchema.createBlob().toAvroSchema();
    Schema avro = SchemaBuilder.record("EmbeddingRow").fields().requiredString("source_path_sha")
        .name(EmbeddingTableSchema.PREPARED_IMAGE).type(blobSchema).noDefault().endRecord();
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(HoodieSchema.fromAvroSchema(avro));
    TypedProperties props = new TypedProperties();
    // The direct Apache writer requires the codec normally resolved by the engine write config.
    props.setProperty(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key(), "snappy");
    props.setProperty(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key(), EmbeddingParquetConfigInjector.class.getName());
    Configuration conf = new Configuration();
    StoragePath output = new StoragePath(tempDir.resolve("embedding.parquet").toString());
    try (HoodieAvroParquetWriter writer = (HoodieAvroParquetWriter) HoodieFileWriterFactory.getFileWriter("001", output,
        new HoodieHadoopStorage(output, new HadoopStorageConfiguration(conf)), new HoodieConfig(props), schema,
        new LocalTaskContextSupplier(), HoodieRecordType.AVRO)) {
      for (int i = 0; i < rows; i++) {
        byte[] image = new byte[IMAGE_BYTES];
        new Random(i).nextBytes(image);
        GenericRecord blob = new GenericData.Record(blobSchema);
        blob.put(HoodieSchema.Blob.TYPE, HoodieSchema.Blob.INLINE);
        blob.put(HoodieSchema.Blob.INLINE_DATA_FIELD, ByteBuffer.wrap(image));
        blob.put(HoodieSchema.Blob.EXTERNAL_REFERENCE, null);
        GenericRecord row = new GenericData.Record(schema.toAvroSchema());
        row.put("source_path_sha", "sha-" + i);
        row.put(EmbeddingTableSchema.PREPARED_IMAGE, blob);
        writer.writeAvro(Integer.toString(i), row);
      }
    }

    long values = 0;
    int pageCount = 0;
    int maxValues = 0;
    int maxBytes = 0;
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(
        new org.apache.hadoop.fs.Path(output.toUri()), conf))) {
      assertEquals(1, reader.getFooter().getBlocks().size(), "the default row group holds all 24 MB");
      assertEquals(rows, reader.getFooter().getBlocks().get(0).getRowCount(), "every image is in the file");
      MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
      ColumnDescriptor data = fileSchema.getColumns().stream().filter(c -> String.join(".", c.getPath()).equals(INLINE_DATA))
          .findFirst().orElseThrow(() -> new AssertionError("No " + INLINE_DATA));
      reader.setRequestedSchema(new MessageType("inline_data", fileSchema.getType(EmbeddingTableSchema.PREPARED_IMAGE)));
      PageReadStore group;
      while ((group = reader.readNextRowGroup()) != null) {
        PageReader pages = group.getPageReader(data);
        DataPage page;
        while ((page = pages.readPage()) != null) {
          values += page.getValueCount();
          pageCount++;
          maxValues = Math.max(maxValues, page.getValueCount());
          maxBytes = Math.max(maxBytes, page.getUncompressedSize());
        }
      }
    }
    String layout = "rows=" + rows + ", groups=1, pages=" + pageCount + ", values=" + values
        + ", maxPageValues=" + maxValues + ", maxPageBytes=" + maxBytes;
    System.out.println(layout);
    assertEquals(rows, values, "every inline image is in a page: " + layout);
    if (SemanticVersion.parse(Version.VERSION_NUMBER).compareTo(new SemanticVersion(1, 12, 3)) >= 0) {
      assertTrue(maxValues <= 2, "a page holds at most two near-threshold images: " + layout);
      assertTrue(maxBytes <= 2L * IMAGE_BYTES + 4096, "a page is bounded by two images plus encoding overhead: " + layout);
    } else {
      // Parquet before 1.12.3 has no builder setters for the row-check interval; the writer keeps its default cadence.
      assertEquals(1, pageCount, "the batch is below the default 100-row check interval: " + layout);
      assertEquals(rows, maxValues, "every image is in the closing page: " + layout);
      assertTrue(maxBytes <= (long) rows * IMAGE_BYTES + 4096, "the page holds the batch plus encoding overhead: " + layout);
    }
  }
}
