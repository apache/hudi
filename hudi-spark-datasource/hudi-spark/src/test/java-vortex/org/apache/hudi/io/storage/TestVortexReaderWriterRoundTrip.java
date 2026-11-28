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

package org.apache.hudi.io.storage;

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.core.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.io.vortex.HoodieVortexFileFormatUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end round-trip test for the Vortex file writer and reader against the real vortex-jni
 * API. Writes InternalRows through {@link HoodieSparkVortexWriter} and reads them back through
 * {@link HoodieSparkVortexReader}, spanning multiple flushed batches.
 */
@DisabledIfSystemProperty(named = "vortex.skip.tests", matches = "true")
public class TestVortexReaderWriterRoundTrip {

  @TempDir
  File tempDir;

  @Test
  public void testWriteReadRoundTrip() throws Exception {
    SparkTaskContextSupplier taskContextSupplier = new SparkTaskContextSupplier();

    StructType schema = new StructType()
        .add("id", DataTypes.LongType, false)
        .add("name", DataTypes.StringType, false)
        .add("score", DataTypes.DoubleType, false);

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/roundtrip.vortex");

    int numRows = 2500; // > default batch size (1000) to force multiple flushed batches
    try (HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.getAbsolutePath())) {
      try (HoodieSparkVortexWriter writer = HoodieSparkVortexWriter.builder()
          .file(path)
          .sparkSchema(schema)
          .instantTime("20251201120000000")
          .taskContextSupplier(taskContextSupplier)
          .storage(storage)
          .populateMetaFields(false)
          .build()) {
        for (int i = 0; i < numRows; i++) {
          InternalRow row = new GenericInternalRow(new Object[] {
              (long) (100 + i), UTF8String.fromString("name" + i), (double) i + 0.5});
          writer.writeRow(row);
        }
      }

      assertTrue(storage.exists(path), "Vortex file should exist");

      try (HoodieSparkVortexReader reader = new HoodieSparkVortexReader(path)) {
        assertEquals(numRows, reader.getTotalRecords(), "row count");
        assertNull(reader.readBloomFilter(), "a writer without a bloom filter writes no bloom filter footer");

        HoodieSchema readSchema = reader.getSchema();
        assertEquals(3, readSchema.getFields().size(), "schema field count");

        int count = 0;
        try (ClosableIterator<HoodieRecord<InternalRow>> it = reader.getRecordIterator(readSchema)) {
          while (it.hasNext()) {
            InternalRow row = it.next().getData();
            assertEquals(100L + count, row.getLong(0), "id at row " + count);
            assertEquals("name" + count, row.getUTF8String(1).toString(), "name at row " + count);
            assertEquals((double) count + 0.5, row.getDouble(2), 1e-9, "score at row " + count);
            count++;
          }
        }
        assertEquals(numRows, count, "rows read back");
      }
    }
  }

  @Test
  public void testProjectedRead() throws Exception {
    SparkTaskContextSupplier taskContextSupplier = new SparkTaskContextSupplier();

    StructType schema = new StructType()
        .add("id", DataTypes.LongType, false)
        .add("name", DataTypes.StringType, false);

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/projected.vortex");
    try (HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.getAbsolutePath())) {
      try (HoodieSparkVortexWriter writer = HoodieSparkVortexWriter.builder()
          .file(path).sparkSchema(schema).instantTime("20251201120000000")
          .taskContextSupplier(taskContextSupplier).storage(storage).populateMetaFields(false).build()) {
        for (int i = 0; i < 3; i++) {
          writer.writeRow(new GenericInternalRow(new Object[] {(long) i, UTF8String.fromString("n" + i)}));
        }
      }

      // Project only the non-leading "name" column so the assertion fails if the reader returns the
      // full row instead of narrowing to the requested schema (projecting the leading "id" column
      // would read back at index 0 either way and could not detect that regression).
      HoodieSchema projected = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
          new StructType().add("name", DataTypes.StringType, false), "record", "", false);
      try (HoodieSparkVortexReader reader = new HoodieSparkVortexReader(path);
           ClosableIterator<HoodieRecord<InternalRow>> it = reader.getRecordIterator(projected)) {
        int count = 0;
        while (it.hasNext()) {
          InternalRow row = it.next().getData();
          assertEquals("n" + count, row.getUTF8String(0).toString(), "projected name at row " + count);
          count++;
        }
        assertEquals(3, count, "projected rows");
      }
    }
  }

  /**
   * Reads the same file with different read concurrency settings. All settings must give the same
   * rows in the same order.
   */
  @Test
  public void testReadConcurrencySettings() throws Exception {
    SparkTaskContextSupplier taskContextSupplier = new SparkTaskContextSupplier();
    int numRows = 200_000;

    StructType schema = new StructType()
        .add("id", DataTypes.LongType, false)
        .add("name", DataTypes.StringType, false);

    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/concurrency.vortex");
    try (HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.getAbsolutePath())) {
      try (HoodieSparkVortexWriter writer = HoodieSparkVortexWriter.builder()
          .file(path).sparkSchema(schema).instantTime("20251201120000000")
          .taskContextSupplier(taskContextSupplier).storage(storage).populateMetaFields(false).build()) {
        for (int i = 0; i < numRows; i++) {
          writer.writeRow(new GenericInternalRow(new Object[] {(long) i, UTF8String.fromString("n" + i)}));
        }
      }

      HoodieSchema readSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
          schema, "record", "", false);
      long allocatorSize = Long.parseLong(HoodieStorageConfig.VORTEX_READ_ALLOCATOR_SIZE_BYTES.defaultValue());

      for (int concurrency : new int[] {0, 1, 4}) {
        Configuration conf = new Configuration();
        conf.setInt(HoodieStorageConfig.VORTEX_READ_SPLIT_CONCURRENCY.key(), concurrency);
        conf.setInt(HoodieStorageConfig.VORTEX_READ_IO_CONCURRENCY.key(), concurrency);

        try (HoodieSparkVortexReader reader =
                 new HoodieSparkVortexReader(path, allocatorSize, new HadoopStorageConfiguration(conf));
             ClosableIterator<HoodieRecord<InternalRow>> it = reader.getRecordIterator(readSchema)) {
          int count = 0;
          while (it.hasNext()) {
            InternalRow row = it.next().getData();
            assertEquals(count, row.getLong(0), "id at row " + count + " with concurrency " + concurrency);
            assertEquals("n" + count, row.getUTF8String(1).toString(),
                "name at row " + count + " with concurrency " + concurrency);
            count++;
          }
          assertEquals(numRows, count, "rows with concurrency " + concurrency);
        }
      }

      // The write, compaction and merge paths get readers from the factory, with the Hudi
      // configuration. A value that is not a number must fail, which shows that it gets to the reader.
      HoodieFileReaderFactory factory = HoodieIOFactory.getIOFactory(storage).getReaderFactory(HoodieRecord.HoodieRecordType.SPARK);
      HoodieConfig validConfig = new HoodieConfig();
      validConfig.setValue(HoodieStorageConfig.VORTEX_READ_SPLIT_CONCURRENCY, "2");
      validConfig.setValue(HoodieStorageConfig.VORTEX_READ_IO_CONCURRENCY, "2");
      try (HoodieFileReader reader = factory.getFileReader(validConfig, path, HoodieFileFormat.VORTEX)) {
        assertEquals(numRows, reader.getTotalRecords());
      }

      // The reader reads the split concurrency when it starts a scan.
      HoodieConfig invalidConfig = new HoodieConfig();
      invalidConfig.setValue(HoodieStorageConfig.VORTEX_READ_SPLIT_CONCURRENCY, "not-a-number");
      try (HoodieFileReader reader = factory.getFileReader(invalidConfig, path, HoodieFileFormat.VORTEX)) {
        Exception e = assertThrows(Exception.class, () -> {
          try (ClosableIterator<?> it = reader.getRecordIterator(readSchema)) {
            it.hasNext();
          }
        });
        assertTrue(hasMessageInCauseChain(e, "not-a-number"), "The Vortex reader must read the split concurrency");
      }

      // The reader reads the I/O concurrency when it opens the file.
      HoodieConfig invalidIoConfig = new HoodieConfig();
      invalidIoConfig.setValue(HoodieStorageConfig.VORTEX_READ_IO_CONCURRENCY, "not-a-number");
      Exception ioError = assertThrows(Exception.class, () -> {
        try (HoodieFileReader reader = factory.getFileReader(invalidIoConfig, path, HoodieFileFormat.VORTEX);
             ClosableIterator<?> it = reader.getRecordIterator(readSchema)) {
          it.hasNext();
        }
      });
      assertTrue(hasMessageInCauseChain(ioError, "not-a-number"), "The Vortex reader must read the I/O concurrency");
    }
  }

  @Test
  public void testBloomFilterAndMinMaxKeysFooter() throws Exception {
    SparkTaskContextSupplier taskContextSupplier = new SparkTaskContextSupplier();
    StructType schema = new StructType().add("id", DataTypes.LongType, false);
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/footer.vortex");
    BloomFilter bloomFilter = BloomFilterFactory.createBloomFilter(
        1000, 0.000001, 10000, BloomFilterTypeCode.DYNAMIC_V0.name());

    // Write the keys out of order and in more than one batch, so that the min/max keys come from
    // the full set of keys and not from the first or the last key.
    int numRows = 2500;
    try (HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.getAbsolutePath())) {
      try (HoodieSparkVortexWriter writer = HoodieSparkVortexWriter.builder()
          .file(path).sparkSchema(schema).instantTime("20251201120000000")
          .taskContextSupplier(taskContextSupplier).storage(storage).populateMetaFields(false)
          .bloomFilterOpt(Option.of(bloomFilter))
          .build()) {
        for (int i = 0; i < numRows; i++) {
          int keyIndex = (i * 7919) % numRows;
          writer.writeRow(recordKey(keyIndex), new GenericInternalRow(new Object[] {(long) keyIndex}));
        }
      }

      String[] expectedMinMax = new String[] {recordKey(0), recordKey(numRows - 1)};

      try (HoodieSparkVortexReader reader = new HoodieSparkVortexReader(path)) {
        assertArrayEquals(expectedMinMax, reader.readMinMaxRecordKeys(), "min/max record keys");

        BloomFilter readBloomFilter = reader.readBloomFilter();
        assertNotNull(readBloomFilter, "bloom filter");
        assertEquals(BloomFilterTypeCode.DYNAMIC_V0, readBloomFilter.getBloomFilterTypeCode(), "bloom filter type");
        assertEquals(bloomFilter.serializeToString(), readBloomFilter.serializeToString(), "bloom filter content");
        for (int i = 0; i < numRows; i++) {
          assertTrue(readBloomFilter.mightContain(recordKey(i)), "bloom filter should contain " + recordKey(i));
        }
        assertFalse(readBloomFilter.mightContain("not-a-key"), "bloom filter should not contain an unknown key");
      }

      // The FileFormatUtils entry points read the same footers through vortex-jni.
      FileFormatUtils vortexUtils = HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(HoodieFileFormat.VORTEX);
      assertTrue(vortexUtils instanceof HoodieVortexFileFormatUtils, "the IO factory loads the vortex-jni utils");
      assertArrayEquals(expectedMinMax, vortexUtils.readMinMaxRecordKeys(storage, path), "min/max record keys");
      assertEquals(bloomFilter.serializeToString(),
          vortexUtils.readBloomFilterFromMetadata(storage, path).serializeToString(), "bloom filter content");
      assertEquals(numRows, vortexUtils.getRowCount(storage, path), "row count");
      assertThrows(MetadataNotFoundException.class,
          () -> vortexUtils.readFooter(storage, true, path, "no.such.key"));
      assertTrue(vortexUtils.readFooter(storage, false, path, "no.such.key").isEmpty());
    }
  }

  @Test
  public void testEmptyFileWithBloomFilter() throws Exception {
    SparkTaskContextSupplier taskContextSupplier = new SparkTaskContextSupplier();
    StructType schema = new StructType().add("id", DataTypes.LongType, false);
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/empty-footer.vortex");
    BloomFilter bloomFilter = BloomFilterFactory.createBloomFilter(
        1000, 0.000001, 10000, BloomFilterTypeCode.SIMPLE.name());

    try (HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.getAbsolutePath())) {
      HoodieSparkVortexWriter writer = HoodieSparkVortexWriter.builder()
          .file(path).sparkSchema(schema).instantTime("20251201120000000")
          .taskContextSupplier(taskContextSupplier).storage(storage).populateMetaFields(false)
          .bloomFilterOpt(Option.of(bloomFilter))
          .build();
      writer.close();

      // An empty file has a bloom filter but no min/max record keys.
      try (HoodieSparkVortexReader reader = new HoodieSparkVortexReader(path)) {
        assertEquals(0, reader.getTotalRecords(), "row count");

        BloomFilter readBloomFilter = reader.readBloomFilter();
        assertNotNull(readBloomFilter, "bloom filter");
        assertEquals(BloomFilterTypeCode.SIMPLE, readBloomFilter.getBloomFilterTypeCode(), "bloom filter type");
        assertThrows(HoodieException.class, reader::readMinMaxRecordKeys);
      }
    }
  }

  @Test
  public void testFileRollsAtCompressedSize() throws Exception {
    SparkTaskContextSupplier taskContextSupplier = new SparkTaskContextSupplier();
    StructType schema = new StructType()
        .add("value", DataTypes.DoubleType, false)
        .add("padding", DataTypes.StringType, false);
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/rolled.vortex");
    // The native writer compresses blocks of 8192 rows and uses a compression ratio of 1.0 until
    // the first block. Use a maximum file size that holds many blocks.
    long maxFileSize = 4 * 1024 * 1024;

    // The random doubles do not compress, and the constant padding compresses to almost nothing.
    // Thus the Arrow size of a row is much larger than its size in the file. If the size check
    // used the Arrow size, the writer would stop at a small part of the maximum file size.
    StringBuilder paddingBuilder = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      paddingBuilder.append('x');
    }
    UTF8String padding = UTF8String.fromString(paddingBuilder.toString());
    Random random = new Random(42);
    long maxRows = 10_000_000L;
    long rows = 0;

    try (HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.getAbsolutePath())) {
      try (HoodieSparkVortexWriter writer = HoodieSparkVortexWriter.builder()
          .file(path).sparkSchema(schema).instantTime("20251201120000000")
          .taskContextSupplier(taskContextSupplier).storage(storage).populateMetaFields(false)
          .maxFileSize(maxFileSize)
          .build()) {
        while (writer.canWrite() && rows < maxRows) {
          writer.writeRow(new GenericInternalRow(new Object[] {random.nextDouble(), padding}));
          rows++;
        }
      }

      assertTrue(rows < maxRows, "the writer should stop at the maximum file size");

      long fileSize = storage.getPathInfo(path).getLength();
      assertTrue(fileSize >= maxFileSize / 2,
          "file of " + fileSize + " bytes should be near the maximum file size of " + maxFileSize);
      assertTrue(fileSize <= maxFileSize + maxFileSize / 2,
          "file of " + fileSize + " bytes should not be much larger than the maximum file size of " + maxFileSize);
    }
  }

  private static String recordKey(int index) {
    return String.format("key%05d", index);
  }

  @Test
  public void testUnknownEditionFailsWrite() throws Exception {
    SparkTaskContextSupplier taskContextSupplier = new SparkTaskContextSupplier();
    StructType schema = new StructType().add("id", DataTypes.LongType, false);
    StoragePath path = new StoragePath(tempDir.getAbsolutePath() + "/unknown-edition.vortex");

    try (HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.getAbsolutePath())) {
      HoodieSparkVortexWriter writer = HoodieSparkVortexWriter.builder()
          .file(path).sparkSchema(schema).instantTime("20251201120000000")
          .taskContextSupplier(taskContextSupplier).storage(storage).populateMetaFields(false)
          .edition("core1999.01.0")
          .build();

      // The writer opens the native session lazily, so the failure can come from the first write
      // or from close(). Make sure that one of them fails and that the cause names the edition.
      Exception failure = assertThrows(Exception.class, () -> {
        try {
          writer.writeRow(new GenericInternalRow(new Object[] {1L}));
        } finally {
          writer.close();
        }
      });

      assertTrue(hasMessageInCauseChain(failure, "Unknown Vortex edition"),
          "Expected an unknown edition error, got: " + failure);
    }
  }

  private static boolean hasMessageInCauseChain(Throwable throwable, String text) {
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      if (t.getMessage() != null && t.getMessage().contains(text)) {
        return true;
      }

      for (Throwable suppressed : t.getSuppressed()) {
        if (hasMessageInCauseChain(suppressed, text)) {
          return true;
        }
      }
    }

    return false;
  }
}
