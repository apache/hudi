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

package org.apache.hudi.io.hadoop;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Public writer/file API checks; no private writer state or production test hooks. Large blob rows exercise the bounds the internal
 * settings give a writer of large values; small deterministic rows check rollover, the every-record size check and physical page
 * splits by readback.
 */
public class TestHoodieBaseParquetWriterPageBounds {
  private static final int PAGE_TARGET = 1048576;
  private static final int GROUP_TARGET = 8388608;
  private static final int INLINE_THRESHOLD = 4194304;
  private static final int ENCODING_ALLOWANCE = 65536;
  private static final int FILE_TARGET = 8388608;
  /** Rows written by the default-cadence check: fewer than Parquet's first file-size check at 100 records. */
  private static final int CADENCE_ROWS = 8;
  /** The page target of the small-row checks: a few 256-byte rows fill a page. */
  private static final int TINY_PAGE_TARGET = 512;

  @TempDir
  Path tempDir;

  /**
   * A key asking for no statistics on one column is honoured where Parquet has the per-column switch (1.15+, the Spark 4
   * builds) and refused where it has none (the Spark 3.x builds), so no caller believes they are off when they are not;
   * a key asking for them is accepted either way. Runs on both, never skips.
   */
  @Test
  void statisticsOffNeedsTheSwitch() throws Exception {
    Schema avro = blobSchema();
    Configuration off = new Configuration();
    off.setBoolean("hoodie.parquet.internal.column.statistics.enabled#blob.data", false);
    StoragePath offPath = new StoragePath(tempDir.resolve("off.parquet").toString());
    if (HoodieBaseParquetWriter.supportsColumnStatisticsSwitch()) {
      try (HoodieAvroParquetWriter writer = new HoodieAvroParquetWriter(offPath, writerConfig(avro, off, FILE_TARGET), "001",
          new LocalTaskContextSupplier(), false)) {
        writer.writeAvro("0", row(avro, 0, 16, false));
      }
      assertFalse(blobDataHasMinMax(tempDir.resolve("off.parquet"), off), "the key turns blob.data's statistics off");
    } else {
      HoodieParquetConfig<HoodieAvroWriteSupport> offConfig = writerConfig(avro, off, FILE_TARGET);
      LocalTaskContextSupplier supplier = new LocalTaskContextSupplier();
      assertThrows(HoodieNotSupportedException.class, () -> new HoodieAvroParquetWriter(offPath, offConfig, "001", supplier, false));
    }
    Configuration on = new Configuration();
    on.setBoolean("hoodie.parquet.internal.column.statistics.enabled#blob.data", true);
    Path acceptedOutput = tempDir.resolve("accepted.parquet");
    try (HoodieAvroParquetWriter writer = new HoodieAvroParquetWriter(new StoragePath(acceptedOutput.toString()),
        writerConfig(avro, on, FILE_TARGET), "001", new LocalTaskContextSupplier(), false)) {
      writer.writeAvro("0", row(avro, 0, 16, false));
    }
    assertTrue(blobDataHasMinMax(acceptedOutput, on), "a key asking for statistics keeps blob.data's statistics");
  }

  /** A per-column dictionary key writes that column PLAIN while the file keeps its dictionary for the others. */
  @Test
  void perColumnDictionaryKeyWritesOnlyThatColumnPlain() throws Exception {
    // control: with the file's dictionary on and no key, repeated blob values are dictionary encoded
    assertTrue(blobDataUsesDictionary("dictionary-control", new Configuration()));
    Configuration conf = new Configuration();
    conf.setBoolean("hoodie.parquet.internal.column.dictionary.enabled#blob.data", false);
    assertFalse(blobDataUsesDictionary("dictionary-off", conf));
  }

  /** Writes 16 rows whose blob values repeat, with the file's dictionary on; whether blob.data was dictionary encoded. */
  private boolean blobDataUsesDictionary(String name, Configuration conf) throws Exception {
    Schema avro = blobSchema();
    Path output = tempDir.resolve(name + ".parquet");
    HoodieAvroWriteSupport support = new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(avro),
        HoodieSchema.fromAvroSchema(avro), Option.empty(), new Properties());
    HoodieParquetConfig<HoodieAvroWriteSupport> config = new HoodieParquetConfig<>(support, CompressionCodecName.UNCOMPRESSED,
        GROUP_TARGET, PAGE_TARGET, FILE_TARGET, new HadoopStorageConfiguration(conf), 0.1, true);
    try (HoodieAvroParquetWriter writer = new HoodieAvroParquetWriter(new StoragePath(output.toString()), config, "001",
        new LocalTaskContextSupplier(), false)) {
      for (int index = 0; index < 16; index++) {
        writer.writeAvro(Integer.toString(index), row(avro, index % 2, 1024, false));
      }
    }
    boolean blobDictionary = false;
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(output.toUri()), conf))) {
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          boolean dictionary = column.getEncodings().contains(Encoding.RLE_DICTIONARY) || column.getEncodings().contains(Encoding.PLAIN_DICTIONARY);
          if ("blob.data".equals(column.getPath().toDotString())) {
            blobDictionary = dictionary;
          } else if ("fid".equals(column.getPath().toDotString())) {
            assertTrue(dictionary, "the other columns keep the file's dictionary: " + column.getEncodings());
          }
        }
      }
    }
    return blobDictionary;
  }

  /**
   * Parquet's generic per-column and page-check keys in the Hadoop configuration leave an ordinary Hudi writer as upstream
   * builds it: the column keeps the file's dictionary, the pages keep Parquet's default check cadence, and a generic key asking
   * for no statistics is not refused.
   */
  @Test
  void genericParquetKeysLeaveAnOrdinaryWriterUnchanged() throws Exception {
    Configuration dictionary = new Configuration();
    dictionary.setBoolean("parquet.enable.dictionary#blob.data", false);
    assertTrue(blobDataUsesDictionary("generic-dictionary", dictionary), "the generic dictionary key is not honoured");

    Configuration cadence = new Configuration();
    cadence.setInt("parquet.page.size.row.check.min", 1);
    cadence.setInt("parquet.page.size.row.check.max", 1);
    assertPhysicalPages("generic-cadence", cadence, false);

    Schema avro = blobSchema();
    Configuration statistics = new Configuration();
    statistics.setBoolean("parquet.column.statistics.enabled#blob.data", false);
    Path statisticsOutput = tempDir.resolve("generic-statistics.parquet");
    try (HoodieAvroParquetWriter writer = new HoodieAvroParquetWriter(new StoragePath(statisticsOutput.toString()),
        writerConfig(avro, statistics, FILE_TARGET), "001", new LocalTaskContextSupplier(), false)) {
      writer.writeAvro("0", row(avro, 0, 16, false));
    }
    assertTrue(blobDataHasMinMax(statisticsOutput, statistics), "the generic statistics key leaves blob.data's statistics on");
  }

  /** Whether every row group of {@code output} keeps min/max statistics for blob.data. */
  private static boolean blobDataHasMinMax(Path output, Configuration conf) throws Exception {
    boolean found = false;
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(output.toUri()), conf))) {
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          if ("blob.data".equals(column.getPath().toDotString())) {
            if (column.getStatistics() == null || !column.getStatistics().hasNonNullValue()) {
              return false;
            }
            found = true;
          }
        }
      }
    }
    return found;
  }

  @Test
  void ordinaryWriterKeepsDefaultPageCheckCadence() throws Exception {
    writeAndAssert("ordinary", 65536, 16, false, false);
    assertPhysicalPages("ordinary-pages", new Configuration(), false);
  }

  @Test
  void configuredPageCheckCadenceSplitsPhysicalPages() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt("hoodie.parquet.internal.page.size.row.check.min", 1);
    conf.setInt("hoodie.parquet.internal.page.size.row.check.max", 1);
    assertPhysicalPages("configured-pages", conf, true);
  }

  @Test
  void everyRecordSizeCheckRollsAfterOneRow() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(HoodieBaseParquetWriter.INTERNAL_SIZE_CHECK_EVERY_RECORD, true);
    assertRollover("every-record", conf, 1L, 64, 1);
  }

  @Test
  void smallRowsFitWithinBound() throws Exception {
    writeAndAssert("small", 4096, 16, true, false);
  }

  @Test
  void tinyCallerFileCapStillAllowsOneWholeRow() throws Exception {
    assertRollover("tiny-file", writerConf(true), 1L, 4160000, 1);
  }

  @Test
  void ordinaryWriterKeepsDefaultFileCheckCadence() throws Exception {
    Schema avro = blobSchema();
    Path output = tempDir.resolve("ordinary-cadence.parquet");
    Configuration conf = writerConf(false);
    try (HoodieAvroParquetWriter writer = new HoodieAvroParquetWriter(new StoragePath(output.toString()),
        writerConfig(avro, conf, 1L), "001", new LocalTaskContextSupplier(), false)) {
      for (int index = 0; index < CADENCE_ROWS; index++) {
        assertTrue(writer.canWrite());
        writer.writeAvro(Integer.toString(index), row(avro, index, 1, true));
      }
      assertTrue(writer.canWrite(), "without the every-record key the default file-check cadence is kept");
    }
    assertRows(output, conf, CADENCE_ROWS, 1, true);
  }

  /** Writes {@code expectedRows} rows of {@code bytes} and checks the writer rolls over exactly then; reads the rows back. */
  private void assertRollover(String name, Configuration conf, long fileTarget, int bytes, int expectedRows) throws Exception {
    Schema avro = blobSchema();
    Path output = tempDir.resolve(name + ".parquet");
    HoodieParquetConfig<HoodieAvroWriteSupport> config = writerConfig(avro, conf, fileTarget);
    try (HoodieAvroParquetWriter writer = new HoodieAvroParquetWriter(new StoragePath(output.toString()),
        config, "001", new LocalTaskContextSupplier(), false)) {
      for (int index = 0; index < expectedRows; index++) {
        assertTrue(writer.canWrite(), "first whole row must make progress");
        writer.writeAvro(Integer.toString(index), row(avro, index, bytes, true));
      }
      assertFalse(writer.canWrite(), "file must roll over before next whole row");
      assertFalse(writer.canWrite(), "repeated checks must preserve the boundary");
    }
    assertRows(output, conf, expectedRows, bytes, true);
  }

  /** Writes 16 rows of 256 bytes at a 512-byte page target and counts the blob column's physical pages. */
  private void assertPhysicalPages(String name, Configuration conf, boolean configured) throws Exception {
    Schema avro = blobSchema();
    Path output = tempDir.resolve(name + ".parquet");
    HoodieAvroWriteSupport support = new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(avro),
        HoodieSchema.fromAvroSchema(avro), Option.empty(), new Properties());
    HoodieParquetConfig<HoodieAvroWriteSupport> config = new HoodieParquetConfig<>(support, CompressionCodecName.UNCOMPRESSED,
        1024 * 1024, TINY_PAGE_TARGET, 1024 * 1024, new HadoopStorageConfiguration(conf), 0, false);
    try (HoodieAvroParquetWriter writer = new HoodieAvroParquetWriter(new StoragePath(output.toString()), config, "001",
        new LocalTaskContextSupplier(), false)) {
      for (int index = 0; index < 16; index++) {
        assertTrue(writer.canWrite());
        writer.writeAvro(Integer.toString(index), row(avro, index, 256, false));
      }
    }
    int pages = 0;
    int values = 0;
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(output.toUri()), conf))) {
      ColumnDescriptor blobColumn = reader.getFooter().getFileMetaData().getSchema().getColumns().stream()
          .filter(column -> "blob.data".equals(String.join(".", column.getPath()))).findFirst().get();
      PageReadStore group;
      while ((group = reader.readNextRowGroup()) != null) {
        PageReader pageReader = group.getPageReader(blobColumn);
        DataPage page;
        while ((page = pageReader.readPage()) != null) {
          pages++;
          values += page.getValueCount();
          if (configured) {
            assertTrue(page.getUncompressedSize() <= TINY_PAGE_TARGET + 256 + 64, "page allows at most one whole-row overshoot");
          } else {
            assertTrue(page.getUncompressedSize() > TINY_PAGE_TARGET, "ordinary pages retain the default check cadence");
          }
        }
      }
    }
    assertEquals(16, values);
    if (configured) {
      assertTrue(pages > 1, "explicit cadence must split the physical blob pages");
    } else {
      assertEquals(1, pages);
    }
    assertRows(output, conf, 16, 256, false);
  }

  /** Reads {@code rows} rows back and checks each whole blob row of {@code bytes} (zeros when compressible, else seeded). */
  private static void assertRows(Path output, Configuration conf, int rows, int bytes, boolean compressible) throws Exception {
    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new org.apache.hadoop.fs.Path(output.toUri())).withConf(conf).build()) {
      for (int index = 0; index < rows; index++) {
        GenericRecord actual = reader.read();
        assertEquals(Integer.toString(index), actual.get("fid").toString());
        GenericRecord blob = (GenericRecord) actual.get("blob");
        assertEquals("INLINE", blob.get("type").toString());
        assertNull(blob.get("reference"));
        ByteBuffer actualBytes = ((ByteBuffer) blob.get("data")).duplicate();
        byte[] restored = new byte[actualBytes.remaining()];
        actualBytes.get(restored);
        byte[] expected = new byte[bytes];
        if (!compressible) {
          new Random(index).nextBytes(expected);
        }
        assertArrayEquals(expected, restored, "whole row " + index);
      }
      assertNull(reader.read());
    }
  }

  private void writeAndAssert(String name, int bytes, int rows, boolean checks, boolean compressible) throws Exception {
    writeAndAssert(name, bytes, rows, checks, compressible, GROUP_TARGET);
  }

  private void writeAndAssert(String name, int bytes, int rows, boolean checks, boolean compressible, int groupTarget) throws Exception {
    Schema avro = blobSchema();
    Configuration conf = writerConf(checks);
    long totalRows = 0;
    long totalValues = 0;
    long maxPage = 0;
    long maxGroup = 0;
    long maxWriterDataSize = 0;
    long maxFooterStatisticsBytes = 0;
    long maxFileBytes = 0;
    int pages = 0;
    int groups = 0;
    int files = 0;
    int writtenRows = 0;
    int maxRowsPerFile = 0;
    while (writtenRows < rows) {
      Path output = tempDir.resolve(name + "-" + files + ".parquet");
      int fileRows = 0;
      HoodieParquetConfig<HoodieAvroWriteSupport> config = writerConfig(avro, conf, checks ? FILE_TARGET : 1073741824L, groupTarget);
      HoodieAvroParquetWriter writer = new HoodieAvroParquetWriter(new StoragePath(output.toString()), config,
          "001", new LocalTaskContextSupplier(), false);
      try (HoodieAvroParquetWriter closeableWriter = writer) {
        assertTrue(writer.canWrite(), "empty handle must take one whole row");
        while (writtenRows < rows && writer.canWrite()) {
          writer.writeAvro(Integer.toString(writtenRows), row(avro, writtenRows, bytes, compressible));
          writtenRows++;
          fileRows++;
          maxWriterDataSize = Math.max(maxWriterDataSize, writer.getDataSize());
          if (checks) {
            assertTrue(writer.getDataSize() <= (long) FILE_TARGET + bytes + ENCODING_ALLOWANCE,
                "public writerDataSize=" + writer.getDataSize());
          }
        }
      }
      // The public in-memory footer retains full values; file serialization truncates only the on-disk statistics.
      long footerStatisticsBytes = 0;
      int fileGroups = ((ParquetMetadata) writer.getFileFormatMetadata()).getBlocks().size();
      for (BlockMetaData block : ((ParquetMetadata) writer.getFileFormatMetadata()).getBlocks()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          if (column.getStatistics().hasNonNullValue()) {
            footerStatisticsBytes += column.getStatistics().getMinBytes().length + column.getStatistics().getMaxBytes().length;
          }
        }
      }
      maxFooterStatisticsBytes = Math.max(maxFooterStatisticsBytes, footerStatisticsBytes);
      if (checks) {
        // per row group: a min and a max of at most one value per column
        assertTrue(footerStatisticsBytes <= 2L * fileGroups * (bytes + ENCODING_ALLOWANCE),
            "live full footer statistics=" + footerStatisticsBytes);
      }
      maxRowsPerFile = Math.max(maxRowsPerFile, fileRows);
      maxFileBytes = Math.max(maxFileBytes, java.nio.file.Files.size(output));
      if (checks) {
        assertTrue(java.nio.file.Files.size(output) <= (long) FILE_TARGET + bytes + ENCODING_ALLOWANCE, "physical file overshoot");
      }
      try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(output.toUri()), conf))) {
        ColumnDescriptor blobColumn = reader.getFooter().getFileMetaData().getSchema().getColumns().stream()
            .filter(column -> "blob.data".equals(String.join(".", column.getPath()))).findFirst().get();
        for (BlockMetaData block : reader.getFooter().getBlocks()) {
          groups++;
          maxGroup = Math.max(maxGroup, compressible ? block.getCompressedSize() : block.getTotalByteSize());
          if (checks) {
            assertTrue((compressible ? block.getCompressedSize() : block.getTotalByteSize())
                <= (long) groupTarget + bytes + ENCODING_ALLOWANCE, "row group bytes=" + block.getTotalByteSize());
          }
          for (ColumnChunkMetaData column : block.getColumns()) {
            assertFalse(column.getEncodings().contains(Encoding.RLE_DICTIONARY));
            assertFalse(column.getEncodings().contains(Encoding.PLAIN_DICTIONARY));
          }
        }
        PageReadStore group;
        while ((group = reader.readNextRowGroup()) != null) {
          totalRows += group.getRowCount();
          PageReader pageReader = group.getPageReader(blobColumn);
          totalValues += pageReader.getTotalValueCount();
          DataPage page;
          while ((page = pageReader.readPage()) != null) {
            pages++;
            maxPage = Math.max(maxPage, page.getUncompressedSize());
            if (checks) {
              long bound = bytes > PAGE_TARGET ? INLINE_THRESHOLD + ENCODING_ALLOWANCE : (long) PAGE_TARGET + bytes + ENCODING_ALLOWANCE;
              assertTrue(page.getUncompressedSize() <= bound, "blob page bytes=" + page.getUncompressedSize() + " bound=" + bound);
            }
          }
        }
      }
      int readIndex = writtenRows - fileRows;
      try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new org.apache.hadoop.fs.Path(output.toUri())).withConf(conf).build()) {
        GenericRecord actual;
        while ((actual = reader.read()) != null) {
          assertEquals(Integer.toString(readIndex), actual.get("fid").toString());
          GenericRecord actualBlob = (GenericRecord) actual.get("blob");
          assertEquals("INLINE", actualBlob.get("type").toString());
          ByteBuffer actualBytes = ((ByteBuffer) actualBlob.get("data")).duplicate();
          byte[] restored = new byte[actualBytes.remaining()];
          actualBytes.get(restored);
          byte[] expected = new byte[bytes];
          if (!compressible) {
            new Random(readIndex).nextBytes(expected);
          }
          assertArrayEquals(expected, restored, "whole blob row after file rollover");
          readIndex++;
        }
      }
      assertEquals(writtenRows, readIndex);
      files++;
    }
    assertEquals(rows, totalRows);
    assertEquals(rows, totalValues);
    if (!checks) {
      assertEquals(1, files);
      // Default minimum100 keeps all16rows together; unconditional checks would split this page.
      assertEquals(1, pages);
      assertTrue(maxPage > PAGE_TARGET);
    }
    System.out.println("WRITER_BOUND case=" + name + " rows=" + rows + " files=" + files + " maxRowsPerFile=" + maxRowsPerFile
        + " pages=" + pages + " groups=" + groups + " maxPhysicalPage=" + maxPage + " maxPhysicalGroup=" + maxGroup
        + " maxPublicWriterDataSize=" + maxWriterDataSize + " maxLiveFooterStatisticsBytes=" + maxFooterStatisticsBytes
        + " maxPhysicalFileBytes=" + maxFileBytes);
  }

  private static Schema blobSchema() {
    return SchemaBuilder.record("WriterPageBound").fields().requiredString("fid")
        .name("blob").type(HoodieSchema.createBlob().toAvroSchema()).noDefault().endRecord();
  }

  private static Configuration writerConf(boolean checks) {
    Configuration conf = new Configuration();
    if (checks) {
      conf.setInt("hoodie.parquet.internal.page.size.row.check.min", 1);
      conf.setInt("hoodie.parquet.internal.page.size.row.check.max", 1);
      conf.setBoolean(HoodieBaseParquetWriter.INTERNAL_SIZE_CHECK_EVERY_RECORD, true);
    }
    return conf;
  }

  private static HoodieParquetConfig<HoodieAvroWriteSupport> writerConfig(Schema avro, Configuration conf, long fileTarget) {
    return writerConfig(avro, conf, fileTarget, GROUP_TARGET);
  }

  private static HoodieParquetConfig<HoodieAvroWriteSupport> writerConfig(Schema avro, Configuration conf, long fileTarget, int groupTarget) {
    HoodieAvroWriteSupport support = new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(avro),
        HoodieSchema.fromAvroSchema(avro), Option.empty(), new Properties());
    return new HoodieParquetConfig<>(support, CompressionCodecName.GZIP, groupTarget, PAGE_TARGET,
        fileTarget, new HadoopStorageConfiguration(conf), 0.1, false);
  }

  private static GenericRecord row(Schema avro, int index, int bytes, boolean compressible) {
    byte[] data = new byte[bytes];
    if (!compressible) {
      new Random(index).nextBytes(data);
    }
    GenericRecord blob = new GenericData.Record(avro.getField("blob").schema());
    blob.put("type", "INLINE");
    blob.put("data", ByteBuffer.wrap(data));
    blob.put("reference", null);
    GenericRecord row = new GenericData.Record(avro);
    row.put("fid", Integer.toString(index));
    row.put("blob", blob);
    return row;
  }
}
