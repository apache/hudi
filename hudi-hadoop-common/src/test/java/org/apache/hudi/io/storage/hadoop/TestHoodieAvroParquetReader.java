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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.testutils.FooterKeyDecryptionFactory;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.HoodieAvroParquetReaderBuilder;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.HadoopDefaultResourceLoadCounter.countDefaultResourceLoads;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieAvroParquetReader}.
 */
class TestHoodieAvroParquetReader {

  private static final MessageType INTEGER_DECIMAL_TYPE = MessageTypeParser.parseMessageType("message m {\n"
      + "  required int64 id;\n"
      + "  optional int32 d32 (DECIMAL(9,2));\n"
      + "  optional int64 d64 (DECIMAL(15,2));\n"
      + "  optional group nested {\n"
      + "    optional int64 inner (DECIMAL(15,2));\n"
      + "  }\n"
      + "}");
  // Unscaled values per row; a null row leaves every decimal unset.
  private static final Integer[] UNSCALED_32 = {1700, -1, null, 999_999_999, 1700};
  private static final Long[] UNSCALED_64 = {1700L, -1L, null, 999_999_999_999_999L, 1700L};

  @TempDir
  java.nio.file.Path tmpDir;

  @ParameterizedTest
  @CsvSource({"fixed,true", "fixed,false", "bytes,true", "bytes,false", "file,true", "file,false"})
  void testReadIntegerBackedDecimals(String readerSchemaKind, boolean dictionaryEncoded) throws Exception {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    StoragePath parquetPath = new StoragePath(tmpDir.resolve("integer_decimals.parquet").toAbsolutePath().toString());
    writeIntegerDecimalFile(parquetPath, dictionaryEncoded);

    List<GenericRecord> records = new ArrayList<>();
    try (HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, parquetPath)) {
      HoodieSchema readerSchema = readerSchemaKind.equals("file") ? reader.getSchema() : integerDecimalReaderSchema(readerSchemaKind.equals("fixed"));
      try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(readerSchema)) {
        iterator.forEachRemaining(record -> records.add((GenericRecord) record));
      }
    }

    assertEquals(UNSCALED_64.length, records.size());
    for (int i = 0; i < records.size(); i++) {
      GenericRecord record = records.get(i);
      assertEquals((long) i, record.get("id"));
      if (UNSCALED_64[i] == null) {
        assertNull(record.get("d32"));
        assertNull(record.get("d64"));
        assertNull(record.get("nested"));
        continue;
      }
      assertEquals(BigDecimal.valueOf(UNSCALED_32[i], 2), decimalOf(record.get("d32"), 2));
      assertEquals(BigDecimal.valueOf(UNSCALED_64[i], 2), decimalOf(record.get("d64"), 2));
      assertEquals(BigDecimal.valueOf(UNSCALED_64[i], 2), decimalOf(((GenericRecord) record.get("nested")).get("inner"), 2));
    }
  }

  @Test
  void testIntegerBackedDecimalWithReorderedProjection() throws Exception {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    StoragePath parquetPath = new StoragePath(tmpDir.resolve("integer_decimals.parquet").toAbsolutePath().toString());
    writeIntegerDecimalFile(parquetPath, true);
    // d64 sits at a different position in the projection than in the file, so a converter keyed on the
    // parquet field index would write it into the wrong Avro field
    HoodieSchema projection = HoodieSchema.createRecord("m", null, null, false, Arrays.asList(
        HoodieSchemaField.of("d64", HoodieSchema.createNullable(HoodieSchema.createDecimal("d64", null, null, 15, 2, 7)), null, null),
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));

    List<GenericRecord> records = readAll(storage, parquetPath, projection);

    assertEquals(UNSCALED_64.length, records.size());
    for (int i = 0; i < records.size(); i++) {
      assertEquals((long) i, records.get(i).get("id"));
      if (UNSCALED_64[i] == null) {
        assertNull(records.get(i).get("d64"));
      } else {
        assertEquals(BigDecimal.valueOf(UNSCALED_64[i], 2), decimalOf(records.get(i).get("d64"), 2));
      }
    }
  }

  /**
   * A projection whose decimal differs from the file's, as the Hive reader builds from the table schema, is
   * re-scaled by the converter itself; this goes through the read support the way parquet-avro's reader does.
   */
  @Test
  void testIntegerBackedDecimalReadAtWiderPrecisionAndScale() throws Exception {
    StoragePath parquetPath = new StoragePath(tmpDir.resolve("integer_decimals.parquet").toAbsolutePath().toString());
    writeIntegerDecimalFile(parquetPath, true);
    HoodieSchema readerSchema = readerSchema(
        HoodieSchema.createDecimal("d32", null, null, 10, 2, 5),
        HoodieSchema.createDecimal("d64", null, null, 16, 3, 7),
        HoodieSchema.createDecimal("inner", null, null, 15, 2, 7));

    List<GenericRecord> records = readWithProjection(parquetPath, readerSchema);

    assertEquals(UNSCALED_64.length, records.size());
    for (int i = 0; i < records.size(); i++) {
      if (UNSCALED_64[i] == null) {
        continue;
      }
      assertEquals(BigDecimal.valueOf(UNSCALED_32[i], 2), decimalOf(records.get(i).get("d32"), 2));
      assertEquals(BigDecimal.valueOf(UNSCALED_64[i], 2).setScale(3), decimalOf(records.get(i).get("d64"), 3));
    }
  }

  @Test
  void testIntegerBackedDecimalReadAtNarrowerScaleOrPrecisionFails() throws Exception {
    StoragePath parquetPath = new StoragePath(tmpDir.resolve("integer_decimals.parquet").toAbsolutePath().toString());
    writeIntegerDecimalFile(parquetPath, true);
    HoodieSchema d32 = HoodieSchema.createDecimal("d32", null, null, 9, 2, 4);
    HoodieSchema inner = HoodieSchema.createDecimal("inner", null, null, 15, 2, 7);

    // -0.01 has no exact representation at scale 1
    Throwable narrowerScale = findCause(assertThrows(Throwable.class,
        () -> readWithProjection(parquetPath, readerSchema(d32, HoodieSchema.createDecimal("d64", null, null, 15, 1, 7), inner))),
        IllegalArgumentException.class);
    assertInstanceOf(IllegalArgumentException.class, narrowerScale, narrowerScale.toString());
    assertTrue(narrowerScale.getMessage().contains("d64") && narrowerScale.getMessage().contains("scale 1"), narrowerScale.getMessage());

    // 9999999999999.99 does not fit precision 5
    Throwable narrowerPrecision = findCause(assertThrows(Throwable.class,
        () -> readWithProjection(parquetPath, readerSchema(d32, HoodieSchema.createDecimal("d64", null, null, 5, 2, 3), inner))),
        AvroTypeException.class);
    assertInstanceOf(AvroTypeException.class, narrowerPrecision, narrowerPrecision.toString());
  }

  @Test
  void testIntegerBackedDecimalFileSchemaKeepsPrecisionAndScale() throws Exception {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    StoragePath parquetPath = new StoragePath(tmpDir.resolve("integer_decimals.parquet").toAbsolutePath().toString());
    writeIntegerDecimalFile(parquetPath, true);

    try (HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, parquetPath)) {
      HoodieSchema fileSchema = reader.getSchema();
      assertDecimal(fileSchema.getField("d32").get().schema().getNonNullType(), 9, 2, 4);
      assertDecimal(fileSchema.getField("d64").get().schema().getNonNullType(), 15, 2, 7);
      assertDecimal(fileSchema.getField("nested").get().schema().getNonNullType().getField("inner").get().schema().getNonNullType(), 15, 2, 7);
    }
  }

  @Test
  void testIntegerBackedDecimalInsideArrayIsRejected() throws Exception {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    StoragePath parquetPath = new StoragePath(tmpDir.resolve("decimal_array.parquet").toAbsolutePath().toString());
    MessageType type = MessageTypeParser.parseMessageType("message m {\n"
        + "  required int64 id;\n"
        + "  optional group prices (LIST) {\n"
        + "    repeated group list {\n"
        + "      optional int64 element (DECIMAL(15,2));\n"
        + "    }\n"
        + "  }\n"
        + "}");
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(parquetPath.toUri())).withType(type).withConf(new Configuration()).build()) {
      Group row = new SimpleGroupFactory(type).newGroup().append("id", 1L);
      row.addGroup("prices").addGroup("list").append("element", 1700L);
      writer.write(row);
    }
    HoodieSchema element = HoodieSchema.createNullable(HoodieSchema.createDecimal("element", null, null, 15, 2, 7));
    HoodieSchema readerSchema = HoodieSchema.createRecord("m", null, null, false, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("prices", HoodieSchema.createNullable(HoodieSchema.createArray(element)), null, null)));

    Throwable cause = findCause(assertThrows(Throwable.class, () -> readAll(storage, parquetPath, readerSchema)), UnsupportedOperationException.class);
    assertInstanceOf(UnsupportedOperationException.class, cause, cause.toString());
    assertTrue(cause.getMessage().contains("prices"), cause.getMessage());
  }

  private static List<GenericRecord> readAll(HoodieStorage storage, StoragePath path, HoodieSchema readerSchema) throws IOException {
    List<GenericRecord> records = new ArrayList<>();
    try (HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, path);
         ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(readerSchema)) {
      iterator.forEachRemaining(record -> records.add((GenericRecord) record));
    }
    return records;
  }

  @Test
  void testIntegerBackedDecimalWithoutReadSchemaBuildsDecimalRecords() throws Exception {
    StoragePath parquetPath = new StoragePath(tmpDir.resolve("integer_decimals.parquet").toAbsolutePath().toString());
    writeIntegerDecimalFile(parquetPath, true);

    // no read schema and no projection: the record schema must still carry the decimal the values are encoded as
    List<GenericRecord> records = readWithProjection(parquetPath, null);

    assertEquals(UNSCALED_64.length, records.size());
    HoodieSchema recordSchema = HoodieSchema.fromAvroSchema(records.get(0).getSchema());
    assertDecimal(recordSchema.getField("d64").get().schema().getNonNullType(), 15, 2, 7);
    assertEquals(BigDecimal.valueOf(UNSCALED_64[0], 2), decimalOf(records.get(0).get("d64"), 2));
    assertEquals(BigDecimal.valueOf(UNSCALED_32[0], 2), decimalOf(records.get(0).get("d32"), 2));
  }

  /** Reads through the read support with a requested projection (none when null), the way parquet-avro's record reader and Hive do. */
  private static List<GenericRecord> readWithProjection(StoragePath path, HoodieSchema projection) throws IOException {
    Configuration conf = new Configuration();
    if (projection != null) {
      AvroReadSupport.setAvroReadSchema(conf, projection.toAvroSchema());
      AvroReadSupport.setRequestedProjection(conf, projection.toAvroSchema());
    }
    List<GenericRecord> records = new ArrayList<>();
    try (ParquetReader<GenericRecord> reader = new HoodieAvroParquetReaderBuilder<GenericRecord>(
        HadoopInputFile.fromPath(new Path(path.toUri()), conf)).withConf(conf).build()) {
      for (GenericRecord record = reader.read(); record != null; record = reader.read()) {
        records.add(record);
      }
    }
    return records;
  }

  /** The first exception of the given type in the cause chain, or the failure itself when none matches. */
  private static <T extends Throwable> Throwable findCause(Throwable failure, Class<T> type) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause() == cause ? null : cause.getCause()) {
      if (type.isInstance(cause)) {
        return cause;
      }
    }
    return failure;
  }

  private static void writeIntegerDecimalFile(StoragePath path, boolean dictionaryEncoded) throws IOException {
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(path.toUri()))
        .withType(INTEGER_DECIMAL_TYPE).withDictionaryEncoding(dictionaryEncoded).withConf(new Configuration()).build()) {
      SimpleGroupFactory factory = new SimpleGroupFactory(INTEGER_DECIMAL_TYPE);
      for (int i = 0; i < UNSCALED_64.length; i++) {
        Group row = factory.newGroup().append("id", (long) i);
        if (UNSCALED_64[i] != null) {
          row.append("d32", UNSCALED_32[i]).append("d64", UNSCALED_64[i]);
          row.addGroup("nested").append("inner", UNSCALED_64[i]);
        }
        writer.write(row);
      }
    }
  }

  /** The schema a table built by Spark carries for this file: fixed decimals of the minimal width, or bytes decimals. */
  private static HoodieSchema integerDecimalReaderSchema(boolean fixed) {
    return readerSchema(
        fixed ? HoodieSchema.createDecimal("d32", null, null, 9, 2, 4) : HoodieSchema.createDecimal(9, 2),
        fixed ? HoodieSchema.createDecimal("d64", null, null, 15, 2, 7) : HoodieSchema.createDecimal(15, 2),
        fixed ? HoodieSchema.createDecimal("inner", null, null, 15, 2, 7) : HoodieSchema.createDecimal(15, 2));
  }

  private static HoodieSchema readerSchema(HoodieSchema d32, HoodieSchema d64, HoodieSchema inner) {
    HoodieSchema nested = HoodieSchema.createRecord("nested", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("inner", HoodieSchema.createNullable(inner), null, null)));
    return HoodieSchema.createRecord("m", null, null, false, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("d32", HoodieSchema.createNullable(d32), null, null),
        HoodieSchemaField.of("d64", HoodieSchema.createNullable(d64), null, null),
        HoodieSchemaField.of("nested", HoodieSchema.createNullable(nested), null, null)));
  }

  private static BigDecimal decimalOf(Object value, int scale) {
    byte[] bytes = value instanceof GenericFixed ? ((GenericFixed) value).bytes() : ((ByteBuffer) value).array();
    return new BigDecimal(new BigInteger(bytes), scale);
  }

  private static void assertDecimal(HoodieSchema schema, int precision, int scale, int fixedSize) {
    HoodieSchema.Decimal decimal = assertInstanceOf(HoodieSchema.Decimal.class, schema);
    assertEquals(precision, decimal.getPrecision());
    assertEquals(scale, decimal.getScale());
    assertTrue(decimal.isFixed());
    assertEquals(fixedSize, decimal.getFixedSize());
  }

  @Test
  void testReadDoesNotLoadHadoopDefaultResources() throws Throwable {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    StoragePath parquetPath = new StoragePath(tmpDir.resolve("file.parquet").toAbsolutePath().toString());
    List<GenericRecord> records = new HoodieTestDataGenerator(0xDEED).generateGenericRecords(20);
    HoodieSchema schema = HoodieSchema.fromAvroSchema(records.get(0).getSchema());
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, "uncompressed");
    HoodieAvroParquetWriter writer = (HoodieAvroParquetWriter) new HoodieAvroFileWriterFactory(storage)
        .newParquetFileWriter("001", parquetPath, config, schema, new LocalTaskContextSupplier());
    for (GenericRecord record : records) {
      writer.writeAvro((String) record.get("_row_key"), record);
    }
    writer.close();

    List<String> readKeys = new ArrayList<>();
    int loads = countDefaultResourceLoads(() -> {
      try (HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, parquetPath);
           ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(schema)) {
        iterator.forEachRemaining(record -> readKeys.add(((GenericRecord) record).get("_row_key").toString()));
      }
    });
    assertEquals(records.stream().map(record -> record.get("_row_key").toString()).collect(Collectors.toList()), readKeys);
    assertEquals(0, loads, "Reading a parquet file must reuse the storage configuration");
  }

  @Test
  void testReadEncryptedFileResolvesDecryptionWithFilePath() throws Exception {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    storage.getConf().set(FooterKeyDecryptionFactory.CRYPTO_FACTORY_CLASS, FooterKeyDecryptionFactory.class.getName());
    StoragePath parquetPath = new StoragePath(tmpDir.resolve("encrypted.parquet").toAbsolutePath().toString());
    FooterKeyDecryptionFactory.writeEncryptedFile(storage.getConf().unwrapAs(Configuration.class), new Path(parquetPath.toUri()), 10);
    FooterKeyDecryptionFactory.drainRequestedPaths();

    List<String> readKeys = new ArrayList<>();
    try (HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, parquetPath);
         ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(HoodieSchema.fromAvroSchema(FooterKeyDecryptionFactory.SCHEMA))) {
      iterator.forEachRemaining(record -> readKeys.add(((GenericRecord) record).get("_row_key").toString()));
    }
    assertEquals(IntStream.range(0, 10).mapToObj(i -> "key" + i).collect(Collectors.toList()), readKeys);
    List<Path> requestedPaths = FooterKeyDecryptionFactory.drainRequestedPaths();
    assertFalse(requestedPaths.isEmpty());
    requestedPaths.forEach(requested -> assertEquals("encrypted.parquet", requested == null ? null : requested.getName()));
  }
}
