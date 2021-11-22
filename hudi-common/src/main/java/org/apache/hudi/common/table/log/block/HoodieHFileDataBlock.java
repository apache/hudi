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

package org.apache.hudi.common.table.log.block;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.inline.InLineFSUtils;
import org.apache.hudi.common.fs.inline.InLineFileSystem;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieHBaseKVComparator;
import org.apache.hudi.io.storage.HoodieHFileReader;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * HoodieHFileDataBlock contains a list of records stored inside an HFile format. It is used with the HFile
 * base file format.
 */
public class HoodieHFileDataBlock extends HoodieDataBlock {
  private static final Logger LOG = LogManager.getLogger(HoodieHFileDataBlock.class);
  private static Compression.Algorithm compressionAlgorithm = Compression.Algorithm.GZ;
  private static int blockSize = 1 * 1024 * 1024;
  private boolean enableInlineReading = false;

  public HoodieHFileDataBlock(HoodieLogFile logFile, FSDataInputStream inputStream, Option<byte[]> content,
                              boolean readBlockLazily, long position, long blockSize, long blockEndpos,
                              Schema readerSchema, Map<HeaderMetadataType, String> header,
                              Map<HeaderMetadataType, String> footer, boolean enableInlineReading, String keyField) {
    super(content, inputStream, readBlockLazily,
        Option.of(new HoodieLogBlockContentLocation(logFile, position, blockSize, blockEndpos)),
        readerSchema, header, footer, keyField);
    this.enableInlineReading = enableInlineReading;
  }

  public HoodieHFileDataBlock(@Nonnull List<IndexedRecord> records, @Nonnull Map<HeaderMetadataType, String> header,
                              String keyField) {
    super(records, header, new HashMap<>(), keyField);
  }

  public HoodieHFileDataBlock(@Nonnull List<IndexedRecord> records, @Nonnull Map<HeaderMetadataType, String> header) {
    this(records, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.HFILE_DATA_BLOCK;
  }

  @Override
  protected byte[] serializeRecords() throws IOException {
    HFileContext context = new HFileContextBuilder().withBlockSize(blockSize).withCompression(compressionAlgorithm)
        .build();
    Configuration conf = new Configuration();
    CacheConfig cacheConfig = new CacheConfig(conf);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream ostream = new FSDataOutputStream(baos, null);

    HFile.Writer writer = HFile.getWriterFactory(conf, cacheConfig)
        .withOutputStream(ostream).withFileContext(context).withComparator(new HoodieHBaseKVComparator()).create();

    // Serialize records into bytes, sort them and write to HFile
    Map<String, byte[]> sortedRecordsMap = new TreeMap<>();
    Iterator<IndexedRecord> itr = records.iterator();
    boolean useIntegerKey = false;
    int key = 0;
    int keySize = 0;

    // Build the record key
    final Field schemaKeyField = records.get(0).getSchema().getField(this.keyField);
    if (schemaKeyField == null) {
      // Missing key metadata field. Use an integer sequence key instead.
      useIntegerKey = true;
      keySize = (int) Math.ceil(Math.log(records.size())) + 1;
    }

    while (itr.hasNext()) {
      IndexedRecord record = itr.next();
      String recordKey;
      if (useIntegerKey) {
        recordKey = String.format("%" + keySize + "s", key++);
      } else {
        recordKey = record.get(schemaKeyField.pos()).toString();
      }
      final byte[] recordBytes = serializeRecord(record, Option.ofNullable(schemaKeyField)).array();
      ValidationUtils.checkState(!sortedRecordsMap.containsKey(recordKey),
          "Writing multiple records with same key not supported for " + this.getClass().getName());
      sortedRecordsMap.put(recordKey, recordBytes);
    }

    // Write the records
    sortedRecordsMap.forEach((recordKey, recordBytes) -> {
      try {
        KeyValue kv = new KeyValue(recordKey.getBytes(), null, null, recordBytes);
        writer.append(kv);
      } catch (IOException e) {
        throw new HoodieIOException("IOException serializing records", e);
      }
    });

    writer.close();
    ostream.flush();
    ostream.close();

    return baos.toByteArray();
  }

  @Override
  protected void createRecordsFromContentBytes() throws IOException {
    if (enableInlineReading) {
      getRecords(Collections.emptyList());
    } else {
      super.createRecordsFromContentBytes();
    }
  }

  @Override
  public List<IndexedRecord> getRecords(List<String> keys) throws IOException {
    readWithInlineFS(keys);
    return records;
  }

  /**
   * Serialize the record to byte buffer.
   *
   * @param record         - Record to serialize
   * @param schemaKeyField - Key field in the schema
   * @return Serialized byte buffer for the record
   */
  protected ByteBuffer serializeRecord(final IndexedRecord record, final Option<Field> schemaKeyField) {
    ValidationUtils.checkArgument(record.getSchema() != null, "Unknown schema for the record!");
    return ByteBuffer.wrap(HoodieAvroUtils.indexedRecordToBytes(record));
  }

  private void readWithInlineFS(List<String> keys) throws IOException {
    boolean enableFullScan = keys.isEmpty();
    // Get schema from the header
    Schema writerSchema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
    // If readerSchema was not present, use writerSchema
    if (schema == null) {
      schema = writerSchema;
    }
    Configuration conf = new Configuration();
    CacheConfig cacheConf = new CacheConfig(conf);
    Configuration inlineConf = new Configuration();
    inlineConf.set("fs." + InLineFileSystem.SCHEME + ".impl", InLineFileSystem.class.getName());

    Path inlinePath = InLineFSUtils.getInlineFilePath(
        getBlockContentLocation().get().getLogFile().getPath(),
        getBlockContentLocation().get().getLogFile().getPath().getFileSystem(conf).getScheme(),
        getBlockContentLocation().get().getContentPositionInLogFile(),
        getBlockContentLocation().get().getBlockSize());
    if (!enableFullScan) {
      // HFile read will be efficient if keys are sorted, since on storage, records are sorted by key. This will avoid unnecessary seeks.
      Collections.sort(keys);
    }

    HoodieHFileReader reader = createHFileReader(inlineConf,
        inlinePath, cacheConf, inlinePath.getFileSystem(inlineConf), keyField);
    List<org.apache.hadoop.hbase.util.Pair<String, IndexedRecord>> logRecords = enableFullScan
        ? reader.readAllRecords(writerSchema, schema)
        : reader.readRecords(keys, schema);
    reader.close();
    this.records = logRecords.stream().map(t -> t.getSecond()).collect(Collectors.toList());
  }

  @Override
  protected void deserializeRecords() throws IOException {
    // Get schema from the header
    Schema writerSchema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    // If readerSchema was not present, use writerSchema
    if (schema == null) {
      schema = writerSchema;
    }

    // Read the content
    HoodieHFileReader<IndexedRecord> reader = createHFileReader(getContent().get(), this.keyField);
    List<Pair<String, IndexedRecord>> records = reader.readAllRecords(writerSchema, schema);
    this.records = records.stream().map(t -> t.getSecond()).collect(Collectors.toList());

    // Free up content to be GC'd, deflate
    deflate();
  }

  /**
   * Create a new HFile reader for the serialized log block content byte array.
   *
   * @param content  - Log block serialized content byte array
   * @param keyField - Key field in the schema
   * @return New HFile reader for the serialized log block content
   * @throws IOException
   */
  protected HoodieHFileReader<IndexedRecord> createHFileReader(final byte[] content,
                                                               final String keyField) throws IOException {
    return new HoodieHFileReader<>(content, keyField);
  }

  /**
   * Create a new HFile reader for the log file.
   *
   * @param inlineConf - Common configuration
   * @param inlinePath - Log file path
   * @param cacheConf  - Cache configuration
   * @param fileSystem - Filesystem for the log file
   * @param keyField   - Key field in the schema
   * @return New HFile reader instance for the log file
   * @throws IOException
   */
  protected HoodieHFileReader<IndexedRecord> createHFileReader(Configuration inlineConf, Path inlinePath,
                                                               CacheConfig cacheConf, FileSystem fileSystem,
                                                               String keyField) throws IOException {
    return new HoodieHFileReader<>(inlineConf, inlinePath, cacheConf, fileSystem, keyField);
  }

}
