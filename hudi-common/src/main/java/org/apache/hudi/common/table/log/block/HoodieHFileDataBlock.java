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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.inline.InLineFSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieAvroHFileReader;
import org.apache.hudi.io.storage.HoodieHBaseKVComparator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * HoodieHFileDataBlock contains a list of records stored inside an HFile format. It is used with the HFile
 * base file format.
 */
public class HoodieHFileDataBlock extends HoodieDataBlock {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieHFileDataBlock.class);
  private static final int DEFAULT_BLOCK_SIZE = 1024 * 1024;

  private final Option<Compression.Algorithm> compressionAlgorithm;
  // This path is used for constructing HFile reader context, which should not be
  // interpreted as the actual file path for the HFile data blocks
  private final Path pathForReader;

  public HoodieHFileDataBlock(FSDataInputStream inputStream,
                              Option<byte[]> content,
                              boolean readBlockLazily,
                              HoodieLogBlockContentLocation logBlockContentLocation,
                              Option<Schema> readerSchema,
                              Map<HeaderMetadataType, String> header,
                              Map<HeaderMetadataType, String> footer,
                              boolean enablePointLookups,
                              Path pathForReader) {
    super(content, inputStream, readBlockLazily, Option.of(logBlockContentLocation), readerSchema, header, footer, HoodieAvroHFileReader.KEY_FIELD_NAME, enablePointLookups);
    this.compressionAlgorithm = Option.empty();
    this.pathForReader = pathForReader;
  }

  public HoodieHFileDataBlock(List<HoodieRecord> records,
                              Map<HeaderMetadataType, String> header,
                              Compression.Algorithm compressionAlgorithm,
                              Path pathForReader) {
    super(records, header, new HashMap<>(), HoodieAvroHFileReader.KEY_FIELD_NAME);
    this.compressionAlgorithm = Option.of(compressionAlgorithm);
    this.pathForReader = pathForReader;
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.HFILE_DATA_BLOCK;
  }

  @Override
  protected byte[] serializeRecords(List<HoodieRecord> records) throws IOException {
    HFileContext context = new HFileContextBuilder()
        .withBlockSize(DEFAULT_BLOCK_SIZE)
        .withCompression(compressionAlgorithm.get())
        .withCellComparator(new HoodieHBaseKVComparator())
        .build();

    Configuration conf = new Configuration();
    CacheConfig cacheConfig = new CacheConfig(conf);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream ostream = new FSDataOutputStream(baos, null);

    // Use simple incrementing counter as a key
    boolean useIntegerKey = !getRecordKey(records.get(0)).isPresent();
    // This is set here to avoid re-computing this in the loop
    int keyWidth = useIntegerKey ? (int) Math.ceil(Math.log(records.size())) + 1 : -1;

    // Serialize records into bytes
    Map<String, byte[]> sortedRecordsMap = new TreeMap<>();
    // Get writer schema
    Schema writerSchema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    Iterator<HoodieRecord> itr = records.iterator();
    int id = 0;
    while (itr.hasNext()) {
      HoodieRecord<?> record = itr.next();
      String recordKey;
      if (useIntegerKey) {
        recordKey = String.format("%" + keyWidth + "s", id++);
      } else {
        recordKey = getRecordKey(record).get();
      }

      final byte[] recordBytes = serializeRecord(record, writerSchema);
      if (sortedRecordsMap.containsKey(recordKey)) {
        LOG.error("Found duplicate record with recordKey: " + recordKey);
        printRecord("Previous record", sortedRecordsMap.get(recordKey), writerSchema);
        printRecord("Current record", recordBytes, writerSchema);
        throw new HoodieException(String.format("Writing multiple records with same key %s not supported for %s",
            recordKey, this.getClass().getName()));
      }
      sortedRecordsMap.put(recordKey, recordBytes);
    }

    HFile.Writer writer = HFile.getWriterFactory(conf, cacheConfig)
        .withOutputStream(ostream).withFileContext(context).create();

    // Write the records
    sortedRecordsMap.forEach((recordKey, recordBytes) -> {
      try {
        KeyValue kv = new KeyValue(recordKey.getBytes(), null, null, recordBytes);
        writer.append(kv);
      } catch (IOException e) {
        throw new HoodieIOException("IOException serializing records", e);
      }
    });

    writer.appendFileInfo(HoodieAvroHFileReader.SCHEMA_KEY.getBytes(), getSchema().toString().getBytes());

    writer.close();
    ostream.flush();
    ostream.close();

    return baos.toByteArray();
  }

  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(byte[] content, HoodieRecordType type) throws IOException {
    checkState(readerSchema != null, "Reader's schema has to be non-null");

    FileSystem fs = FSUtils.getFs(pathForReader.toString(), FSUtils.buildInlineConf(getBlockContentLocation().get().getHadoopConf()));
    // Read the content
    HoodieAvroHFileReader reader = new HoodieAvroHFileReader(fs, pathForReader, content, Option.of(getSchemaFromHeader()));
    return unsafeCast(reader.getRecordIterator(readerSchema));
  }

  // TODO abstract this w/in HoodieDataBlock
  @Override
  protected <T> ClosableIterator<HoodieRecord<T>> lookupRecords(List<String> sortedKeys, boolean fullKey) throws IOException {
    HoodieLogBlockContentLocation blockContentLoc = getBlockContentLocation().get();

    // NOTE: It's important to extend Hadoop configuration here to make sure configuration
    //       is appropriately carried over
    Configuration inlineConf = FSUtils.buildInlineConf(blockContentLoc.getHadoopConf());

    Path inlinePath = InLineFSUtils.getInlineFilePath(
        blockContentLoc.getLogFile().getPath(),
        blockContentLoc.getLogFile().getPath().toUri().getScheme(),
        blockContentLoc.getContentPositionInLogFile(),
        blockContentLoc.getBlockSize());

    final HoodieAvroHFileReader reader =
             new HoodieAvroHFileReader(inlineConf, inlinePath, new CacheConfig(inlineConf), inlinePath.getFileSystem(inlineConf),
             Option.of(getSchemaFromHeader()));

    // Get writer's schema from the header
    final ClosableIterator<HoodieRecord<IndexedRecord>> recordIterator =
        fullKey ? reader.getRecordsByKeysIterator(sortedKeys, readerSchema) : reader.getRecordsByKeyPrefixIterator(sortedKeys, readerSchema);

    return new CloseableMappingIterator<>(recordIterator, data -> (HoodieRecord<T>) data);
  }

  private byte[] serializeRecord(HoodieRecord<?> record, Schema schema) throws IOException {
    Option<Schema.Field> keyField = getKeyField(schema);
    // Reset key value w/in the record to avoid duplicating the key w/in payload
    if (keyField.isPresent()) {
      record.truncateRecordKey(schema, new Properties(), keyField.get().name());
    }
    return HoodieAvroUtils.recordToBytes(record, schema).get();
  }

  /**
   * Print the record in json format
   */
  private void printRecord(String msg, byte[] bs, Schema schema) throws IOException {
    GenericRecord record = HoodieAvroUtils.bytesToAvro(bs, schema);
    byte[] json = HoodieAvroUtils.avroToJson(record, true);
    LOG.error(String.format("%s: %s", msg, new String(json)));
  }
}
