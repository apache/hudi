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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieHFileReader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.util.Pair;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

/**
 * HoodieHFileDataBlock contains a list of records stored inside an HFile format. It is used with the HFile
 * base file format.
 */
public class HoodieHFileDataBlock extends HoodieDataBlock {
  private static final Logger LOG = LogManager.getLogger(HoodieHFileDataBlock.class);
  private static Compression.Algorithm compressionAlgorithm = Compression.Algorithm.GZ;
  private static int blockSize = 1 * 1024 * 1024;

  public HoodieHFileDataBlock(@Nonnull Map<HeaderMetadataType, String> logBlockHeader,
       @Nonnull Map<HeaderMetadataType, String> logBlockFooter,
       @Nonnull Option<HoodieLogBlockContentLocation> blockContentLocation, @Nonnull Option<byte[]> content,
       FSDataInputStream inputStream, boolean readBlockLazily) {
    super(logBlockHeader, logBlockFooter, blockContentLocation, content, inputStream, readBlockLazily);
  }

  public HoodieHFileDataBlock(HoodieLogFile logFile, FSDataInputStream inputStream, Option<byte[]> content,
       boolean readBlockLazily, long position, long blockSize, long blockEndpos, Schema readerSchema,
       Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    super(content, inputStream, readBlockLazily,
          Option.of(new HoodieLogBlockContentLocation(logFile, position, blockSize, blockEndpos)), readerSchema, header,
          footer);
  }

  public HoodieHFileDataBlock(@Nonnull List<IndexedRecord> records, @Nonnull Map<HeaderMetadataType, String> header) {
    super(records, header, new HashMap<>());
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
        .withOutputStream(ostream).withFileContext(context).create();

    // Serialize records into bytes
    Map<String, byte[]> sortedRecordsMap = new TreeMap<>();
    Iterator<IndexedRecord> itr = records.iterator();
    boolean useIntegerKey = false;
    int key = 0;
    int keySize = 0;
    Field keyField = records.get(0).getSchema().getField(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    if (keyField == null) {
      // Missing key metadata field so we should use an integer sequence key
      useIntegerKey = true;
      keySize = (int) Math.ceil(Math.log(records.size())) + 1;
    }
    while (itr.hasNext()) {
      IndexedRecord record = itr.next();
      String recordKey;
      if (useIntegerKey) {
        recordKey = String.format("%" + keySize + "s", key++);
      } else {
        recordKey = record.get(keyField.pos()).toString();
      }
      byte[] recordBytes = HoodieAvroUtils.indexedRecordToBytes(record);
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
  protected void deserializeRecords() throws IOException {
    // Get schema from the header
    Schema writerSchema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));

    // If readerSchema was not present, use writerSchema
    if (schema == null) {
      schema = writerSchema;
    }

    // Read the content
    HoodieHFileReader reader = new HoodieHFileReader<>(getContent().get());
    List<Pair<String, IndexedRecord>> records = reader.readAllRecords(writerSchema, schema);
    this.records = records.stream().map(t -> t.getSecond()).collect(Collectors.toList());

    // Free up content to be GC'd, deflate
    deflate();
  }
}
