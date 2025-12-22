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

package org.apache.hudi.common.util;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.compress.CompressionCodec;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieHBaseKVComparator;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.hudi.common.config.HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Utility functions for HFile files.
 */
public class HFileUtils extends FileFormatUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HFileUtils.class);
  private static final int DEFAULT_BLOCK_SIZE_FOR_LOG_FILE = 1024 * 1024;

  /**
   * Gets the {@link Compression.Algorithm} Enum based on the {@link CompressionCodec} name.
   *
   * @param paramsMap parameter map containing the compression codec config.
   * @return the {@link Compression.Algorithm} Enum.
   */
  public static Compression.Algorithm getHFileCompressionAlgorithm(Map<String, String> paramsMap) {
    String algoName = paramsMap.get(HFILE_COMPRESSION_ALGORITHM_NAME.key());
    if (StringUtils.isNullOrEmpty(algoName)) {
      return Compression.Algorithm.GZ;
    }
    return Compression.Algorithm.valueOf(algoName.toUpperCase());
  }

  @Override
  public List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath) {
    throw new UnsupportedOperationException("HFileUtils does not support readAvroRecords");
  }

  @Override
  public List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath, Schema schema) {
    throw new UnsupportedOperationException("HFileUtils does not support readAvroRecords");
  }

  @Override
  public Map<String, String> readFooter(HoodieStorage storage, boolean required, StoragePath filePath, String... footerNames) {
    throw new UnsupportedOperationException("HFileUtils does not support readFooter");
  }

  @Override
  public long getRowCount(HoodieStorage storage, StoragePath filePath) {
    throw new UnsupportedOperationException("HFileUtils does not support getRowCount");
  }

  @Override
  public Set<String> filterRowKeys(HoodieStorage storage, StoragePath filePath, Set<String> filter) {
    throw new UnsupportedOperationException("HFileUtils does not support filterRowKeys");
  }

  @Override
  public List<HoodieKey> fetchHoodieKeys(HoodieStorage storage, StoragePath filePath) {
    throw new UnsupportedOperationException("HFileUtils does not support fetchRecordKeysWithPositions");
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt) {
    throw new UnsupportedOperationException("HFileUtils does not support getHoodieKeyIterator");
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath) {
    throw new UnsupportedOperationException("HFileUtils does not support getHoodieKeyIterator");
  }

  @Override
  public List<HoodieKey> fetchHoodieKeys(HoodieStorage storage, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt) {
    throw new UnsupportedOperationException("HFileUtils does not support fetchRecordKeysWithPositions");
  }

  @Override
  public Schema readAvroSchema(HoodieStorage storage, StoragePath filePath) {
    LOG.info("Reading schema from {}", filePath);

    try (HoodieFileReader fileReader =
             HoodieIOFactory.getIOFactory(storage)
                 .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
                 .getFileReader(
                     ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER,
                     filePath)) {
      return fileReader.getSchema();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read schema from HFile", e);
    }
  }

  @Override
  public List<HoodieColumnRangeMetadata<Comparable>> readColumnStatsFromMetadata(HoodieStorage storage, StoragePath filePath, List<String> columnList) {
    throw new UnsupportedOperationException(
        "Reading column statistics from metadata is not supported for HFile format yet");
  }

  @Override
  public HoodieFileFormat getFormat() {
    return HoodieFileFormat.HFILE;
  }

  @Override
  public void writeMetaFile(HoodieStorage storage, StoragePath filePath, Properties props) throws IOException {
    throw new UnsupportedOperationException("HFileUtils does not support writeMetaFile");
  }

  @Override
  public byte[] serializeRecordsToLogBlock(HoodieStorage storage,
                                           List<HoodieRecord> records,
                                           Schema writerSchema,
                                           Schema readerSchema,
                                           String keyFieldName,
                                           Map<String, String> paramsMap) throws IOException {
    Compression.Algorithm compressionAlgorithm = getHFileCompressionAlgorithm(paramsMap);
    HFileContext context = new HFileContextBuilder()
        .withBlockSize(DEFAULT_BLOCK_SIZE_FOR_LOG_FILE)
        .withCompression(compressionAlgorithm)
        .withCellComparator(new HoodieHBaseKVComparator())
        .build();

    Configuration conf = storage.getConf().unwrapAs(Configuration.class);
    CacheConfig cacheConfig = new CacheConfig(conf);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream ostream = new FSDataOutputStream(baos, null);

    // Use simple incrementing counter as a key
    boolean useIntegerKey = !getRecordKey(records.get(0), readerSchema, keyFieldName).isPresent();
    // This is set here to avoid re-computing this in the loop
    int keyWidth = useIntegerKey ? (int) Math.ceil(Math.log(records.size())) + 1 : -1;

    // Serialize records into bytes
    Map<String, List<byte[]>> sortedRecordsMap = new TreeMap<>();

    Iterator<HoodieRecord> itr = records.iterator();
    int id = 0;
    while (itr.hasNext()) {
      HoodieRecord<?> record = itr.next();
      String recordKey;
      if (useIntegerKey) {
        recordKey = String.format("%" + keyWidth + "s", id++);
      } else {
        recordKey = getRecordKey(record, readerSchema, keyFieldName).get();
      }

      final byte[] recordBytes = serializeRecord(record, writerSchema, keyFieldName);
      // If key exists in the map, append to its list. If not, create a new list.
      // Get the existing list of recordBytes for the recordKey, or an empty list if it doesn't exist
      List<byte[]> recordBytesList = sortedRecordsMap.getOrDefault(recordKey, new ArrayList<>());
      recordBytesList.add(recordBytes);
      // Put the updated list back into the map
      sortedRecordsMap.put(recordKey, recordBytesList);
    }

    HFile.Writer writer = HFile.getWriterFactory(conf, cacheConfig)
        .withOutputStream(ostream).withFileContext(context).create();

    // Write the records
    sortedRecordsMap.forEach((recordKey, recordBytesList) -> {
      for (byte[] recordBytes : recordBytesList) {
        try {
          KeyValue kv = new KeyValue(recordKey.getBytes(), null, null, recordBytes);
          writer.append(kv);
        } catch (IOException e) {
          throw new HoodieIOException("IOException serializing records", e);
        }
      }
    });

    writer.appendFileInfo(
        getUTF8Bytes(HoodieAvroHFileReaderImplBase.SCHEMA_KEY), getUTF8Bytes(readerSchema.toString()));

    writer.close();
    ostream.flush();
    ostream.close();

    return baos.toByteArray();
  }

  private static Option<String> getRecordKey(HoodieRecord record, Schema readerSchema, String keyFieldName) {
    return Option.ofNullable(record.getRecordKey(readerSchema, keyFieldName));
  }

  private static byte[] serializeRecord(HoodieRecord<?> record, Schema schema, String keyFieldName) throws IOException {
    Option<Schema.Field> keyField = Option.ofNullable(schema.getField(keyFieldName));
    // Reset key value w/in the record to avoid duplicating the key w/in payload
    if (keyField.isPresent()) {
      record.truncateRecordKey(schema, new Properties(), keyField.get().name());
    }
    return HoodieAvroUtils.recordToBytes(record, schema).get();
  }
}
