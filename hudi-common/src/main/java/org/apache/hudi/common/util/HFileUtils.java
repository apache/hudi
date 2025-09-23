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
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.compress.CompressionCodec;
import org.apache.hudi.io.hfile.HFileContext;
import org.apache.hudi.io.hfile.HFileWriter;
import org.apache.hudi.io.hfile.HFileWriterImpl;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
   * @param paramsMap parameter map containing the compression codec config.
   * @return the {@link CompressionCodec} Enum.
   */
  public static CompressionCodec getHFileCompressionAlgorithm(Map<String, String> paramsMap) {
    String codecName = paramsMap.get(HFILE_COMPRESSION_ALGORITHM_NAME.key());
    if (StringUtils.isNullOrEmpty(codecName)) {
      return CompressionCodec.GZIP;
    }
    return CompressionCodec.findCodecByName(codecName);
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
  public Set<Pair<String, Long>> filterRowKeys(HoodieStorage storage, StoragePath filePath, Set<String> filter) {
    throw new UnsupportedOperationException("HFileUtils does not support filterRowKeys");
  }

  @Override
  public ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieStorage storage, StoragePath filePath) {
    throw new UnsupportedOperationException("HFileUtils does not support fetchRecordKeysWithPositions");
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt, Option<String> partitionPath) {
    try {
      HoodieFileReader reader = HoodieIOFactory
          .getIOFactory(storage)
          .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
          .getFileReader(new HoodieReaderConfig(), filePath, HoodieFileFormat.HFILE);
      ClosableIterator<String> keyIterator = reader.getRecordKeyIterator();
      return new ClosableIterator<HoodieKey>() {
        @Override
        public void close() {
          keyIterator.close();
        }

        @Override
        public boolean hasNext() {
          return keyIterator.hasNext();
        }

        @Override
        public HoodieKey next() {
          String key = keyIterator.next();
          return new HoodieKey(key, partitionPath.orElse(null));
        }
      };
    } catch (IOException e) {
      throw new HoodieIOException("Unable to read the HFile: ", e);
    }
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath) {
    throw new UnsupportedOperationException("HFileUtils does not support getHoodieKeyIterator");
  }

  @Override
  public ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieStorage storage, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt, Option<String> partitionPath) {
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
  public ByteArrayOutputStream serializeRecordsToLogBlock(HoodieStorage storage,
                                                          List<HoodieRecord> records,
                                                          Schema writerSchema,
                                                          Schema readerSchema,
                                                          String keyFieldName,
                                                          Map<String, String> paramsMap) throws IOException {
    CompressionCodec compressionCodec = getHFileCompressionAlgorithm(paramsMap);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream ostream = new DataOutputStream(baos);

    // Use simple incrementing counter as a key
    boolean useIntegerKey = !getRecordKey(records.get(0), readerSchema, keyFieldName).isPresent();
    // This is set here to avoid re-computing this in the loop
    int keyWidth = useIntegerKey ? (int) Math.ceil(Math.log(records.size())) + 1 : -1;

    // Serialize records into bytes
    Map<String, byte[]> sortedRecordsMap = new TreeMap<>();

    Iterator<HoodieRecord> itr = records.iterator();
    int id = 0;
    Option<Schema.Field> keyField = Option.ofNullable(writerSchema.getField(keyFieldName));
    while (itr.hasNext()) {
      HoodieRecord<?> record = itr.next();
      String recordKey;
      if (useIntegerKey) {
        recordKey = String.format("%" + keyWidth + "s", id++);
      } else {
        recordKey = getRecordKey(record, readerSchema, keyFieldName).get();
      }

      final byte[] recordBytes = serializeRecord(record, writerSchema, keyField);
      if (sortedRecordsMap.containsKey(recordKey)) {
        LOG.error("Found duplicate record with recordKey: {} ", recordKey);
        logRecordMetadata("Previous record", sortedRecordsMap.get(recordKey), writerSchema);
        logRecordMetadata("Current record", recordBytes, writerSchema);
        throw new HoodieException(String.format("Writing multiple records with same key %s not supported for Hfile format with Metadata table",
            recordKey));
      }
      sortedRecordsMap.put(recordKey, recordBytes);
    }

    HFileContext context = HFileContext.builder()
        .blockSize(DEFAULT_BLOCK_SIZE_FOR_LOG_FILE)
        .compressionCodec(compressionCodec)
        .build();
    try (HFileWriter writer = new HFileWriterImpl(context, ostream)) {
      sortedRecordsMap.forEach((recordKey,recordBytes) -> {
        try {
          writer.append(recordKey, recordBytes);
        } catch (IOException e) {
          throw new HoodieIOException("IOException serializing records", e);
        }
      });
      writer.appendFileInfo(
          HoodieAvroHFileReaderImplBase.SCHEMA_KEY,
          getUTF8Bytes(readerSchema.toString()));
    }

    ostream.flush();
    ostream.close();
    return baos;
  }

  /**
   * Print the meta fields of the record of interest
   */
  private void logRecordMetadata(String msg, byte[] bs, Schema schema) throws IOException {
    GenericRecord record = HoodieAvroUtils.bytesToAvro(bs, schema);
    if (schema.getField(HoodieRecord.RECORD_KEY_METADATA_FIELD) != null) {
      LOG.error("{}: Hudi meta field values -> Record key: {}, Partition Path: {}, FileName: {}, CommitTime: {}, CommitSeqNo: {}", msg,
          record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD), record.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD),
          record.get(HoodieRecord.FILENAME_METADATA_FIELD), record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
          record.get(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD));
    }
  }

  @Override
  public Pair<ByteArrayOutputStream, Object> serializeRecordsToLogBlock(
      HoodieStorage storage,
      Iterator<HoodieRecord> records,
      HoodieRecord.HoodieRecordType recordType,
      Schema writerSchema,
      Schema readerSchema,
      String keyFieldName,
      Map<String, String> paramsMap) throws IOException {
    throw new UnsupportedOperationException("HFileUtils does not support serializeRecordsToLogBlock returning HoodieColumnRangeMetadata.");
  }

  private static Option<String> getRecordKey(HoodieRecord record, Schema readerSchema, String keyFieldName) {
    return Option.ofNullable(record.getRecordKey(readerSchema, keyFieldName));
  }

  private static byte[] serializeRecord(HoodieRecord<?> record, Schema schema, Option<Schema.Field> keyField) throws IOException {
    return record.toIndexedRecord(schema, CollectionUtils.emptyProps()).map(HoodieAvroIndexedRecord::getData).map(indexedRecord -> {
      keyField.ifPresent(field -> indexedRecord.put(field.pos(), StringUtils.EMPTY_STRING));
      return HoodieAvroUtils.avroToBytes(indexedRecord);
    }).orElseThrow(() -> new HoodieException("Unable to convert record to indexed record"));
  }
}
