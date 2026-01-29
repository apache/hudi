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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class LanceUtils extends FileFormatUtils {
  private static final int COPY_BUFFER_SIZE = 8192;

  @Override
  public ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieStorage storage, StoragePath filePath) {
    return fetchRecordKeysWithPositions(storage, filePath, Option.empty(), Option.empty());
  }

  @Override
  public ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieStorage storage,
                                                                               StoragePath filePath,
                                                                               Option<BaseKeyGenerator> keyGeneratorOpt,
                                                                               Option<String> partitionPath) {
    AtomicLong position = new AtomicLong(0);
    return new CloseableMappingIterator<>(
        getHoodieKeyIterator(storage, filePath, keyGeneratorOpt, partitionPath),
        key -> Pair.of(key, position.getAndIncrement()));
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath) {
    return getHoodieKeyIterator(storage, filePath, Option.empty(), Option.empty());
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage,
                                                           StoragePath filePath,
                                                           Option<BaseKeyGenerator> keyGeneratorOpt,
                                                           Option<String> partitionPath) {
    try {
      HoodieFileReader reader = HoodieIOFactory.getIOFactory(storage)
          .getReaderFactory(HoodieRecord.HoodieRecordType.SPARK)
          .getFileReader(new HoodieReaderConfig(), filePath, HoodieFileFormat.LANCE);
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
      throw new HoodieIOException("Failed to read from Lance file" + filePath, e);
    }
  }

  @Override
  public HoodieSchema readSchema(HoodieStorage storage, StoragePath filePath) {
    try (HoodieFileReader fileReader =
                 HoodieIOFactory.getIOFactory(storage)
                         .getReaderFactory(HoodieRecord.HoodieRecordType.SPARK)
                         .getFileReader(
                                 ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER,
                                 filePath,
                                 HoodieFileFormat.LANCE)) {
      return fileReader.getSchema();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read schema from Lance file", e);
    }
  }

  @Override
  public HoodieFileFormat getFormat() {
    return HoodieFileFormat.LANCE;
  }

  @Override
  public List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath) {
    throw new UnsupportedOperationException("readAvroRecords is not yet supported for Lance format");
  }

  @Override
  public List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath, HoodieSchema schema) {
    throw new UnsupportedOperationException("readAvroRecords with schema is not yet supported for Lance format");
  }

  @Override
  public Map<String, String> readFooter(HoodieStorage storage, boolean required, StoragePath filePath, String... footerNames) {
    throw new UnsupportedOperationException("readFooter is not yet supported for Lance format");
  }

  @Override
  public long getRowCount(HoodieStorage storage, StoragePath filePath) {
    try (HoodieFileReader fileReader =
                 HoodieIOFactory.getIOFactory(storage)
                         .getReaderFactory(HoodieRecord.HoodieRecordType.SPARK)
                         .getFileReader(
                                 ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER,
                                 filePath,
                                 HoodieFileFormat.LANCE)) {
      return fileReader.getTotalRecords();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read schema from Lance file", e);
    }
  }

  @Override
  public Set<Pair<String, Long>> filterRowKeys(HoodieStorage storage, StoragePath filePath, Set<String> filterKeys) {
    try (HoodieFileReader fileReader =
                 HoodieIOFactory.getIOFactory(storage)
                         .getReaderFactory(HoodieRecord.HoodieRecordType.SPARK)
                         .getFileReader(
                                 ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER,
                                 filePath,
                                 HoodieFileFormat.LANCE)) {
      return fileReader.filterRowKeys(filterKeys);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read filter keys from Lance file", e);
    }
  }

  @Override
  public List<HoodieColumnRangeMetadata<Comparable>> readColumnStatsFromMetadata(HoodieStorage storage,
                                                                                  StoragePath filePath,
                                                                                  List<String> columnList,
                                                                                  HoodieIndexVersion indexVersion) {
    throw new UnsupportedOperationException("readColumnStatsFromMetadata is not yet supported for Lance format");
  }

  @Override
  public void writeMetaFile(HoodieStorage storage, StoragePath filePath, Properties props) throws IOException {
    throw new UnsupportedOperationException("writeMetaFile is not yet supported for Lance format");
  }

  @Override
  public ByteArrayOutputStream serializeRecordsToLogBlock(HoodieStorage storage,
                                                          List<HoodieRecord> records,
                                                          HoodieSchema writerSchema,
                                                          HoodieSchema readerSchema,
                                                          String keyFieldName,
                                                          Map<String, String> paramsMap) throws IOException {
    if (records.isEmpty()) {
      return new ByteArrayOutputStream(0);
    }
    return getByteArrayOutputStream(storage, records.iterator(), writerSchema, readerSchema, keyFieldName, paramsMap, records.get(0).getRecordType());
  }

  @Override
  public Pair<ByteArrayOutputStream, Object> serializeRecordsToLogBlock(HoodieStorage storage,
                                                                        Iterator<HoodieRecord> records,
                                                                        HoodieRecord.HoodieRecordType recordType,
                                                                        HoodieSchema writerSchema,
                                                                        HoodieSchema readerSchema,
                                                                        String keyFieldName,
                                                                        Map<String, String> paramsMap) throws IOException {
    return Pair.of(getByteArrayOutputStream(storage, records, writerSchema, readerSchema, keyFieldName, paramsMap, recordType), null);
  }

  private static HoodieConfig getHoodieConfig(Map<String, String> paramsMap) {
    HoodieConfig config = new HoodieConfig();
    paramsMap.forEach(config::setValue);
    return config;
  }

  private static ByteArrayOutputStream getByteArrayOutputStream(HoodieStorage storage, Iterator<HoodieRecord> records, HoodieSchema writerSchema, HoodieSchema readerSchema, String keyFieldName,
                                                                Map<String, String> paramsMap, HoodieRecord.HoodieRecordType recordType) throws IOException {
    HoodieConfig config = getHoodieConfig(paramsMap);

    File tempFile = File.createTempFile("lance-log-block-" + UUID.randomUUID(), ".lance");
    StoragePath tempFilePath = new StoragePath(tempFile.toURI());

    Object fileFormatMetadata;
    try (HoodieFileWriter lanceWriter = HoodieFileWriterFactory.getFileWriter(null, tempFilePath, storage, config, writerSchema, new LocalTaskContextSupplier(), recordType)) {
      while (records.hasNext()) {
        HoodieRecord record = records.next();
        String recordKey = record.getRecordKey(readerSchema, keyFieldName);
        lanceWriter.write(recordKey, record, writerSchema);
      }
    }

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[COPY_BUFFER_SIZE];
    try (FileInputStream fis = new FileInputStream(tempFile)) {
      int bytesRead;
      while ((bytesRead = fis.read(buffer)) != -1) {
        outputStream.write(buffer, 0, bytesRead);
      }
      outputStream.flush();
    }
    tempFile.delete();
    return outputStream;
  }
}
