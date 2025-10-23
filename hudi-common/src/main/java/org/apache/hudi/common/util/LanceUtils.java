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
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class LanceUtils extends FileFormatUtils {

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
      // Follow similar pattern as HFileUtils
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
  public Schema readAvroSchema(HoodieStorage storage, StoragePath filePath) {
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
  public List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath, Schema schema) {
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
  public Set<Pair<String, Long>> filterRowKeys(HoodieStorage storage, StoragePath filePath, Set<String> filter) {
    // TODO: Confusing naming of filter in the FileFormatsUtils, should be renamed to filterKeys if my assumption is correct
    try (HoodieFileReader fileReader =
                 HoodieIOFactory.getIOFactory(storage)
                         .getReaderFactory(HoodieRecord.HoodieRecordType.SPARK)
                         .getFileReader(
                                 ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER,
                                 filePath,
                                 HoodieFileFormat.LANCE)) {
      return fileReader.filterRowKeys(filter);
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
                                                          Schema writerSchema,
                                                          Schema readerSchema,
                                                          String keyFieldName,
                                                          Map<String, String> paramsMap) throws IOException {
    throw new UnsupportedOperationException("serializeRecordsToLogBlock is not yet supported for Lance format");
  }

  @Override
  public Pair<ByteArrayOutputStream, Object> serializeRecordsToLogBlock(HoodieStorage storage,
                                                                        Iterator<HoodieRecord> records,
                                                                        HoodieRecord.HoodieRecordType recordType,
                                                                        Schema writerSchema,
                                                                        Schema readerSchema,
                                                                        String keyFieldName,
                                                                        Map<String, String> paramsMap) throws IOException {
    throw new UnsupportedOperationException("serializeRecordsToLogBlock with iterator is not yet supported for Lance format");
  }
}