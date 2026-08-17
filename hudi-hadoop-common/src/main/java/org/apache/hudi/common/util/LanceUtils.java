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

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.io.memory.HoodieArrowAllocator;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.avro.generic.GenericRecord;
import org.lance.file.LanceFileReader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class LanceUtils extends FileFormatUtils {

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage,
                                                           StoragePath filePath,
                                                           Option<BaseKeyGenerator> keyGeneratorOpt,
                                                           Option<String> partitionPath) {
    try {
      HoodieSchema keySchema = getKeyIteratorSchema(storage, filePath, keyGeneratorOpt, partitionPath);
      HoodieFileReader reader = HoodieIOFactory.getIOFactory(storage)
          .getReaderFactory(HoodieRecord.HoodieRecordType.SPARK)
          .getFileReader(new HoodieReaderConfig(), filePath, HoodieFileFormat.LANCE);
      ClosableIterator<HoodieRecord> recordIterator = reader.getRecordIterator(keySchema);

      return new CloseableMappingIterator<>(
          recordIterator,
          record -> {
            String recordKey;
            if (keyGeneratorOpt.isPresent()) {
              recordKey = record.getRecordKey(keySchema, keyGeneratorOpt);
            } else {
              recordKey = record.getRecordKey(keySchema, HoodieRecord.RECORD_KEY_METADATA_FIELD);
            }
            String partitionPathValue = partitionPath.orElseGet(() ->
                (String) record.getColumnValueAsJava(keySchema, HoodieRecord.PARTITION_PATH_METADATA_FIELD,
                    CollectionUtils.emptyProps()));
            return new HoodieKey(recordKey, partitionPathValue);
          }
      );
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
    Map<String, String> footerVals = new HashMap<>();
    long metadataAllocatorSize = Long.parseLong(
        HoodieStorageConfig.LANCE_READ_METADATA_ALLOCATOR_SIZE_BYTES.defaultValue());
    try (BufferAllocator allocator = HoodieArrowAllocator.newChildAllocator(
        getClass().getSimpleName() + "-metadata-" + filePath.getName(), metadataAllocatorSize);
         LanceFileReader reader = LanceFileReader.open(filePath.toString(), allocator)) {
      Map<String, String> metadata = reader.schema().getCustomMetadata();
      for (String footerName : footerNames) {
        if (metadata != null && metadata.containsKey(footerName)) {
          footerVals.put(footerName, metadata.get(footerName));
        } else if (required) {
          throw new MetadataNotFoundException(
              "Could not find metadata key in Lance footer. Key " + footerName + ", file: " + filePath);
        }
      }
      return footerVals;
    } catch (MetadataNotFoundException e) {
      throw e;
    } catch (IOException e) {
      throw new HoodieIOException("Unable to read footer for Lance: " + filePath, e);
    } catch (Exception e) {
      throw new HoodieException("Unable to close Lance footer reader: " + filePath, e);
    }
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
    throw new UnsupportedOperationException("serializeRecordsToLogBlock is not yet supported for Lance format");
  }

  @Override
  public Pair<ByteArrayOutputStream, Object> serializeRecordsToLogBlock(HoodieStorage storage,
                                                                        Iterator<HoodieRecord> records,
                                                                        HoodieRecord.HoodieRecordType recordType,
                                                                        HoodieSchema writerSchema,
                                                                        HoodieSchema readerSchema,
                                                                        String keyFieldName,
                                                                        Map<String, String> paramsMap) throws IOException {
    throw new UnsupportedOperationException("serializeRecordsToLogBlock with iterator is not yet supported for Lance format");
  }
}
