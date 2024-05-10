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

import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Utility functions for HFile files.
 */
public class HFileUtils extends BaseFileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HFileUtils.class);

  @Override
  public List<GenericRecord> readAvroRecords(StorageConfiguration<?> configuration, StoragePath filePath) {
    throw new UnsupportedOperationException("HFileUtils does not support readAvroRecords");
  }

  @Override
  public List<GenericRecord> readAvroRecords(StorageConfiguration<?> configuration, StoragePath filePath, Schema schema) {
    throw new UnsupportedOperationException("HFileUtils does not support readAvroRecords");
  }

  @Override
  public Map<String, String> readFooter(StorageConfiguration<?> configuration, boolean required, StoragePath filePath, String... footerNames) {
    throw new UnsupportedOperationException("HFileUtils does not support readFooter");
  }

  @Override
  public long getRowCount(StorageConfiguration<?> configuration, StoragePath filePath) {
    throw new UnsupportedOperationException("HFileUtils does not support getRowCount");
  }

  @Override
  public Set<Pair<String, Long>> filterRowKeys(StorageConfiguration<?> configuration, StoragePath filePath, Set<String> filter) {
    throw new UnsupportedOperationException("HFileUtils does not support filterRowKeys");
  }

  @Override
  public List<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(StorageConfiguration<?> configuration, StoragePath filePath) {
    throw new UnsupportedOperationException("HFileUtils does not support fetchRecordKeysWithPositions");
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(StorageConfiguration<?> configuration, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt) {
    throw new UnsupportedOperationException("HFileUtils does not support getHoodieKeyIterator");
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(StorageConfiguration<?> configuration, StoragePath filePath) {
    throw new UnsupportedOperationException("HFileUtils does not support getHoodieKeyIterator");
  }

  @Override
  public List<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(StorageConfiguration<?> configuration, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt) {
    throw new UnsupportedOperationException("HFileUtils does not support fetchRecordKeysWithPositions");
  }

  @Override
  public Schema readAvroSchema(StorageConfiguration<?> configuration, StoragePath filePath) {
    LOG.info("Reading schema from {}", filePath);

    try (HoodieFileReader fileReader =
             HoodieFileReaderFactory.getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
                 .getFileReader(
                     ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER,
                     configuration,
                     filePath)) {
      return fileReader.getSchema();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read schema from HFile", e);
    }
  }

  @Override
  public List<HoodieColumnRangeMetadata<Comparable>> readColumnStatsFromMetadata(StorageConfiguration<?> storageConf, StoragePath filePath, List<String> columnList) {
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
}
