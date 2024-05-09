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

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
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

public class HFileUtils extends BaseFileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HFileUtils.class);

  @Override
  public List<GenericRecord> readAvroRecords(StorageConfiguration<?> configuration, StoragePath filePath) {
    return null;
  }

  @Override
  public List<GenericRecord> readAvroRecords(StorageConfiguration<?> configuration, StoragePath filePath, Schema schema) {
    return null;
  }

  @Override
  public Map<String, String> readFooter(StorageConfiguration<?> configuration, boolean required, StoragePath filePath, String... footerNames) {
    return null;
  }

  @Override
  public long getRowCount(StorageConfiguration<?> configuration, StoragePath filePath) {
    return 0;
  }

  @Override
  public Set<Pair<String, Long>> filterRowKeys(StorageConfiguration<?> configuration, StoragePath filePath, Set<String> filter) {
    return null;
  }

  @Override
  public List<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(StorageConfiguration<?> configuration, StoragePath filePath) {
    return null;
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(StorageConfiguration<?> configuration, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt) {
    return null;
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(StorageConfiguration<?> configuration, StoragePath filePath) {
    return null;
  }

  @Override
  public List<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(StorageConfiguration<?> configuration, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt) {
    return null;
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
      throw new RuntimeException(e);
    }
  }

  @Override
  public HoodieFileFormat getFormat() {
    return null;
  }

  @Override
  public void writeMetaFile(HoodieStorage storage, StoragePath filePath, Properties props) throws IOException {

  }
}
