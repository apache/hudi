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

package org.apache.hudi.io.hadoop;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.storage.HoodieAvroBootstrapFileReader;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieNativeAvroHFileReader;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;

import java.io.IOException;

public class HoodieAvroFileReaderFactory extends HoodieFileReaderFactory {
  public static final String HBASE_AVRO_HFILE_READER = "org.apache.hudi.io.hadoop.HoodieHBaseAvroHFileReader";

  @Override
  protected HoodieFileReader newParquetFileReader(StorageConfiguration<?> conf, StoragePath path) {
    return new HoodieAvroParquetReader(conf, path);
  }

  @Override
  protected HoodieFileReader newHFileFileReader(HoodieConfig hoodieConfig,
                                                StorageConfiguration<?> conf,
                                                StoragePath path,
                                                Option<Schema> schemaOption) throws IOException {
    if (isUseNativeHFileReaderEnabled(hoodieConfig)) {
      return new HoodieNativeAvroHFileReader(conf, path, schemaOption);
    }
    try {
      if (schemaOption.isPresent()) {
        return (HoodieFileReader) ReflectionUtils.loadClass(HBASE_AVRO_HFILE_READER,
            new Class<?>[] {StorageConfiguration.class, StoragePath.class, Option.class}, conf, path, schemaOption);
      }
      return (HoodieFileReader) ReflectionUtils.loadClass(HBASE_AVRO_HFILE_READER,
          new Class<?>[] {StorageConfiguration.class, StoragePath.class}, conf, path);
    } catch (HoodieException e) {
      throw new IOException("Cannot instantiate HoodieHBaseAvroHFileReader", e);
    }
  }

  @Override
  protected HoodieFileReader newHFileFileReader(HoodieConfig hoodieConfig,
                                                StorageConfiguration<?> conf,
                                                StoragePath path,
                                                HoodieStorage storage,
                                                byte[] content,
                                                Option<Schema> schemaOption)
      throws IOException {
    if (isUseNativeHFileReaderEnabled(hoodieConfig)) {
      return new HoodieNativeAvroHFileReader(conf, content, schemaOption);
    }
    try {
      return (HoodieFileReader) ReflectionUtils.loadClass(HBASE_AVRO_HFILE_READER,
          new Class<?>[] {StorageConfiguration.class, StoragePath.class, HoodieStorage.class, byte[].class, Option.class},
          conf, path, storage, content, schemaOption);
    } catch (HoodieException e) {
      throw new IOException("Cannot instantiate HoodieHBaseAvroHFileReader", e);
    }
  }

  @Override
  protected HoodieFileReader newOrcFileReader(StorageConfiguration<?> conf, StoragePath path) {
    return new HoodieAvroOrcReader(conf, path);
  }

  @Override
  public HoodieFileReader newBootstrapFileReader(HoodieFileReader skeletonFileReader, HoodieFileReader dataFileReader, Option<String[]> partitionFields, Object[] partitionValues) {
    return new HoodieAvroBootstrapFileReader(skeletonFileReader, dataFileReader, partitionFields, partitionValues);
  }
}
