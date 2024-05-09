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
import org.apache.hudi.io.storage.HoodieAvroBootstrapFileReader;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieHBaseAvroHFileReader;
import org.apache.hudi.io.storage.HoodieNativeAvroHFileReader;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;

import java.io.IOException;

public class HoodieAvroFileReaderFactory extends HoodieFileReaderFactory {
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
    CacheConfig cacheConfig = new CacheConfig(conf.unwrapAs(Configuration.class));
    if (schemaOption.isPresent()) {
      return new HoodieHBaseAvroHFileReader(conf, path, cacheConfig, HoodieStorageUtils.getStorage(path, conf), schemaOption);
    }
    return new HoodieHBaseAvroHFileReader(conf, path, cacheConfig);
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
    CacheConfig cacheConfig = new CacheConfig(conf.unwrapAs(Configuration.class));
    return new HoodieHBaseAvroHFileReader(conf, path, cacheConfig, storage, content, schemaOption);
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
