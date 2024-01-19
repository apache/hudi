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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;

import java.io.IOException;

public class HoodieAvroFileReaderFactory extends HoodieFileReaderFactory {
  protected HoodieFileReader newParquetFileReader(Configuration conf, Path path) {
    return new HoodieAvroParquetReader(conf, path);
  }

  protected HoodieFileReader newHFileFileReader(boolean useBuiltInHFileReader,
                                                Configuration conf,
                                                Path path,
                                                Option<Schema> schemaOption) throws IOException {
    if (useBuiltInHFileReader) {
      return new HoodieAvroHFileReader(conf, path, schemaOption);
    }
    CacheConfig cacheConfig = new CacheConfig(conf);
    if (schemaOption.isPresent()) {
      return new HoodieAvroHBaseHFileReader(conf, path, cacheConfig, path.getFileSystem(conf), schemaOption);
    }
    return new HoodieAvroHBaseHFileReader(conf, path, cacheConfig);
  }

  protected HoodieFileReader newHFileFileReader(boolean useBuiltInHFileReader,
                                                Configuration conf,
                                                Path path,
                                                FileSystem fs,
                                                byte[] content,
                                                Option<Schema> schemaOption)
      throws IOException {
    if (useBuiltInHFileReader) {
      return new HoodieAvroHFileReader(conf, content, schemaOption);
    }
    CacheConfig cacheConfig = new CacheConfig(conf);
    return new HoodieAvroHBaseHFileReader(conf, path, cacheConfig, fs, content, schemaOption);
  }

  @Override
  protected HoodieFileReader newOrcFileReader(Configuration conf, Path path) {
    return new HoodieAvroOrcReader(conf, path);
  }

  @Override
  public HoodieFileReader newBootstrapFileReader(HoodieFileReader skeletonFileReader, HoodieFileReader dataFileReader, Option<String[]> partitionFields, Object[] partitionValues) {
    return new HoodieAvroBootstrapFileReader(skeletonFileReader, dataFileReader, partitionFields, partitionValues);
  }
}
