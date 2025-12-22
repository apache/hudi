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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * ParquetConfig for datasource implementation with {@link org.apache.hudi.client.model.HoodieInternalRow}.
 */
public class HoodieRowParquetConfig extends HoodieParquetConfig<HoodieRowParquetWriteSupport> {

  public HoodieRowParquetConfig(HoodieRowParquetWriteSupport writeSupport, CompressionCodecName compressionCodecName,
      int blockSize, int pageSize, long maxFileSize, Configuration hadoopConf,
      double compressionRatio, boolean enableDictionary) {
    super(writeSupport, compressionCodecName, blockSize, pageSize, maxFileSize,
        new HadoopStorageConfiguration(hadoopConf), compressionRatio, enableDictionary);
  }

  public Configuration getHadoopConf() {
    return getStorageConf().unwrapAs(Configuration.class);
  }
}
