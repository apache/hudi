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

import org.apache.hudi.avro.HoodieAvroWriteSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * ParquetConfig for writing avro records in Parquet files.
 */
public class HoodieAvroParquetConfig extends HoodieBaseParquetConfig<HoodieAvroWriteSupport> {

  public HoodieAvroParquetConfig(HoodieAvroWriteSupport writeSupport, CompressionCodecName compressionCodecName,
                                 int blockSize, int pageSize, long maxFileSize, Configuration hadoopConf,
                                 double compressionRatio) {
    super(writeSupport, compressionCodecName, blockSize, pageSize, maxFileSize, hadoopConf, compressionRatio);
  }

  public HoodieAvroParquetConfig(HoodieAvroWriteSupport writeSupport, CompressionCodecName compressionCodecName,
      int blockSize, int pageSize, long maxFileSize, Configuration hadoopConf,
      double compressionRatio, boolean directoryEnabled) {
    super(writeSupport, compressionCodecName, blockSize, pageSize, maxFileSize, hadoopConf, compressionRatio, directoryEnabled);
  }
}
