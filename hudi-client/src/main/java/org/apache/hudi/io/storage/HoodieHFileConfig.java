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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hudi.common.bloom.BloomFilter;

public class HoodieHFileConfig {

  private Compression.Algorithm compressionAlgorithm;
  private int blockSize;
  private long maxFileSize;
  private Configuration hadoopConf;
  private BloomFilter bloomFilter;

  public HoodieHFileConfig(Compression.Algorithm compressionAlgorithm, int blockSize, long maxFileSize,
                           Configuration hadoopConf, BloomFilter bloomFilter) {
    this.compressionAlgorithm = compressionAlgorithm;
    this.blockSize = blockSize;
    this.maxFileSize = maxFileSize;
    this.hadoopConf = hadoopConf;
    this.bloomFilter = bloomFilter;
  }

  public Compression.Algorithm getCompressionAlgorithm() {
    return compressionAlgorithm;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public long getMaxFileSize() {
    return maxFileSize;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public boolean useBloomFilter() {
    return bloomFilter != null;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }
}
