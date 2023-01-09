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
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.orc.CompressionKind;

public class HoodieOrcConfig {
  static final String AVRO_SCHEMA_METADATA_KEY = "orc.avro.schema";

  private final CompressionKind compressionKind;
  private final int stripeSize;
  private final int blockSize;
  private final long maxFileSize;
  private final Configuration hadoopConf;
  private final BloomFilter bloomFilter;

  public HoodieOrcConfig(Configuration hadoopConf, CompressionKind compressionKind, int stripeSize,
      int blockSize, long maxFileSize, BloomFilter bloomFilter) {
    this.hadoopConf = hadoopConf;
    this.compressionKind = compressionKind;
    this.stripeSize = stripeSize;
    this.blockSize = blockSize;
    this.maxFileSize = maxFileSize;
    this.bloomFilter = bloomFilter;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public CompressionKind getCompressionKind() {
    return compressionKind;
  }

  public int getStripeSize() {
    return stripeSize;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public long getMaxFileSize() {
    return maxFileSize;
  }

  public boolean useBloomFilter() {
    return bloomFilter != null;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }
}
