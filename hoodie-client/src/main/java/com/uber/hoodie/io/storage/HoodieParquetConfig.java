/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io.storage;

import com.uber.hoodie.avro.HoodieAvroWriteSupport;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class HoodieParquetConfig {
    private HoodieAvroWriteSupport writeSupport;
    private CompressionCodecName compressionCodecName;
    private int blockSize;
    private int pageSize;
    private long maxFileSize;
    private Configuration hadoopConf;

    public HoodieParquetConfig(HoodieAvroWriteSupport writeSupport,
        CompressionCodecName compressionCodecName, int blockSize, int pageSize, long maxFileSize,
        Configuration hadoopConf) {
        this.writeSupport = writeSupport;
        this.compressionCodecName = compressionCodecName;
        this.blockSize = blockSize;
        this.pageSize = pageSize;
        this.maxFileSize = maxFileSize;
        this.hadoopConf = hadoopConf;
    }

    public HoodieAvroWriteSupport getWriteSupport() {
        return writeSupport;
    }

    public CompressionCodecName getCompressionCodecName() {
        return compressionCodecName;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public Configuration getHadoopConf() {
        return hadoopConf;
    }
}
