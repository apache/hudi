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

package org.apache.parquet.hadoop;

import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Bloom filter reader that reads Bloom filter data from an open {@link HoodieParquetFileReader}.
 */
public class HoodieBloomFilterReader {
  private final HoodieParquetFileReader reader;
  private final Map<ColumnPath, ColumnChunkMetaData> columns;
  private final Map<ColumnPath, BloomFilter> cache = new HashMap<>();
  private Logger logger = LoggerFactory.getLogger(HoodieBloomFilterReader.class);

  public HoodieBloomFilterReader(HoodieParquetFileReader fileReader, BlockMetaData block) {
    this.reader = fileReader;
    this.columns = new HashMap<>();
    for (ColumnChunkMetaData column : block.getColumns()) {
      columns.put(column.getPath(), column);
    }
  }

  public BloomFilter readBloomFilter(ColumnChunkMetaData meta) {
    if (cache.containsKey(meta.getPath())) {
      return cache.get(meta.getPath());
    }
    try {
      if (!cache.containsKey(meta.getPath())) {
        BloomFilter bloomFilter = reader.readBloomFilter(meta);
        if (bloomFilter == null) {
          return null;
        }

        cache.put(meta.getPath(), bloomFilter);
      }
      return cache.get(meta.getPath());
    } catch (IOException e) {
      logger.error("Failed to read Bloom filter data", e);
    }

    return null;
  }
}
