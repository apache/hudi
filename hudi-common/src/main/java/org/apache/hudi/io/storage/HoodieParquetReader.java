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

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

public class HoodieParquetReader<R extends IndexedRecord> implements HoodieFileReader {
  private Path path;
  private Configuration conf;

  public HoodieParquetReader(Configuration configuration, Path path) {
    this.conf = configuration;
    this.path = path;
  }

  public String[] readMinMaxRecordKeys() {
    return ParquetUtils.readMinMaxRecordKeys(conf, path);
  }

  @Override
  public BloomFilter readBloomFilter() {
    return ParquetUtils.readBloomFilterFromParquetMetadata(conf, path);
  }

  @Override
  public Set<String> filterRowKeys(Set candidateRowKeys) {
    return ParquetUtils.filterParquetRowKeys(conf, path, candidateRowKeys);
  }

  @Override
  public Iterator<R> getRecordIterator(Schema schema) throws IOException {
    AvroReadSupport.setAvroReadSchema(conf, schema);
    ParquetReader<IndexedRecord> reader = AvroParquetReader.<IndexedRecord>builder(path).withConf(conf).build();
    return new ParquetReaderIterator(reader);
  }

  @Override
  public Schema getSchema() {
    return ParquetUtils.readAvroSchema(conf, path);
  }

  @Override
  public void close() {
  }

  @Override
  public long getTotalRecords() {
    // TODO Auto-generated method stub
    return 0;
  }
}
