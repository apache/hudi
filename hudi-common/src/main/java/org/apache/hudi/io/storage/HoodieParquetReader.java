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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

public class HoodieParquetReader<R extends IndexedRecord> implements HoodieFileReader {
  private Path path;
  private Configuration conf;
  private final BaseFileUtils parquetUtils;
  private Schema schema;

  public HoodieParquetReader(Configuration configuration, Path path) {
    this.conf = configuration;
    this.path = path;
    this.parquetUtils = BaseFileUtils.getInstance(HoodieFileFormat.PARQUET);
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    return parquetUtils.readMinMaxRecordKeys(conf, path);
  }

  @Override
  public BloomFilter readBloomFilter() {
    return parquetUtils.readBloomFilterFromMetadata(conf, path);
  }

  @Override
  public Set<String> filterRowKeys(Set candidateRowKeys) {
    return parquetUtils.filterRowKeys(conf, path, candidateRowKeys);
  }

  @Override
  public Iterator<R> getRecordIterator(Schema schema) throws IOException {
    AvroReadSupport.setAvroReadSchema(conf, schema);
    AvroReadSupport.setRequestedProjection(conf, schema);
    ParquetReader<IndexedRecord> reader = AvroParquetReader.<IndexedRecord>builder(path).withConf(conf).build();
    return new ParquetReaderIterator(reader);
  }

  @Override
  public Schema getSchema() {
    if (schema == null) {
      schema = parquetUtils.readAvroSchema(conf, path);
    }

    return schema;
  }

  @Override
  public void close() {
  }

  @Override
  public long getTotalRecords() {
    return parquetUtils.getRowCount(conf, path);
  }

  @Override
  public Iterator<String> getRecordKeyIterator() throws IOException {
    Iterator<R> recordIterator = getRecordIterator(HoodieAvroUtils.getRecordKeySchema());
    return new Iterator<String>() {
      @Override
      public boolean hasNext() {
        return recordIterator.hasNext();
      }

      @Override
      public String next() {
        Object obj = recordIterator.next();
        return ((GenericRecord) obj).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      }
    };
  }
}
