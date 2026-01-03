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

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;

import java.io.IOException;
import java.util.Set;

public abstract class HoodieBootstrapFileReader<T> implements HoodieFileReader<T> {

  private final HoodieFileReader<T> skeletonFileReader;
  private final HoodieFileReader<T> dataFileReader;

  private final Option<String[]> partitionFields;
  private final Object[] partitionValues;

  public HoodieBootstrapFileReader(HoodieFileReader<T> skeletonFileReader, HoodieFileReader<T> dataFileReader, Option<String[]> partitionFields, Object[] partitionValues) {
    this.skeletonFileReader = skeletonFileReader;
    this.dataFileReader = dataFileReader;
    this.partitionFields = partitionFields;
    this.partitionValues = partitionValues;
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    return skeletonFileReader.readMinMaxRecordKeys();
  }

  @Override
  public BloomFilter readBloomFilter() {
    return skeletonFileReader.readBloomFilter();
  }

  @Override
  public Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys) {
    return skeletonFileReader.filterRowKeys(candidateRowKeys);
  }

  @Override
  public ClosableIterator<HoodieRecord<T>> getRecordIterator(HoodieSchema readerSchema, HoodieSchema requestedSchema) throws IOException {
    ClosableIterator<HoodieRecord<T>> skeletonIterator = skeletonFileReader.getRecordIterator(readerSchema, requestedSchema);
    ClosableIterator<HoodieRecord<T>> dataFileIterator = dataFileReader.getRecordIterator(HoodieSchema.removeMetadataFields(readerSchema), requestedSchema);
    return new HoodieBootstrapRecordIterator<T>(skeletonIterator, dataFileIterator, readerSchema, partitionFields, partitionValues) {
      @Override
      protected void setPartitionPathField(int position, Object fieldValue, T row) {
        setPartitionField(position, fieldValue, row);
      }
    };
  }

  public ClosableIterator<HoodieRecord<T>> getRecordIterator(HoodieSchema schema) throws IOException {
    ClosableIterator<HoodieRecord<T>> skeletonIterator = skeletonFileReader.getRecordIterator(schema);
    ClosableIterator<HoodieRecord<T>> dataFileIterator = dataFileReader.getRecordIterator(dataFileReader.getSchema());
    return new HoodieBootstrapRecordIterator<T>(skeletonIterator, dataFileIterator, schema, partitionFields, partitionValues) {
      @Override
      protected void setPartitionPathField(int position, Object fieldValue, T row) {
        setPartitionField(position, fieldValue, row);
      }
    };
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() throws IOException {
    HoodieSchema schema = HoodieSchemaUtils.getRecordKeySchema();
    ClosableIterator<HoodieRecord<T>> skeletonIterator = skeletonFileReader.getRecordIterator(schema, schema);
    return new ClosableIterator<String>() {
      @Override
      public void close() {
        skeletonIterator.close();
      }

      @Override
      public boolean hasNext() {
        return skeletonIterator.hasNext();
      }

      @Override
      public String next() {
        HoodieRecord<T> skeletonRecord = skeletonIterator.next();
        return skeletonRecord.getRecordKey(schema, HoodieRecord.RECORD_KEY_METADATA_FIELD);
      }
    };
  }

  protected abstract void setPartitionField(int position, Object fieldValue, T row);

  @Override
  public HoodieSchema getSchema() {
    // return merged schema (meta fields + data file schema)
    return HoodieSchemaUtils.addMetadataFields(dataFileReader.getSchema());
  }

  @Override
  public void close() {
    skeletonFileReader.close();
    dataFileReader.close();
  }

  @Override
  public long getTotalRecords() {
    return Math.min(skeletonFileReader.getTotalRecords(), dataFileReader.getTotalRecords());
  }
}
