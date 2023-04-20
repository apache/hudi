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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;

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
  public Set<String> filterRowKeys(Set<String> candidateRowKeys) {
    return skeletonFileReader.filterRowKeys(candidateRowKeys);
  }

  @Override
  public ClosableIterator<HoodieRecord<T>> getRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
    ClosableIterator<HoodieRecord<T>> skeletonIterator = skeletonFileReader.getRecordIterator(readerSchema, requestedSchema);
    ClosableIterator<HoodieRecord<T>> dataFileIterator = dataFileReader.getRecordIterator(HoodieAvroUtils.removeMetadataFields(readerSchema), requestedSchema);
    return new ClosableIterator<HoodieRecord<T>>() {
      @Override
      public void close() {
        skeletonIterator.close();
        dataFileIterator.close();
      }

      @Override
      public boolean hasNext() {
        return skeletonIterator.hasNext() && dataFileIterator.hasNext();
      }

      @Override
      public HoodieRecord<T> next() {
        HoodieRecord<T> dataRecord = dataFileIterator.next();
        HoodieRecord<T> skeletonRecord = skeletonIterator.next();
        HoodieRecord<T> ret = dataRecord.prependMetaFields(readerSchema, readerSchema,
            new MetadataValues().setCommitTime(skeletonRecord.getRecordKey(readerSchema, HoodieRecord.COMMIT_TIME_METADATA_FIELD))
                .setCommitSeqno(skeletonRecord.getRecordKey(readerSchema, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD))
                .setRecordKey(skeletonRecord.getRecordKey(readerSchema, HoodieRecord.RECORD_KEY_METADATA_FIELD))
                .setPartitionPath(skeletonRecord.getRecordKey(readerSchema, HoodieRecord.PARTITION_PATH_METADATA_FIELD))
                .setFileName(skeletonRecord.getRecordKey(readerSchema, HoodieRecord.FILENAME_METADATA_FIELD)), null);
        if (partitionFields.isPresent()) {
          for (int i = 0; i < partitionValues.length; i++) {
            int position = readerSchema.getField(partitionFields.get()[i]).pos();
            setPartitionField(position, partitionValues[i], ret.getData());
          }
        }
        return ret;
      }
    };
  }

  protected abstract void setPartitionField(int position, Object fieldValue, T row);

  @Override
  public Schema getSchema() {
    // return merged schema (meta fields + data file schema)
    return HoodieAvroUtils.addMetadataFields(dataFileReader.getSchema());
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
