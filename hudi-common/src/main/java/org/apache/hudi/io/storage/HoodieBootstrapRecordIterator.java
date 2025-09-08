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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;

import org.apache.avro.Schema;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

public abstract class HoodieBootstrapRecordIterator<T> implements ClosableIterator<HoodieRecord<T>> {

  protected ClosableIterator<HoodieRecord<T>> skeletonIterator;
  protected ClosableIterator<HoodieRecord<T>> dataFileIterator;
  private final Option<String[]> partitionFields;
  private final Object[] partitionValues;

  protected Schema schema;

  public HoodieBootstrapRecordIterator(ClosableIterator<HoodieRecord<T>> skeletonIterator,
                                       ClosableIterator<HoodieRecord<T>> dataFileIterator,
                                       Schema schema,
                                       Option<String[]> partitionFields,
                                       Object[] partitionValues) {
    this.skeletonIterator = skeletonIterator;
    this.dataFileIterator = dataFileIterator;
    this.schema = schema;
    this.partitionFields = partitionFields;
    this.partitionValues = partitionValues;
  }

  @Override
  public void close() {
    skeletonIterator.close();
    dataFileIterator.close();
  }

  @Override
  public boolean hasNext() {
    checkState(skeletonIterator.hasNext() == dataFileIterator.hasNext());
    return skeletonIterator.hasNext();
  }

  @Override
  public HoodieRecord<T> next() {
    HoodieRecord<T> dataRecord = dataFileIterator.next();
    HoodieRecord<T> skeletonRecord = skeletonIterator.next();
    HoodieRecord<T> ret = dataRecord.prependMetaFields(schema, schema,
        new MetadataValues().setCommitTime(skeletonRecord.getRecordKey(schema, HoodieRecord.COMMIT_TIME_METADATA_FIELD))
            .setCommitSeqno(skeletonRecord.getRecordKey(schema, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD))
            .setRecordKey(skeletonRecord.getRecordKey(schema, HoodieRecord.RECORD_KEY_METADATA_FIELD))
            .setPartitionPath(skeletonRecord.getRecordKey(schema, HoodieRecord.PARTITION_PATH_METADATA_FIELD))
            .setFileName(skeletonRecord.getRecordKey(schema, HoodieRecord.FILENAME_METADATA_FIELD)), null, false);
    if (partitionFields.isPresent()) {
      for (int i = 0; i < partitionValues.length; i++) {
        int position = schema.getField(partitionFields.get()[i]).pos();
        setPartitionPathField(position, partitionValues[i], ret.getData());
      }
    }
    return ret;
  }

  protected abstract void setPartitionPathField(int position, Object fieldValue, T row);
}
