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

package org.apache.hudi.table.format.mor.lsm;

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.lsm.RecordMergeWrapper;

import org.apache.avro.Schema;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class FlinkRecordMergerWrapper implements RecordMergeWrapper<RowData> {

  private final HoodieRecordMerger recordMerger;
  private final Schema oldSchema;
  private final Schema newSchema;
  private final TypedProperties props;

  private final boolean isIgnoreDelete;

  private Option<Pair<HoodieRecord, Schema>> nextRecord = Option.empty();

  public FlinkRecordMergerWrapper(
                                  boolean isIgnoreDelete,
                                  HoodieRecordMerger recordMerger,
                                  Schema oldSchema,
                                  Schema newSchema,
                                  TypedProperties props) {
    this.isIgnoreDelete = isIgnoreDelete;
    this.recordMerger = recordMerger;
    this.oldSchema = oldSchema;
    this.newSchema = newSchema;
    this.props = props;
  }

  @Override
  public Option<RowData> merge(List<HoodieRecord> recordGroup) {
    return null;
  }

  @Override
  public Option<RowData> merge(Iterator<HoodieRecord> sameKeyIterator) {
    while (sameKeyIterator.hasNext()) {
      HoodieFlinkRecord record = (HoodieFlinkRecord) sameKeyIterator.next();
      if (!nextRecord.isPresent()) {
        nextRecord = Option.of(Pair.of(record, oldSchema));
      } else {
        nextRecord = mergeInternal((HoodieFlinkRecord)nextRecord.get().getKey(), record);
      }
    }

    return nextRecord.map(pair -> {
      return (RowData)pair.getLeft().getData();
    });
  }

  @Override
  public void merge(HoodieRecord record) {
    HoodieFlinkRecord flinkRecord = (HoodieFlinkRecord) record;
    if (!nextRecord.isPresent()) {
      nextRecord = Option.of(Pair.of(flinkRecord, oldSchema));
    } else {
      nextRecord = mergeInternal((HoodieFlinkRecord) nextRecord.get().getKey(), flinkRecord);
    }
  }

  @Override
  public Option<RowData> getMergedResult() {
    try {
      if (this.isIgnoreDelete && nextRecord.get().getLeft().isDelete(newSchema, new Properties())) {
        return Option.empty();
      }
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage());
    }
    return nextRecord.map(pair -> (RowData) pair.getLeft().getData());
  }

  @Override
  public void reset() {
    this.nextRecord = Option.empty();
  }

  private Option<Pair<HoodieRecord, Schema>> mergeInternal(HoodieFlinkRecord oldRecord, HoodieFlinkRecord newRecord) {
    try {
      return recordMerger.merge(oldRecord, oldSchema, newRecord, newSchema, props);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }
}
