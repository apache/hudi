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

package org.apache.hudi.io.lsm;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class SparkRecordMergeWrapper implements RecordMergeWrapper<InternalRow> {
  private final HoodieRecordMerger recordMerger;
  private final Schema oldSchema;
  private final Schema newSchema;
  private final TypedProperties props;
  private final StructType structTypeSchema;

  private final boolean isIgnoreDelete;

  private Option<Pair<HoodieRecord, Schema>> nextRecord = Option.empty();

  public SparkRecordMergeWrapper(boolean isIgnoreDelete,
                                 HoodieRecordMerger recordMerger,
                                 Schema oldSchema,
                                 Schema newSchema,
                                 TypedProperties props,
                                 StructType structTypeSchema) {
    this.isIgnoreDelete = isIgnoreDelete;
    this.recordMerger = recordMerger;
    this.oldSchema = oldSchema;
    this.newSchema = newSchema;
    this.props = props;
    this.structTypeSchema = structTypeSchema;
  }

  @Override
  public Option<InternalRow> merge(List<HoodieRecord> recordGroup) {

    for (HoodieRecord hoodieRecord : recordGroup) {
      HoodieSparkRecord sparkRecord = (HoodieSparkRecord) hoodieRecord;
      if (!nextRecord.isPresent()) {
        nextRecord = Option.of(Pair.of(sparkRecord, oldSchema));
      } else {
        nextRecord = mergeInternal((HoodieSparkRecord)nextRecord.get().getKey(), sparkRecord);
      }
    }

    return nextRecord.map(pair -> {
      StructType schema = HoodieInternalRowUtils.getCachedSchema(pair.getRight());
      UnsafeProjection projection = HoodieInternalRowUtils.getCachedUnsafeProjection(schema, structTypeSchema);
      return projection.apply((InternalRow)pair.getLeft().getData());
    });
  }

  @Override
  public Option<InternalRow> merge(Iterator<HoodieRecord> sameKeyIterator) {
    while (sameKeyIterator.hasNext()) {
      HoodieSparkRecord sparkRecord = (HoodieSparkRecord) sameKeyIterator.next();
      if (!nextRecord.isPresent()) {
        nextRecord = Option.of(Pair.of(sparkRecord, oldSchema));
      } else {
        nextRecord = mergeInternal((HoodieSparkRecord)nextRecord.get().getKey(), sparkRecord);
      }
    }

    return nextRecord.map(pair -> {
      return (InternalRow)pair.getLeft().getData();
    });
  }

  @Override
  public void merge(HoodieRecord record) {
    HoodieSparkRecord sparkRecord = (HoodieSparkRecord) record;
    if (!nextRecord.isPresent()) {
      nextRecord = Option.of(Pair.of(sparkRecord, oldSchema));
    } else {
      nextRecord = mergeInternal((HoodieSparkRecord) nextRecord.get().getKey(), sparkRecord);
    }
  }

  @Override
  public Option<InternalRow> getMergedResult() {
    HoodieRecord left = nextRecord.get().getLeft();
    try {
      if (isIgnoreDelete && left.isDelete(newSchema, new Properties())) {
        return Option.empty();
      }
    } catch (Exception e) {
      throw new HoodieIOException(e.getMessage());
    }
    return nextRecord.map(pair -> (InternalRow) pair.getLeft().getData());
  }

  @Override
  public void reset() {
    this.nextRecord = Option.empty();
  }

  private Option<Pair<HoodieRecord, Schema>> mergeInternal(HoodieSparkRecord oldRecord, HoodieSparkRecord newRecord) {
    try {
      return recordMerger.merge(oldRecord, oldSchema, newRecord, newSchema, props);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }
}
