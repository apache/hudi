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

package org.apache.hudi;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.function.UnaryOperator;

import scala.Function1;

import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.spark.sql.HoodieInternalRowUtils.getCachedSchema;

/**
 * An abstract class implementing {@link HoodieReaderContext} to handle {@link InternalRow}s.
 * Subclasses need to implement {@link #getFileRecordIterator} with the reader logic.
 */
public abstract class BaseSparkInternalRowReaderContext extends HoodieReaderContext<InternalRow> {

  @Override
  public HoodieRecordMerger getRecordMerger(String mergerStrategy) {
    return HoodieSparkRecordMerger.getRecordMerger(mergerStrategy);
  }

  @Override
  public Object getValue(InternalRow row, Schema schema, String fieldName) {
    return getFieldValueFromInternalRow(row, schema, fieldName);
  }

  @Override
  public String getRecordKey(InternalRow row, Schema schema) {
    return getFieldValueFromInternalRow(row, schema, RECORD_KEY_METADATA_FIELD).toString();
  }

  @Override
  public HoodieRecord<InternalRow> constructHoodieRecord(Option<InternalRow> rowOption,
                                                         Map<String, Object> metadataMap) {
    if (!rowOption.isPresent()) {
      return new HoodieEmptyRecord<>(
          new HoodieKey((String) metadataMap.get(INTERNAL_META_RECORD_KEY),
              (String) metadataMap.get(INTERNAL_META_PARTITION_PATH)),
          HoodieRecord.HoodieRecordType.SPARK);
    }

    Schema schema = (Schema) metadataMap.get(INTERNAL_META_SCHEMA);
    InternalRow row = rowOption.get();
    return new HoodieSparkRecord(row, HoodieInternalRowUtils.getCachedSchema(schema));
  }

  @Override
  public InternalRow seal(InternalRow internalRow) {
    return internalRow.copy();
  }

  private Object getFieldValueFromInternalRow(InternalRow row, Schema recordSchema, String fieldName) {
    StructType structType = getCachedSchema(recordSchema);
    scala.Option<HoodieUnsafeRowUtils.NestedFieldPath> cachedNestedFieldPath =
        HoodieInternalRowUtils.getCachedPosList(structType, fieldName);
    if (cachedNestedFieldPath.isDefined()) {
      HoodieUnsafeRowUtils.NestedFieldPath nestedFieldPath = cachedNestedFieldPath.get();
      return HoodieUnsafeRowUtils.getNestedInternalRowValue(row, nestedFieldPath);
    } else {
      return null;
    }
  }

  @Override
  public UnaryOperator<InternalRow> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
    Function1<InternalRow, UnsafeRow> unsafeRowWriter =
        HoodieInternalRowUtils.getCachedUnsafeRowWriter(getCachedSchema(from), getCachedSchema(to), renamedColumns);
    return row -> (InternalRow) unsafeRowWriter.apply(row);

  }

  protected UnaryOperator<InternalRow> getIdentityProjection() {
    return row -> row;
  }
}
