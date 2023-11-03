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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID;

/**
 * An abstract class implementing {@link HoodieReaderContext} to handle {@link InternalRow}s.
 * Subclasses need to implement {@link #getFileRecordIterator} with the reader logic.
 */
public abstract class BaseSparkInternalRowReaderContext extends HoodieReaderContext<InternalRow> {
  @Override
  public FileSystem getFs(String path, Configuration conf) {
    return FSUtils.getFs(path, conf);
  }

  @Override
  public HoodieRecordMerger getRecordMerger(String mergerStrategy) {
    switch (mergerStrategy) {
      case DEFAULT_MERGER_STRATEGY_UUID:
        return new HoodieSparkRecordMerger();
      default:
        throw new HoodieException("The merger strategy UUID is not supported: " + mergerStrategy);
    }
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
  public Comparable getOrderingValue(Option<InternalRow> rowOption,
                                     Map<String, Object> metadataMap,
                                     Schema schema,
                                     TypedProperties props) {
    if (metadataMap.containsKey(INTERNAL_META_ORDERING_FIELD)) {
      return (Comparable) metadataMap.get(INTERNAL_META_ORDERING_FIELD);
    }

    if (!rowOption.isPresent()) {
      return 0;
    }

    String orderingFieldName = ConfigUtils.getOrderingField(props);
    Object value = getFieldValueFromInternalRow(rowOption.get(), schema, orderingFieldName);
    return value != null ? (Comparable) value : 0;
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
    StructType structType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    scala.Option<HoodieUnsafeRowUtils.NestedFieldPath> cachedNestedFieldPath =
        HoodieInternalRowUtils.getCachedPosList(structType, fieldName);
    if (cachedNestedFieldPath.isDefined()) {
      HoodieUnsafeRowUtils.NestedFieldPath nestedFieldPath = cachedNestedFieldPath.get();
      return HoodieUnsafeRowUtils.getNestedInternalRowValue(row, nestedFieldPath);
    } else {
      return null;
    }
  }
}
