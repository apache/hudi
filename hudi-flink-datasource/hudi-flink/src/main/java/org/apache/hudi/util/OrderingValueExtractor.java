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

package org.apache.hudi.util;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

/**
 * Interface for extracting ordering value from RowData.
 */
public interface OrderingValueExtractor extends Serializable {
  Comparable<?> getOrderingValue(RowData rowData);

  static OrderingValueExtractor getInstance(Configuration conf, RowType rowType) {
    String fieldName = OptionsResolver.getPreCombineField(conf);
    boolean needCombine = conf.getBoolean(FlinkOptions.PRE_COMBINE)
        || WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION)) == WriteOperationType.UPSERT;
    boolean shouldCombine = needCombine && !StringUtils.isNullOrEmpty(fieldName);
    if (!shouldCombine) {
      // returns a natual order value extractor.
      return rowData -> HoodieRecord.DEFAULT_ORDERING_VALUE;
    }
    final int fieldPos = rowType.getFieldNames().indexOf(fieldName);
    final LogicalType fieldType = rowType.getTypeAt(fieldPos);
    final RowData.FieldGetter orderingValueFieldGetter = RowData.createFieldGetter(fieldType, fieldPos);
    boolean utcTimezone = conf.get(FlinkOptions.WRITE_UTC_TIMEZONE);

    return rowData -> (Comparable<?>) DataTypeUtils.resolveOrderingValue(
        fieldType, orderingValueFieldGetter.getFieldOrNull(rowData), utcTimezone);
  }
}
