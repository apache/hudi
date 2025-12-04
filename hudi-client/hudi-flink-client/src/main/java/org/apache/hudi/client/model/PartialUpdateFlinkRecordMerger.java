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

package org.apache.hudi.client.model;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.util.RowDataAvroQueryContexts;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * Record merger for Flink HoodieRecord that implements event time based partial update merging strategy.
 * Payload clazz that is used for partial update Hudi Table.
 *
 * <p>Simplified partial update Logic:
 * <pre>
 *  For every incoming record, checks whether its ordering value is larger than the existing record.
 *  If yes, overwrites the existing one for specified fields that doesn't equal to null.
 *  else overwrites the incoming one with the existing record for specified fields that doesn't equal to null
 *  and returns a merged record.
 *
 *  Illustration with simple data.
 *  let's say the order field is 'ts' and schema is :
 *  {
 *    [
 *      {"name":"id","type":"string"},
 *      {"name":"ts","type":"long"},
 *      {"name":"name","type":"string"},
 *      {"name":"price","type":"string"}
 *    ]
 *  }
 *
 *  case 1
 *  Current data:
 *      id      ts      name    price
 *      1       1       name_1  price_1
 *  Insert data:
 *      id      ts      name    price
 *      1       2       null    price_2
 *
 *  Result data after merging:
 *      id      ts      name    price
 *      1       2       name_1  price_2
 *
 *  case 2
 *  Current data:
 *      id      ts      name    price
 *      1       2       name_1  null
 *  Insert data:
 *      id      ts      name    price
 *      1       1       null    price_1
 *
 *  Result data after merging:
 *      id      ts      name    price
 *      1       2       name_1  price_1
 * </pre>
 */
public class PartialUpdateFlinkRecordMerger extends HoodieFlinkRecordMerger {

  @Override
  public String getMergingStrategy() {
    return EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
  }

  @Override
  public <T> BufferedRecord<T> merge(
      BufferedRecord<T> older,
      BufferedRecord<T> newer,
      RecordContext<T> recordContext,
      TypedProperties props) throws IOException {
    if (older.getOrderingValue().compareTo(newer.getOrderingValue()) > 0) {
      if (older.isDelete() || newer.isDelete()) {
        return older;
      } else {
        HoodieSchema oldSchema = recordContext.getSchemaFromBufferRecord(older);
        HoodieSchema newSchema = recordContext.getSchemaFromBufferRecord(newer);
        return mergeRecord(newer, newSchema, older, oldSchema, newSchema, recordContext, props);
      }
    } else {
      if (newer.isDelete() || older.isDelete()) {
        return newer;
      } else {
        HoodieSchema oldSchema = recordContext.getSchemaFromBufferRecord(older);
        HoodieSchema newSchema = recordContext.getSchemaFromBufferRecord(newer);
        return mergeRecord(older, oldSchema, newer, newSchema, newSchema, recordContext, props);
      }
    }
  }

  private <T> BufferedRecord<T> mergeRecord(
      BufferedRecord<T> lowOrderRecord,
      HoodieSchema lowOrderSchema,
      BufferedRecord<T> highOrderRecord,
      HoodieSchema highOrderSchema,
      HoodieSchema newSchema,
      RecordContext<T> recordContext,
      TypedProperties props) {
    // Assumptions: there is no schema evolution, will solve it in HUDI-9253
    // 1. schema differences are ONLY due to meta fields;
    // 2. meta fields are consecutive and in the same order;
    // 3. meta fields start from index 0 if exists.
    int lowOrderArity = lowOrderSchema.getFields().size();
    int highOrderArity = highOrderSchema.getFields().size();
    // Merged record is always created with new schema, which may not contain metadata fields.
    // The merged record has no metadata fields for this case, and metadata fields will be prepended
    // later in the file writer.
    int mergedArity = newSchema.getFields().size();
    boolean utcTimezone = Boolean.parseBoolean(props.getProperty("read.utc-timezone", "true"));
    RowData.FieldGetter[] fieldGetters = RowDataAvroQueryContexts.fromAvroSchema(newSchema.toAvroSchema(), utcTimezone).fieldGetters();

    int lowOrderIdx = 0;
    int highOrderIdx = 0;
    RowData.FieldGetter[] lowOrderFieldGetters = fieldGetters;
    RowData.FieldGetter[] highOrderFieldGetters = fieldGetters;
    // shift start index for merging if there is schema discrepancy
    if (lowOrderArity != mergedArity) {
      lowOrderIdx += lowOrderArity - mergedArity;
      lowOrderFieldGetters = RowDataAvroQueryContexts.fromAvroSchema(lowOrderSchema.toAvroSchema(), utcTimezone).fieldGetters();
    } else if (highOrderArity != mergedArity) {
      highOrderIdx += highOrderArity - mergedArity;
      highOrderFieldGetters = RowDataAvroQueryContexts.fromAvroSchema(highOrderSchema.toAvroSchema(), utcTimezone).fieldGetters();
    }

    RowData lowOrderRow = (RowData) lowOrderRecord.getRecord();
    RowData highOrderRow = (RowData) highOrderRecord.getRecord();
    GenericRowData mergedRow = new GenericRowData(mergedArity);
    for (int i = 0; i < mergedArity; i++) {
      Object fieldValWithHighOrder = highOrderFieldGetters[highOrderIdx].getFieldOrNull(highOrderRow);
      if (fieldValWithHighOrder != null) {
        mergedRow.setField(i, fieldValWithHighOrder);
      } else {
        mergedRow.setField(i, lowOrderFieldGetters[lowOrderIdx].getFieldOrNull(lowOrderRow));
      }
      lowOrderIdx++;
      highOrderIdx++;
    }
    return BufferedRecords.fromEngineRecord((T) mergedRow, newSchema, recordContext, highOrderRecord.getOrderingValue(), highOrderRecord.getRecordKey(), highOrderRecord.isDelete());
  }
}
