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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.util.RowDataAvroQueryContexts;

import org.apache.avro.Schema;
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
    return CUSTOM_MERGE_STRATEGY_UUID;
  }

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(
      HoodieRecord older,
      Schema oldSchema,
      HoodieRecord newer,
      Schema newSchema,
      TypedProperties props) throws IOException {
    // Note: can be removed if we can ensure the type from invoker.
    ValidationUtils.checkArgument(older.getRecordType() == HoodieRecord.HoodieRecordType.FLINK);
    ValidationUtils.checkArgument(newer.getRecordType() == HoodieRecord.HoodieRecordType.FLINK);

    if (older.getOrderingValue(oldSchema, props).compareTo(newer.getOrderingValue(newSchema, props)) > 0) {
      if (older.isDelete(oldSchema, props) || newer.isDelete(newSchema, props)) {
        return Option.of(Pair.of(older, oldSchema));
      } else {
        HoodieRecord mergedRecord = mergeRecord(newer, newSchema, older, oldSchema, props);
        return Option.of(Pair.of(mergedRecord, oldSchema));
      }
    } else {
      if (newer.isDelete(newSchema, props) || older.isDelete(oldSchema, props)) {
        return Option.of(Pair.of(newer, newSchema));
      } else {
        HoodieRecord mergedRecord = mergeRecord(older, oldSchema, newer, newSchema, props);
        return Option.of(Pair.of(mergedRecord, oldSchema));
      }
    }
  }

  private HoodieRecord mergeRecord(
      HoodieRecord lowOrderRecord,
      Schema lowOrderSchema,
      HoodieRecord highOrderRecord,
      Schema highOrderSchema,
      TypedProperties props) {
    // merge older and newer, currently assume there is no schema evolution, solve it in HUDI-9253
    ValidationUtils.checkArgument(lowOrderSchema.getFields().size() == highOrderSchema.getFields().size());
    RowData mergedRow = mergeRowData((RowData) lowOrderRecord.getData(), (RowData) highOrderRecord.getData(), highOrderSchema, props);
    return new HoodieFlinkRecord(
        highOrderRecord.getKey(),
        highOrderRecord.getOperation(),
        highOrderRecord.getOrderingValue(highOrderSchema, props),
        mergedRow);
  }

  private RowData mergeRowData(RowData lowOrderRow, RowData highOrderRow, Schema schema, TypedProperties props) {
    boolean utcTimezone = Boolean.parseBoolean(props.getProperty("read.utc-timezone", "true"));
    GenericRowData result = new GenericRowData(schema.getFields().size());
    RowData.FieldGetter[] fieldGetters = RowDataAvroQueryContexts.fromAvroSchema(schema, utcTimezone).fieldGetters();
    for (int i = 0; i < fieldGetters.length; i++) {
      result.setField(i, fieldGetters[i].getFieldOrNull(lowOrderRow));
      Object fieldValWithHighOrder = fieldGetters[i].getFieldOrNull(highOrderRow);
      if (fieldValWithHighOrder != null) {
        result.setField(i, fieldValWithHighOrder);
      }
    }
    return result;
  }
}
