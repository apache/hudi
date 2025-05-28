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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

/**
 * This is the merger that replaces PartialUpdateAvroPayload class.
 *
 * Payload clazz that is used for partial update Hudi Table.
 *
 * <p>Simplified partial update Logic:
 * <pre>
 *  1. #preCombine
 *  For records with the same record key in one batch
 *  or in the delta logs that belongs to same File Group,
 *  Checks whether one record's ordering value is larger than the other record.
 *  If yes, overwrites the existing one for specified fields that doesn't equal to null.
 *
 *  2. #combineAndGetUpdateValue
 *  For every incoming record with existing record in storage (same record key)
 *  Checks whether incoming record's ordering value is larger than the existing record.
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
 *  Result data after #preCombine or #combineAndGetUpdateValue:
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
 *  Result data after preCombine or combineAndGetUpdateValue:
 *      id      ts      name    price
 *      1       2       name_1  price_1
 * </pre>
 *
 * <p>Gotchas:
 * <p>In cases where a batch of records is preCombine before combineAndGetUpdateValue with the underlying records to be updated located in parquet files, the end states of records might not be as how
 * one will expect when applying a straightforward partial update.
 *
 * <p>Gotchas-Example:
 * <pre>
 *  -- Insertion order of records:
 *  INSERT INTO t1 VALUES (1, 'a1', 10, 1000);                          -- (1)
 *  INSERT INTO t1 VALUES (1, 'a1', 11, 999), (1, 'a1_0', null, 1001);  -- (2)
 *
 *  SELECT id, name, price, _ts FROM t1;
 *  -- One would the results to return:
 *  -- 1    a1_0    10.0    1001

 *  -- However, the results returned are:
 *  -- 1    a1_0    11.0    1001
 *
 *  -- This occurs as preCombine is applied on (2) first to return:
 *  -- 1    a1_0    11.0    1001
 *
 *  -- And this then combineAndGetUpdateValue with the existing oldValue:
 *  -- 1    a1_0    10.0    1000
 *
 *  -- To return:
 *  -- 1    a1_0    11.0    1001
 * </pre>
 */
public class PartialUpdateAvroMerger extends EventTimeBasedAvroRecordMerger {
  public static final PartialUpdateAvroMerger INSTANCE = new PartialUpdateAvroMerger();

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.PARTIAL_UPDATE_MERGE_STRATEGY_UUID;
  }

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord oldRecord,
                                                  Schema oldSchema,
                                                  HoodieRecord newRecord,
                                                  Schema newSchema,
                                                  TypedProperties props) throws IOException {
    Comparable newOrderingVal = newRecord.getOrderingValue(newSchema, props);
    Comparable oldOrderingVal = oldRecord.getOrderingValue(oldSchema, props);
    HoodieRecord lowOrderRecord = oldRecord;
    HoodieRecord highOrderRecord = newRecord;
    Schema lowOrderSchema = oldSchema;
    Schema highOrderSchema = newSchema;

    if (oldOrderingVal.compareTo(newOrderingVal) > 0) {
      lowOrderRecord = newRecord;
      lowOrderSchema = newSchema;
      highOrderRecord = oldRecord;
      highOrderSchema = oldSchema;
    }

    if (lowOrderRecord.isDelete(lowOrderSchema, props)
        || highOrderRecord.isDelete(highOrderSchema, props)) {
      return Option.of(Pair.of(highOrderRecord, highOrderSchema));
    } else {
      return Option.of(Pair.of(
          mergeRecord(lowOrderRecord, lowOrderSchema, highOrderRecord, highOrderSchema),
          highOrderSchema));
    }
  }

  HoodieRecord mergeRecord(HoodieRecord lowOrderRecord,
                           Schema lowOrderSchema,
                           HoodieRecord highOrderRecord,
                           Schema highOrderSchema) {
    // Currently assume there is no schema evolution, solve it in HUDI-9253
    ValidationUtils.checkArgument(
        lowOrderSchema.getFields().size() == highOrderSchema.getFields().size());
    return new HoodieAvroIndexedRecord(mergeIndexedRecord(
        (IndexedRecord) lowOrderRecord.data, (IndexedRecord) highOrderRecord.data, highOrderSchema));
  }

  protected IndexedRecord mergeIndexedRecord(IndexedRecord lowOrderRecord,
                                             IndexedRecord highOrderRecord,
                                             Schema schema) {
    GenericRecord result = new GenericData.Record(schema);
    for (int i = 0; i < schema.getFields().size(); i++) {
      Object lowVal = lowOrderRecord.get(i);
      Object highVal = highOrderRecord.get(i);
      // Start with lowOrderRecord value
      Object value = lowVal;
      // Override if highOrderRecord has a non-null value
      if (highVal != null) {
        value = highVal;
      }
      result.put(i, value);
    }
    return result;
  }
}
