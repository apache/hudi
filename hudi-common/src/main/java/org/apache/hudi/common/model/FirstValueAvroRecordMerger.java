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

import org.apache.avro.Schema;

import java.io.IOException;

/**
 * The following is the description of FirstValueAvroPayload.
 *
 * Payload clazz that is used for Hudi Table.
 *
 * <p>Simplified FirstValueAvroPayload Logic:
 * <pre>
 *
 *  Illustration with simple data.
 *  the order field is 'ts', recordkey is 'id' and schema is :
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
 *      1       1       name_2  price_2
 *
 *  Result data after #preCombine or #combineAndGetUpdateValue:
 *      id      ts      name    price
 *      1       1       name_1  price_1
 *
 *  If precombine is the same, would keep the first one record
 *
 *  case 2
 *  Current data:
 *      id      ts      name    price
 *      1       1       name_1  price_1
 *  Insert data:
 *      id      ts      name    price
 *      1       2       name_2  price_2
 *
 *  Result data after preCombine or combineAndGetUpdateValue:
 *      id      ts      name    price
 *      1       2       name_2  price_2
 *
 *  The other functionalities are inherited from DefaultHoodieRecordPayload.
 * </pre>
 */
public class FirstValueAvroRecordMerger extends EventTimeBasedAvroRecordMerger {
  public static FirstValueAvroRecordMerger INSTANCE = new FirstValueAvroRecordMerger();
  protected boolean shouldKeepNewerRecord(HoodieRecord oldRecord,
                                          HoodieRecord newRecord,
                                          Schema oldSchema,
                                          Schema newSchema,
                                          TypedProperties props) throws IOException {
    if (isCommitTimeOrderingDelete(newRecord, newSchema, props)) {
      // handle records coming from DELETE statements(the orderingVal is constant 0)
      return true;
    }
    return newRecord.getOrderingValue(newSchema, props)
        .compareTo(oldRecord.getOrderingValue(oldSchema, props)) > 0;
  }

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.FIRST_VALUE_MERGE_STRATEGY_UUID;
  }
}
