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

package org.apache.hudi.common.model;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValueUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.util.Properties;

/**
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
public class FirstValueAvroPayload extends DefaultHoodieRecordPayload {

  public FirstValueAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public FirstValueAvroPayload(Option<GenericRecord> record) {
    super(record);
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue) {
    if (oldValue.recordBytes.length == 0) {
      // use natural order for delete record
      return this;
    }
    if (oldValue.orderingVal.compareTo(orderingVal) >= 0) {
      // pick the payload with greatest ordering value
      return oldValue;
    } else {
      return this;
    }
  }

  @Override
  protected boolean needUpdatingPersistedRecord(IndexedRecord currentValue,
                                                Option<IndexedRecord> incomingRecord, Properties properties) {
    /*
     * Combining strategy here returns currentValue on disk if incoming record is older absolutely.
     * The incoming record can be either a delete (sent as an upsert with _hoodie_is_deleted set to true)
     * or an insert/update record. In any case, if it is older absolutely than the record in disk, the currentValue
     * in disk is returned (to be rewritten with new commit time).
     */
    String orderField = ConfigUtils.getOrderingField(properties);
    if (orderField == null) {
      return true;
    }
    boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(properties.getProperty(
            KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
            KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
    Object persistedOrderingVal = HoodieAvroUtils.getNestedFieldVal((GenericRecord) currentValue,
            orderField,
            true, consistentLogicalTimestampEnabled);
    Comparable incomingOrderingVal = incomingRecord.map(record -> (Comparable) HoodieAvroUtils.getNestedFieldVal((GenericRecord) record,
        orderField,
        true, consistentLogicalTimestampEnabled)).orElse(orderingVal);
    if (incomingRecord.isEmpty() && DEFAULT_VALUE.equals(incomingOrderingVal)) {
      return true;
    }
    Pair<Comparable, Comparable> comparablePair = OrderingValueUtils.canonicalizeOrderingValue((Comparable) persistedOrderingVal, incomingOrderingVal);
    persistedOrderingVal = comparablePair.getLeft();
    incomingOrderingVal = comparablePair.getRight();
    return persistedOrderingVal == null || ((Comparable) persistedOrderingVal).compareTo(incomingOrderingVal) < 0;
  }
}
