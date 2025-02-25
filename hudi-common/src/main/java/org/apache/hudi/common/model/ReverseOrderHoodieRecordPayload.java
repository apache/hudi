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
import org.apache.hudi.common.util.Option;

/**
 * {@link HoodieRecordPayload} impl that honors ordering field in both preCombine and combineAndGetUpdateValue.
 * This class extends DefaultHoodieRecordPayload implementation but overrides comparable method that determines the order.
 *
 * 1. preCombine - Keeps the oldest record for a key, based on an ordering field
 * 2. combineAndGetUpdateValue/getInsertValue - Chooses the oldest record based on ordering field value.
 */
public class ReverseOrderHoodieRecordPayload extends DefaultHoodieRecordPayload {

  public ReverseOrderHoodieRecordPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public ReverseOrderHoodieRecordPayload(Option<GenericRecord> record) {
    super(record);
  }

  @Override
  protected OverwriteWithLatestAvroPayload getRecordWithLatestAvroPayload(OverwriteWithLatestAvroPayload oldValue) {
    if (oldValue.orderingVal.compareTo(orderingVal) < 0) {
      // pick the payload with the smallest ordering value
      return oldValue;
    } else {
      return this;
    }
  }

  /**
   * Overrides the default implementation so that when a record with lower value appear it overrides.
   * If updateOnSameOrderingField is true, then incoming record is returned when payload ordering field is the same.
   * @param persistedOrderingVal record present in Disk
   * @param incomingOrderingVal record part of input payload
   * @return true if the incoming record is older than existing record(persisted entry).
   */
  @Override
  protected boolean compareOrderingVal(Comparable persistedOrderingVal, Comparable incomingOrderingVal,
                                       boolean updateOnSameOrderingField) {
    if (persistedOrderingVal == null) {
      return true;
    } else {
      int compareVal = persistedOrderingVal.compareTo(incomingOrderingVal);
      if (updateOnSameOrderingField) {
        return compareVal >= 0;
      } else {
        return compareVal > 0;
      }
    }
  }
}
