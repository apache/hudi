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

package org.apache.hudi.common.testutils.reader;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Properties;

public class HoodieAvroRecordTestMerger extends HoodieAvroRecordMerger {

  private String[] orderingFields;

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(
      HoodieRecord older,
      Schema oldSchema,
      HoodieRecord newer,
      Schema newSchema,
      TypedProperties props
  ) throws IOException {
    if (orderingFields == null) {
      orderingFields = ConfigUtils.getOrderingFields(props);
    }

    Comparable oldOrderingVal = older.getOrderingValue(oldSchema, props, orderingFields);
    Comparable newOrderingVal = newer.getOrderingValue(newSchema, props, orderingFields);

    // The record with higher ordering value is returned.
    if (oldOrderingVal == null || newOrderingVal.compareTo(oldOrderingVal) > 0) {
      return Option.of(Pair.of(newer, newSchema));
    } else if (newOrderingVal.compareTo(oldOrderingVal) < 0) {
      return Option.of(Pair.of(older, oldSchema));
    }

    // When their orderings are the same, we rely on the logic of the payload.
    return combineAndGetUpdateValue(older, newer, newSchema, props)
        .map(r -> Pair.of(new HoodieAvroIndexedRecord(r), r.getSchema()));
  }

  private Option<IndexedRecord> combineAndGetUpdateValue(
      HoodieRecord older,
      HoodieRecord newer,
      Schema schema,
      Properties props
  ) throws IOException {
    Option<IndexedRecord> previousAvroData = older
        .toIndexedRecord(schema, props)
        .map(HoodieAvroIndexedRecord::getData);

    if (!previousAvroData.isPresent()) {
      return newer
          .toIndexedRecord(schema, props)
          .map(HoodieAvroIndexedRecord::getData);
    }

    return ((HoodieAvroRecord) newer)
        .getData()
        .combineAndGetUpdateValue(
            previousAvroData.get(), schema, props);
  }
}
