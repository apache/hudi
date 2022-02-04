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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.hudi.TypeUtils.unsafeCast;

public class HoodieAvroRecord<T extends HoodieRecordPayload> extends HoodieRecord<T> {
  public HoodieAvroRecord(HoodieKey key, T data) {
    super(key, data);
  }

  public HoodieAvroRecord(HoodieKey key, T data, HoodieOperation operation) {
    super(key, data, operation);
  }

  public HoodieAvroRecord(HoodieRecord<T> record) {
    super(record);
  }

  public HoodieAvroRecord() {
  }

  @Override
  public HoodieRecord<T> newInstance() {
    return new HoodieAvroRecord<>(this);
  }

  @Override
  public T getData() {
    if (data == null) {
      throw new IllegalStateException("Payload already deflated for record.");
    }
    return data;
  }

  //////////////////////////////////////////////////////////////////////////////

  //
  // NOTE: This method duplicates those ones of the HoodieRecordPayload and are placed here
  //       for the duration of RFC-46 implementation, until migration off `HoodieRecordPayload`
  //       is complete
  //
  // TODO cleanup

  // NOTE: This method is assuming semantic that `preCombine` operation is bound to pick one or the other
  //       object, and may not create a new one
  @Override
  public HoodieRecord<T> preCombine(HoodieRecord<T> previousRecord) {
    T picked = unsafeCast(getData().preCombine(previousRecord.getData()));
    return picked.equals(getData()) ? this : previousRecord;
  }

  // NOTE: This method is assuming semantic that only records bearing the same (partition, key) could
  //       be combined
  @Override
  public Option<HoodieRecord<T>> combineAndGetUpdateValue(HoodieRecord<T> previousRecord, Schema schema, Properties props) throws IOException {
    ValidationUtils.checkState(Objects.equals(getKey(), previousRecord.getKey()));

    Option<IndexedRecord> previousRecordAvroPayload = previousRecord.getData().getInsertValue(schema, props);
    if (!previousRecordAvroPayload.isPresent()) {
      return Option.empty();
    }

    return getData().combineAndGetUpdateValue(previousRecordAvroPayload.get(), schema, props)
        .map(combinedAvroPayload -> {
          // NOTE: It's assumed that records aren't precombined more than once in its lifecycle,
          //       therefore we simply stub out precombine value here
          int newPreCombineVal = 0;
          T combinedPayload = unsafeCast(
              ReflectionUtils.loadPayload(
                  getData().getClass().getCanonicalName(),
                  new Object[]{combinedAvroPayload, newPreCombineVal /* NOTE */},
                  GenericRecord.class,
                  Comparable.class));
          return new HoodieAvroRecord<>(getKey(), combinedPayload, getOperation());
        });
  }

  public Option<Map<String, String>> getMetadata() {
    return getData().getMetadata();
  }

  //////////////////////////////////////////////////////////////////////////////
}
