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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.List;

/**
 * subclass of OverwriteWithLatestAvroPayload.
 *
 * <ol>
 * <li>preCombine - When more than one HoodieRecord have the same HoodieKey, this function combines all fields(which is not null)
 * before attempting to insert/upsert.
 * eg: 1)
 * Before:
 * id name  age   ts
 * 1  Karl  null  0.0
 * 1  null  18    0.0
 * After:
 * id name  age   ts
 * 1  Karl  18    0.0
 * </ol>
 */
public class UpdatePrecombineAvroPayload extends OverwriteWithLatestAvroPayload {
  public UpdatePrecombineAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public UpdatePrecombineAvroPayload(Option<GenericRecord> record) {
    super(record);
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload another, Schema schema) throws IOException {
    // pick the payload with greatest ordering value and aggregate all the fields,choosing the
    // value that is not null
    GenericRecord thisValue = HoodieAvroUtils.bytesToAvro(this.recordBytes, schema);
    GenericRecord anotherValue = HoodieAvroUtils.bytesToAvro(another.recordBytes, schema);
    List<Schema.Field> fields = schema.getFields();

    if (another.orderingVal.compareTo(orderingVal) > 0) {
      GenericRecord anotherRoc = combineAllFields(fields, anotherValue, thisValue);
      another.recordBytes = HoodieAvroUtils.avroToBytes(anotherRoc);
      return another;
    } else {
      GenericRecord thisRoc = combineAllFields(fields, thisValue, anotherValue);
      this.recordBytes = HoodieAvroUtils.avroToBytes(thisRoc);
      return this;
    }
  }

  public GenericRecord combineAllFields(List<Schema.Field> fields, GenericRecord priorRec, GenericRecord secPriorRoc) {
    for (int i = 0; i < fields.size(); i++) {
      Object priorValue = priorRec.get(fields.get(i).name());
      Object secPriorValue = secPriorRoc.get(fields.get(i).name());
      Object defaultVal = fields.get(i).defaultVal();
      if (overwriteField(priorValue, defaultVal) && !overwriteField(secPriorValue, defaultVal)) {
        priorRec.put(fields.get(i).name(), secPriorValue);
      }
    }
    return priorRec;
  }
}
