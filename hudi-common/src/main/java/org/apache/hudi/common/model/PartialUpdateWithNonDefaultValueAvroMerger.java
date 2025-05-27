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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

public class PartialUpdateWithNonDefaultValueAvroMerger extends PartialUpdateAvroMerger {
  public static final PartialUpdateWithNonDefaultValueAvroMerger INSTANCE =
      new PartialUpdateWithNonDefaultValueAvroMerger();

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.PARTIAL_UPDATE_WITH_NON_DEFAULT_VALUE_MERGE_STRATEGY_UUID;
  }

  @Override
  protected IndexedRecord mergeIndexedRecord(IndexedRecord lowOrderRecord,
                                             IndexedRecord highOrderRecord,
                                             Schema schema) {
    GenericRecord result = new GenericData.Record(schema);
    for (int i = 0; i < schema.getFields().size(); i++) {
      Object lowVal = lowOrderRecord.get(i);
      Object highVal = highOrderRecord.get(i);
      // Start with lowOrderRecord value
      Object value = lowVal;
      // Override if highOrderRecord has a non-default value
      Object defaultValue = null;
      Schema.Field field = schema.getFields().get(i);
      if (field.hasDefaultValue()) {
        defaultValue = field.defaultVal();
      }
      if (highVal != null && highVal != defaultValue) {
        value = highVal;
      }
      result.put(i, value);
    }
    return result;
  }
}
