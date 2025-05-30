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
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;

import java.util.List;

/**
 * Description from original class: OverwriteNonDefaultsWithLatestAvroPayload
 *
 * subclass of OverwriteWithLatestAvroPayload.
 *
 * <ol>
 * <li>preCombine - Picks the latest delta record for a key, based on an ordering field;
 * <li>combineAndGetUpdateValue/getInsertValue - overwrite the storage for the specified fields
 * with the fields from the latest delta record that doesn't equal defaultValue.
 * </ol>
 */
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
                                             Schema lowOrderSchema,
                                             Schema highOrderSchema) {
    int lowOrderSchemaFieldSize = lowOrderSchema.getFields().size();
    int highOrderSchemaFieldSize = highOrderSchema.getFields().size();
    final Schema finalSchema =
        lowOrderSchemaFieldSize > highOrderSchemaFieldSize ? lowOrderSchema : highOrderSchema;
    final GenericRecordBuilder builder = new GenericRecordBuilder(finalSchema);

    // Assumptions:
    // 1. Schema differences are ONLY due to meta fields.
    // 2. Meta fields are consecutive and in the same order.
    // 3. Meta fields start from index 0 if exist.
    int indexForLow = 0;
    int indexForHigh = 0;
    if (lowOrderSchemaFieldSize > highOrderSchemaFieldSize) {
      indexForHigh -= (lowOrderSchemaFieldSize - highOrderSchemaFieldSize);
    } else {
      indexForLow -= (highOrderSchemaFieldSize - lowOrderSchemaFieldSize);
    }

    // Loop from both schemas.
    int index = 0;
    List<Schema.Field> fields = finalSchema.getFields();
    while (indexForHigh < highOrderSchemaFieldSize && indexForLow < lowOrderSchemaFieldSize) {
      Object lowVal = indexForLow >= 0 ? lowOrderRecord.get(indexForLow) : null;
      Object highVal = indexForHigh >= 0 ? highOrderRecord.get(indexForHigh) : null;
      // Start with lowOrderRecord value
      Object value = lowVal;
      // Override if highOrderRecord has a non-default value.
      Schema.Field field = fields.get(index);
      if (highVal != null && highVal != field.defaultVal()) {
        value = highVal;
      }
      // Set the field.
      builder.set(fields.get(index), value);
      // Move indexes.
      index++;
      indexForLow++;
      indexForHigh++;
    }
    return builder.build();
  }
}
