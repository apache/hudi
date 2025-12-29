/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.avro.processors;

import org.apache.hudi.avro.AvroLogicalTypeEnum;
import org.apache.hudi.common.schema.HoodieSchema;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.util.List;

public abstract class DurationLogicalTypeProcessor extends JsonFieldProcessor {
  private static final int NUM_ELEMENTS_FOR_DURATION_TYPE = 3;

  /**
   * We expect the input to be a list of 3 integers representing months, days and milliseconds.
   */
  protected boolean isValidDurationInput(Object value) {
    if (!(value instanceof List<?>)) {
      return false;
    }
    List<?> list = (List<?>) value;
    if (list.size() != NUM_ELEMENTS_FOR_DURATION_TYPE) {
      return false;
    }
    for (Object element : list) {
      if (!(element instanceof Integer)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if the given schema is a valid decimal type configuration.
   */
  protected static boolean isValidDurationTypeConfig(HoodieSchema schema) {
    String durationTypeName = AvroLogicalTypeEnum.DURATION.getValue();
    LogicalType durationType = schema.toAvroSchema().getLogicalType();
    String durationTypeProp = (String) schema.getProp("logicalType");
    // 1. The Avro type should be "Fixed".
    // 2. Fixed size must be of 12 bytes as it hold 3 integers.
    // 3. Logical type name should be "duration". The name might be stored in different places based on Avro version
    //    being used here.
    return schema.getType().equals(Schema.Type.FIXED)
        && schema.getFixedSize() == Integer.BYTES * NUM_ELEMENTS_FOR_DURATION_TYPE
        && (durationType != null && durationType.getName().equals(durationTypeName)
        || durationTypeProp != null && durationTypeProp.equals(durationTypeName));
  }
}
