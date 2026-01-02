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
import org.apache.hudi.common.util.collection.Pair;

import java.time.Instant;
import java.time.temporal.ChronoField;

/**
 * Processor for TimeMicro logical type.
 */
public class TimestampMilliLogicalTypeProcessor extends TimeLogicalTypeProcessor {
  public TimestampMilliLogicalTypeProcessor() {
    super(AvroLogicalTypeEnum.TIMESTAMP_MILLIS);
  }

  @Override
  public Pair<Boolean, Object> convert(
      Object value, String name, HoodieSchema schema) {
    return convertCommon(
        new Parser.LongParser() {
          @Override
          public Pair<Boolean, Object> handleStringValue(String value) {
            return convertDateTime(
                value,
                null,
                time -> Instant.EPOCH.until(time, ChronoField.MILLI_OF_SECOND.getBaseUnit()));  // Diff in millis
          }
        },
        value, schema);
  }
}
