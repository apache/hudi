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
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;

public class LocalTimestampMilliLogicalTypeProcessor extends TimeLogicalTypeProcessor {
  public LocalTimestampMilliLogicalTypeProcessor() {
    super(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MILLIS);
  }

  @Override
  public Pair<Boolean, Object> convert(
      Object value, String name, Schema schema) {
    return convertCommon(
        new Parser.LongParser() {
          @Override
          public Pair<Boolean, Object> handleStringValue(String value) {
            if (!isWellFormedDateTime(value)) {
              return Pair.of(false, null);
            }
            Pair<Boolean, LocalDateTime> result = convertToLocalDateTime(value);
            if (!result.getLeft()) {
              return Pair.of(false, null);
            }
            LocalDateTime time = result.getRight();

            // Calculate the difference in milliseconds
            long diffInMillis = LOCAL_UNIX_EPOCH.until(time, ChronoField.MILLI_OF_SECOND.getBaseUnit());
            return Pair.of(true, diffInMillis);
          }
        },
        value, schema);
  }
}
