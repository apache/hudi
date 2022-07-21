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

package org.apache.hudi.keygen;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.TimestampType;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;

public class RowKeyGenUtils {

  /**
   * Converts provided (raw) value extracted from the {@link InternalRow} object into a deserialized,
   * JVM native format (for ex, converting {@code Long} into {@link Instant},
   * {@code Integer} to {@link LocalDate}, etc)
   *
   * This method allows to avoid costly full-row deserialization sequence. Note, that this method
   * should be maintained in sync w/
   *
   * <ol>
   *   <li>{@code RowEncoder#deserializerFor}, as well as</li>
   *   <li>{@code HoodieAvroUtils#convertValueForAvroLogicalTypes}</li>
   * </ol>
   *
   * @param dataType target data-type of the given value
   * @param value target value to be converted
   */
  public static Object convertToLogicalDataType(DataType dataType, Object value) {
    if (dataType instanceof TimestampType) {
      // Provided value have to be [[Long]] in this case, representing micros since epoch
      return new Timestamp((Long) value / 1000);
    } else if (dataType instanceof DateType) {
      // Provided value have to be [[Int]] in this case
      return LocalDate.ofEpochDay((Integer) value);
    }

    return value;
  }
}
