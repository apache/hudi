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

package org.apache.hudi.io.storage;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Utility class for Lance file format tests.
 * Provides helper methods for creating test data rows.
 */
public class LanceTestUtils {

  private LanceTestUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Create InternalRow with placeholder Hudi metadata fields + user data.
   * The 5 Hudi metadata fields are populated with null as placeholders.
   *
   * @param userValues User data values to append after the 5 metadata fields
   * @return InternalRow with 5 metadata fields + user values
   */
  public static InternalRow createRowWithMetaFields(Object... userValues) {
    Object[] allValues = new Object[5 + userValues.length];

    // Meta fields - use null as placeholders (will be populated by writer)
    allValues[0] = null; // commit_time
    allValues[1] = null; // commit_seqno
    allValues[2] = null; // record_key
    allValues[3] = null; // partition_path
    allValues[4] = null; // file_name

    // Copy user values starting at index 5
    for (int i = 0; i < userValues.length; i++) {
      allValues[5 + i] = processValue(userValues[i]);
    }

    return new GenericInternalRow(allValues);
  }

  /**
   * Create InternalRow from variable number of values.
   * Automatically converts String values to UTF8String.
   *
   * @param values Values to include in the row
   * @return InternalRow containing the processed values
   */
  public static InternalRow createRow(Object... values) {
    Object[] processedValues = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      processedValues[i] = processValue(values[i]);
    }
    return new GenericInternalRow(processedValues);
  }

  /**
   * Process a value for use in InternalRow.
   * Recursively converts String to UTF8String in complex types (arrays, maps, structs).
   *
   * @param value Value to process
   * @return Processed value suitable for InternalRow
   */
  protected static Object processValue(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof String) {
      return UTF8String.fromString((String) value);
    } else if (value instanceof GenericArrayData) {
      // Process array elements recursively
      GenericArrayData array = (GenericArrayData) value;
      Object[] elements = array.array();
      Object[] processed = new Object[elements.length];
      for (int i = 0; i < elements.length; i++) {
        processed[i] = processValue(elements[i]);
      }
      return new GenericArrayData(processed);
    } else if (value instanceof Object[] && !(value instanceof byte[])) {
      // Process Object[] arrays (used for creating GenericArrayData)
      Object[] array = (Object[]) value;
      Object[] processed = new Object[array.length];
      for (int i = 0; i < array.length; i++) {
        processed[i] = processValue(array[i]);
      }
      return new GenericArrayData(processed);
    } else if (value instanceof ArrayBasedMapData) {
      // Process map keys and values recursively
      ArrayBasedMapData map = (ArrayBasedMapData) value;
      return new ArrayBasedMapData(
        (ArrayData) processValue(map.keyArray()),
        (ArrayData) processValue(map.valueArray())
      );
    } else if (value instanceof GenericInternalRow) {
      // Process struct fields recursively
      GenericInternalRow row = (GenericInternalRow) value;
      Object[] values = row.values();
      Object[] processed = new Object[values.length];
      for (int i = 0; i < values.length; i++) {
        processed[i] = processValue(values[i]);
      }
      return new GenericInternalRow(processed);
    } else {
      // Pass through all other types unchanged (Decimal, Long, Integer, byte[], etc.)
      return value;
    }
  }
}
