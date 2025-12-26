/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * “License”); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.collection.Pair;

import java.math.BigDecimal;

/**
 * Utility class for ordering value canonicalization. Handle compatibility issues between different data types in precombine values.
 */
public class OrderingValueUtils {
  /**
   * Currently, there are some discrepancies between the types of the ordering value for delete record and normal record during merging.
   * E.g., if the precombine field is of STRING type, the ordering value of delete record is a String, while that of normal record is an
   * Avro UTF8. We should canonicalize the ordering values before merging. Specifically, STRING and DECIMAL type should be handled here.
   * <p>
   * Processing logic:
   * 1. Unifies string types: Converts between Utf8 (Avro) ↔ String (Java)
   * 2. Unifies numeric types: Converts between GenericData.Fixed (Avro) ↔ BigDecimal (Java)
   *
   * @param oldOrder Existing ordering value in the table (typically Avro types)
   * @param incomingOrder New incoming ordering value (could be Java types or Avro types)
   * @return Pair<Comparable, Comparable> Canonicalized ordering value pair with consistent types for comparison
   */
  public static Pair<Comparable, Comparable> canonicalizeOrderingValue(Comparable oldOrder, Comparable incomingOrder) {
    // Case 1: Old value is Avro Utf8 type, new value is Java String type
    // Convert Utf8 to String to unify as Java String type for comparison
    if (oldOrder instanceof Utf8 && incomingOrder instanceof String) {
      oldOrder = oldOrder.toString();
    } else if (incomingOrder instanceof Utf8 && oldOrder instanceof String) {
      // Case 2: New value is Avro Utf8 type, old value is Java String type
      // Convert Utf8 to String to unify as Java String type for comparison
      incomingOrder = incomingOrder.toString();
    }

    // Case 3: Old value is Avro Fixed type, new value is Java BigDecimal type
    // Convert Fixed type to BigDecimal to unify as Java BigDecimal type for comparison
    // Fixed type is typically used for storing decimal values (e.g., DECIMAL)
    if (oldOrder instanceof GenericData.Fixed && incomingOrder instanceof BigDecimal) {
      oldOrder = (BigDecimal) HoodieAvroUtils.convertValueForSpecificDataTypes(((GenericData.Fixed) oldOrder).getSchema(), oldOrder, false);
    } else if (incomingOrder instanceof GenericData.Fixed && oldOrder instanceof BigDecimal) {
      // Case 4: New value is Avro Fixed type, old value is Java BigDecimal type
      // Convert Fixed type to BigDecimal to unify as Java BigDecimal type for comparison
      incomingOrder = (BigDecimal) HoodieAvroUtils.convertValueForSpecificDataTypes(((GenericData.Fixed) incomingOrder).getSchema(), incomingOrder, false);
    }

    // Return canonicalized ordering value pair ensuring both values have consistent types
    // ImmutablePair is an immutable Pair implementation from Apache Commons Lang
    return Pair.of(oldOrder, incomingOrder);
  }
}
