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
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;

import java.math.BigDecimal;
/**
 * Utility class for ordering value canonicalization.
 * Handles compatibility issues between different data types in sorting values,
 * particularly for Avro and Java type conversions.
 */
public class OrderingValueUtils {
    /**
     * Canonicalizes ordering values to resolve compatibility issues between different data types.
     * Primarily handles conversions between Avro types and Java types to ensure proper comparison operations.
     *
     * @param oldOrder Existing ordering value in the table (typically Avro types)
     * @param incomingOrder New incoming ordering value (could be Java types or Avro types)
     * @return Pair<Comparable, Comparable> Canonicalized ordering value pair with consistent types for comparison
     *
     * Processing logic:
     * 1. Unifies string types: Converts between Utf8 (Avro) ↔ String (Java)
     * 2. Unifies numeric types: Converts between GenericData.Fixed (Avro) ↔ BigDecimal (Java)
     *
     * Use cases:
     * - When merging data in Hudi tables, comparing ordering fields between old and new records
     * - Ensuring semantically identical data with different types can be compared correctly
     */
    public static Pair<Comparable, Comparable> canonicalizeOrderingValue(Comparable oldOrder, Comparable incomingOrder) {
        // Case 1: Old value is Avro Utf8 type, new value is Java String type
        // Convert Utf8 to String to unify as Java String type for comparison
        if (oldOrder instanceof Utf8 && incomingOrder instanceof String) {
            oldOrder = oldOrder.toString();
        }

        // Case 2: New value is Avro Utf8 type, old value is Java String type
        // Convert Utf8 to String to unify as Java String type for comparison
        if (incomingOrder instanceof Utf8 && oldOrder instanceof String) {
            incomingOrder = incomingOrder.toString();
        }

        // Case 3: Old value is Avro Fixed type, new value is Java BigDecimal type
        // Convert Fixed type to BigDecimal to unify as Java BigDecimal type for comparison
        // Fixed type is typically used for storing decimal values (e.g., DECIMAL)
        if (oldOrder instanceof GenericData.Fixed && incomingOrder instanceof BigDecimal) {
            oldOrder = (BigDecimal) HoodieAvroUtils.convertValueForSpecificDataTypes(((GenericData.Fixed) oldOrder).getSchema(), oldOrder, false);
        }

        // Case 4: New value is Avro Fixed type, old value is Java BigDecimal type
        // Convert Fixed type to BigDecimal to unify as Java BigDecimal type for comparison
        if (incomingOrder instanceof GenericData.Fixed && oldOrder instanceof BigDecimal) {
            incomingOrder = (BigDecimal) HoodieAvroUtils.convertValueForSpecificDataTypes(((GenericData.Fixed) incomingOrder).getSchema(), incomingOrder, false);
        }

        // Return canonicalized ordering value pair ensuring both values have consistent types
        // ImmutablePair is an immutable Pair implementation from Apache Commons Lang
        return new ImmutablePair<>(oldOrder, incomingOrder);
    }
}
