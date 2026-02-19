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

package org.apache.hudi.common.schema;

import java.util.List;

/**
 * Defines equality comparison rules for HoodieSchema projection equivalence.
 *
 * <p>This class extends {@link HoodieSchemaComparatorForSchemaEvolution} and overrides
 * certain rules to check if two schemas are projection equivalent. Two schemas are
 * considered projection equivalent if projecting a record from one schema to the other
 * would be an identity operation (no actual transformation needed).</p>
 *
 * <h2>Key Differences from Schema Evolution Comparison</h2>
 * <ul>
 *   <li><b>Nullability:</b> Ignored - union with null is treated as optional</li>
 *   <li><b>Record names/namespaces:</b> Ignored - only field structure matters</li>
 *   <li><b>Field names:</b> Case-insensitive comparison</li>
 *   <li><b>Field order:</b> Ignored - order doesn't affect projection</li>
 *   <li><b>Default values:</b> Ignored - not relevant for projection</li>
 *   <li><b>Enum symbols:</b> Allows additional symbols in second schema</li>
 *   <li><b>Unions:</b> Not supported - throws exception</li>
 * </ul>
 *
 * <p>This class is package-private and used internally by HoodieSchemaCompatibility.</p>
 */
class HoodieSchemaComparatorForRecordProjection extends HoodieSchemaComparatorForSchemaEvolution {

  private static final HoodieSchemaComparatorForRecordProjection INSTANCE =
      new HoodieSchemaComparatorForRecordProjection();

  /**
   * Checks if two schemas are projection equivalent.
   *
   * <p>If schemas are projection equivalent, then a record with schema1 does not need
   * to be projected to schema2 because the projection will be the identity.</p>
   *
   * @param s1 first schema to compare
   * @param s2 second schema to compare
   * @return true if schemas are projection equivalent
   */
  static boolean areSchemasProjectionEquivalent(HoodieSchema s1, HoodieSchema s2) {
    return INSTANCE.schemaEqualsInternal(s1, s2);
  }

  @Override
  protected boolean schemaEqualsInternal(HoodieSchema s1, HoodieSchema s2) {
    if (s1 == s2) {
      return true;
    }
    if (s1 == null || s2 == null) {
      return false;
    }

    // Strip nullability before comparison
    HoodieSchema nonNullS1 = s1.getNonNullType();
    HoodieSchema nonNullS2 = s2.getNonNullType();

    return super.schemaEqualsInternal(nonNullS1, nonNullS2);
  }

  @Override
  protected boolean validateRecord(HoodieSchema s1, HoodieSchema s2) {
    // For projection equivalence, we don't care about record names or error flags
    return true;
  }

  @Override
  protected boolean validateField(HoodieSchemaField f1, HoodieSchemaField f2) {
    // For projection equivalence, field names are case-insensitive
    // and we ignore order and default values
    return f1.name().equalsIgnoreCase(f2.name());
  }

  @Override
  protected boolean enumSchemaEquals(HoodieSchema s1, HoodieSchema s2) {
    List<String> symbols1 = s1.getEnumSymbols();
    List<String> symbols2 = s2.getEnumSymbols();

    // For projection equivalence, s2 can have MORE symbols than s1
    // but s1 cannot have more symbols than s2
    if (symbols1.size() > symbols2.size()) {
      return false;
    }

    // Check that all symbols in s1 exist in s2 (case-insensitive)
    for (int i = 0; i < symbols1.size(); i++) {
      if (!symbols1.get(i).equalsIgnoreCase(symbols2.get(i))) {
        return false;
      }
    }

    return true;
  }

  @Override
  protected boolean unionSchemaEquals(HoodieSchema s1, HoodieSchema s2) {
    // Unions are not supported for projection equivalence
    // (they should have been stripped by getNonNullTypeFromUnion)
    throw new UnsupportedOperationException("Union types are not supported for projection equivalence");
  }
}
