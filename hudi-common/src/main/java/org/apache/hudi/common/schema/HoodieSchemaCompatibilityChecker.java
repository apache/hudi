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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Core compatibility checker for HoodieSchema schemas.
 *
 * <p>This class implements recursive schema compatibility checking with memoization
 * to handle circular references. It determines if a reader schema can read data
 * written with a writer schema.</p>
 *
 * <p>Compatibility rules:
 * <ul>
 *   <li>Same types are compatible (with recursive checks for complex types)</li>
 *   <li>Type promotions are allowed (INT → LONG, etc.)</li>
 *   <li>Reader unions must be compatible with all writer union branches</li>
 *   <li>Writer unions require reader to be compatible with at least one branch</li>
 *   <li>Record fields must match (reader fields need writer fields or defaults)</li>
 *   <li>Enum reader must contain all writer symbols</li>
 *   <li>Fixed types must have same size</li>
 *   <li>Decimal types follow precision/scale widening rules</li>
 * </ul>
 * </p>
 *
 * <p>This class is package-private and used internally by HoodieSchemaCompatibility.</p>
 */
class HoodieSchemaCompatibilityChecker {

  // Memoization map to track compatibility results and detect recursion
  private final Map<ReaderWriterPair, Boolean> memoizationMap = new HashMap<>();

  // Sentinel value to detect recursion in progress
  private static final Boolean RECURSION_IN_PROGRESS = null;

  /**
   * Checks if the reader schema is compatible with the writer schema.
   *
   * @param readerSchema the schema used to read the data
   * @param writerSchema the schema used to write the data
   * @param checkNaming  whether to check schema names (for enums, fixed, records)
   * @return true if the reader can successfully read data written with the writer schema
   */
  boolean isCompatible(HoodieSchema readerSchema, HoodieSchema writerSchema, boolean checkNaming) {
    return checkCompatibility(readerSchema, writerSchema, checkNaming);
  }

  /**
   * Recursive compatibility check with memoization.
   */
  private boolean checkCompatibility(HoodieSchema reader, HoodieSchema writer, boolean checkNaming) {
    ReaderWriterPair pair = new ReaderWriterPair(reader, writer);

    // Check memoization map
    Boolean memoized = memoizationMap.get(pair);
    if (memoized == RECURSION_IN_PROGRESS) {
      // Break recursion - assume compatible unless proven otherwise
      return true;
    } else if (memoized != null) {
      return memoized;
    }

    // Mark as in progress to detect recursion
    memoizationMap.put(pair, RECURSION_IN_PROGRESS);

    // Calculate compatibility
    boolean compatible = calculateCompatibility(reader, writer, checkNaming);

    // Store result
    memoizationMap.put(pair, compatible);

    return compatible;
  }

  /**
   * Calculates compatibility between reader and writer schemas.
   */
  private boolean calculateCompatibility(HoodieSchema reader, HoodieSchema writer, boolean checkNaming) {
    // Same type compatibility
    if (reader.getType() == writer.getType()) {
      return checkSameTypeCompatibility(reader, writer, checkNaming);
    }

    // Different types - check if writer is a union
    if (writer.getType() == HoodieSchemaType.UNION) {
      // Reader must be compatible with ALL branches of writer union
      for (HoodieSchema writerBranch : writer.getTypes()) {
        if (!checkCompatibility(reader, writerBranch, checkNaming)) {
          return false;
        }
      }
      return true;
    }

    // Different types - check if reader is a union
    if (reader.getType() == HoodieSchemaType.UNION) {
      // At least ONE branch of reader union must be compatible with writer
      for (HoodieSchema readerBranch : reader.getTypes()) {
        if (checkCompatibility(readerBranch, writer, checkNaming)) {
          return true;
        }
      }
      return false;
    }

    // Check type promotions for different types
    return HoodieSchemaTypePromotion.canPromote(reader.getType(), writer.getType());
  }

  /**
   * Checks compatibility when reader and writer have the same type.
   */
  private boolean checkSameTypeCompatibility(HoodieSchema reader, HoodieSchema writer, boolean checkNaming) {
    switch (reader.getType()) {
      case NULL:
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BYTES:
      case STRING:
      case DATE:
      case UUID:
        // Primitive types are compatible
        return true;

      case ARRAY:
        // Array element types must be compatible
        return checkCompatibility(reader.getElementType(), writer.getElementType(), checkNaming);

      case MAP:
        // Map value types must be compatible
        return checkCompatibility(reader.getValueType(), writer.getValueType(), checkNaming);

      case FIXED:
        // Check name if required
        if (checkNaming && !reader.getName().equals(writer.getName())) {
          return false;
        }
        // Fixed size must match, and check decimal widening
        return checkFixedCompatibility(reader, writer);

      case ENUM:
        // Check name if required
        if (checkNaming && !reader.getName().equals(writer.getName())) {
          return false;
        }
        // Reader must contain all writer enum symbols
        return checkEnumCompatibility(reader, writer);

      case RECORD:
        // Check name if required
        if (checkNaming && !reader.getName().equals(writer.getName())) {
          return false;
        }
        // Check all record fields
        return checkRecordCompatibility(reader, writer, checkNaming);

      case UNION:
        // Check that each writer branch can be decoded by reader
        for (HoodieSchema writerBranch : writer.getTypes()) {
          if (!checkCompatibility(reader, writerBranch, checkNaming)) {
            return false;
          }
        }
        return true;

      case DECIMAL:
        // Decimal types must have compatible precision/scale
        return HoodieSchemaTypePromotion.isDecimalWidening(reader, writer);

      case TIME:
        return checkTimeCompatibility(reader, writer);

      case TIMESTAMP:
        return checkTimestampCompatibility(reader, writer);

      default:
        throw new IllegalArgumentException("Unknown schema type: " + reader.getType());
    }
  }

  /**
   * Checks compatibility for FIXED types (including decimals).
   */
  private boolean checkFixedCompatibility(HoodieSchema reader, HoodieSchema writer) {
    // Fixed size must match
    if (reader.getFixedSize() != writer.getFixedSize()) {
      return false;
    }

    // If both are decimals, check decimal widening
    if (reader.getType() == HoodieSchemaType.DECIMAL && writer.getType() == HoodieSchemaType.DECIMAL) {
      return HoodieSchemaTypePromotion.isDecimalWidening(reader, writer);
    }

    return true;
  }

  /**
   * Checks compatibility for ENUM types.
   */
  private boolean checkEnumCompatibility(HoodieSchema reader, HoodieSchema writer) {
    List<String> readerSymbols = reader.getEnumSymbols();
    List<String> writerSymbols = writer.getEnumSymbols();

    // Reader must contain all writer symbols
    Set<String> readerSymbolSet = new HashSet<>(readerSymbols);
    for (String writerSymbol : writerSymbols) {
      if (!readerSymbolSet.contains(writerSymbol)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Checks compatibility for RECORD types.
   */
  private boolean checkRecordCompatibility(HoodieSchema reader, HoodieSchema writer, boolean checkNaming) {
    // Check that each field in the reader record can be populated from the writer record
    for (HoodieSchemaField readerField : reader.getFields()) {
      // Find corresponding writer field (handles aliases)
      HoodieSchemaField writerField = HoodieSchemaCompatibility.lookupWriterField(writer, readerField);

      if (writerField == null) {
        // Reader field does not exist in writer - must have default value
        if (!readerField.hasDefaultValue()) {
          return false;
        }
      } else {
        // Check field schema compatibility recursively
        if (!checkCompatibility(readerField.schema(), writerField.schema(), checkNaming)) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Checks compatibility for TIME types.
   */
  private boolean checkTimeCompatibility(HoodieSchema reader, HoodieSchema writer) {
    if (reader.getType() != HoodieSchemaType.TIME || writer.getType() != HoodieSchemaType.TIME) {
      return false;
    }

    HoodieSchema.Time readerTime = (HoodieSchema.Time) reader;
    HoodieSchema.Time writerTime = (HoodieSchema.Time) writer;

    // Precision must match
    return readerTime.getPrecision() == writerTime.getPrecision();
  }

  /**
   * Checks compatibility for TIMESTAMP types.
   */
  private boolean checkTimestampCompatibility(HoodieSchema reader, HoodieSchema writer) {
    if (reader.getType() != HoodieSchemaType.TIMESTAMP || writer.getType() != HoodieSchemaType.TIMESTAMP) {
      return false;
    }

    HoodieSchema.Timestamp readerTs = (HoodieSchema.Timestamp) reader;
    HoodieSchema.Timestamp writerTs = (HoodieSchema.Timestamp) writer;

    // Precision and UTC adjustment must match
    return readerTs.getPrecision() == writerTs.getPrecision()
        && readerTs.isUtcAdjusted() == writerTs.isUtcAdjusted();
  }

  /**
   * Pair of reader and writer schemas for memoization.
   */
  private static class ReaderWriterPair {
    private final HoodieSchema reader;
    private final HoodieSchema writer;
    private final int hashCode;

    ReaderWriterPair(HoodieSchema reader, HoodieSchema writer) {
      this.reader = reader;
      this.writer = writer;
      // Pre-compute hash code for performance
      this.hashCode = Objects.hash(System.identityHashCode(reader), System.identityHashCode(writer));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReaderWriterPair that = (ReaderWriterPair) o;
      // Use identity comparison for schemas to handle recursion correctly
      return reader == that.reader && writer == that.writer;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
