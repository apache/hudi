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

package org.apache.parquet.schema;

import org.apache.hudi.ParquetAdapter;
import org.apache.hudi.stats.ValueType;

/**
 * Uses LogicalTypeAnnotation to extract value type, precision, and scale
 */
public class LogicalTypeParquetAdapter implements ParquetAdapter {
  @Override
  public boolean hasAnnotation(PrimitiveType primitiveType) {
    return primitiveType.getLogicalTypeAnnotation() != null;
  }

  @Override
  public ValueType getValueTypeFromAnnotation(PrimitiveType primitiveType) {
    switch (primitiveType.getLogicalTypeAnnotation().getType()) {
      case STRING:
        return ValueType.STRING;
      case DECIMAL:
        return ValueType.DECIMAL;
      case DATE:
        return ValueType.DATE;
      case TIME:
        // TODO: decide if we need to support adjusted to UTC
        boolean isAdjustedToUTCTime = ((LogicalTypeAnnotation.TimeLogicalTypeAnnotation) primitiveType.getLogicalTypeAnnotation()).isAdjustedToUTC();
        LogicalTypeAnnotation.TimeUnit unit = ((LogicalTypeAnnotation.TimeLogicalTypeAnnotation) primitiveType.getLogicalTypeAnnotation()).getUnit();
        if (unit == LogicalTypeAnnotation.TimeUnit.MILLIS) {
          return ValueType.TIME_MILLIS;
        } else if (unit == LogicalTypeAnnotation.TimeUnit.MICROS) {
          return ValueType.TIME_MICROS;
        } else {
          throw new IllegalArgumentException("Unsupported time unit: " + unit);
        }
      case TIMESTAMP:
        boolean isAdjustedToUTCTimestamp = ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) primitiveType.getLogicalTypeAnnotation()).isAdjustedToUTC();
        LogicalTypeAnnotation.TimeUnit timestampUnit = ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) primitiveType.getLogicalTypeAnnotation()).getUnit();
        if (timestampUnit == LogicalTypeAnnotation.TimeUnit.MILLIS) {
          return isAdjustedToUTCTimestamp ? ValueType.TIMESTAMP_MILLIS : ValueType.LOCAL_TIMESTAMP_MILLIS;
        } else if (timestampUnit == LogicalTypeAnnotation.TimeUnit.MICROS) {
          return isAdjustedToUTCTimestamp ? ValueType.TIMESTAMP_MICROS : ValueType.LOCAL_TIMESTAMP_MICROS;
        } else if (timestampUnit == LogicalTypeAnnotation.TimeUnit.NANOS) {
          return isAdjustedToUTCTimestamp ? ValueType.TIMESTAMP_NANOS : ValueType.LOCAL_TIMESTAMP_NANOS;
        } else {
          throw new IllegalArgumentException("Unsupported timestamp unit: " + timestampUnit);
        }
      case UUID:
        return ValueType.UUID;
      default:
        throw new IllegalArgumentException("Unsupported logical type: " + primitiveType.getLogicalTypeAnnotation().getType());
    }
  }

  @Override
  public int getPrecision(PrimitiveType primitiveType) {
    if (primitiveType.getLogicalTypeAnnotation() == null) {
      throw new IllegalArgumentException("Unsupported primitive type: " + primitiveType.getPrimitiveTypeName());
    }

    if (primitiveType.getLogicalTypeAnnotation().getType() != LogicalTypeAnnotation.LogicalTypeToken.DECIMAL) {
      throw new IllegalArgumentException("Unsupported logical type: " + primitiveType.getLogicalTypeAnnotation().getType());
    }

    return ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) primitiveType.getLogicalTypeAnnotation()).getPrecision();
  }

  @Override
  public int getScale(PrimitiveType primitiveType) {
    if (primitiveType.getLogicalTypeAnnotation() == null) {
      throw new IllegalArgumentException("Unsupported primitive type: " + primitiveType.getPrimitiveTypeName());
    }

    if (primitiveType.getLogicalTypeAnnotation().getType() != LogicalTypeAnnotation.LogicalTypeToken.DECIMAL) {
      throw new IllegalArgumentException("Unsupported logical type: " + primitiveType.getLogicalTypeAnnotation().getType());
    }

    return ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) primitiveType.getLogicalTypeAnnotation()).getScale();
  }
}
