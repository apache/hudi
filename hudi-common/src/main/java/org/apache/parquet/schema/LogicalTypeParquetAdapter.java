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
    LogicalTypeAnnotation annotation = primitiveType.getLogicalTypeAnnotation();
    if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
      return ValueType.STRING;
    } else if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      return ValueType.DECIMAL;
    } else if (annotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
      return ValueType.DATE;
    } else if (annotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
      // TODO: decide if we need to support adjusted to UTC
      LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeAnnotation = (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) annotation;
      if (timeAnnotation.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS) {
        return ValueType.TIME_MILLIS;
      } else if (timeAnnotation.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS) {
        return ValueType.TIME_MICROS;
      } else {
        throw new IllegalArgumentException("Unsupported time unit: " + timeAnnotation.getUnit());
      }
    } else if (annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampAnnotation = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) annotation;
      if (timestampAnnotation.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS) {
        return timestampAnnotation.isAdjustedToUTC() ? ValueType.TIMESTAMP_MILLIS : ValueType.LOCAL_TIMESTAMP_MILLIS;
      } else if (timestampAnnotation.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS) {
        return timestampAnnotation.isAdjustedToUTC() ? ValueType.TIMESTAMP_MICROS : ValueType.LOCAL_TIMESTAMP_MICROS;
      } else if (timestampAnnotation.getUnit() == LogicalTypeAnnotation.TimeUnit.NANOS) {
        return timestampAnnotation.isAdjustedToUTC() ? ValueType.TIMESTAMP_NANOS : ValueType.LOCAL_TIMESTAMP_NANOS;
      } else {
        throw new IllegalArgumentException("Unsupported timestamp unit: " + timestampAnnotation.getUnit());
      }
    } else if (annotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
      return ValueType.UUID;
    }

    throw new IllegalArgumentException("Unsupported logical type annotation: " + annotation);
  }

  private static void validatePrimitiveType(PrimitiveType primitiveType) {
    if (primitiveType.getLogicalTypeAnnotation() == null) {
      throw new IllegalArgumentException("Unsupported primitive type: " + primitiveType.getPrimitiveTypeName());
    }

    if (!(primitiveType.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)) {
      throw new IllegalArgumentException("Unsupported logical type annotation: " + primitiveType.getLogicalTypeAnnotation());
    }
  }

  @Override
  public int getPrecision(PrimitiveType primitiveType) {
    validatePrimitiveType(primitiveType);
    return ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) primitiveType.getLogicalTypeAnnotation()).getPrecision();
  }

  @Override
  public int getScale(PrimitiveType primitiveType) {
    validatePrimitiveType(primitiveType);
    return ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) primitiveType.getLogicalTypeAnnotation()).getScale();
  }
}
