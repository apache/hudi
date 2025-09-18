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
 * Uses OriginalType to extract value type, precision, and scale
 */
public class OriginalTypeParquetAdapter implements ParquetAdapter {
  @Override
  public boolean hasAnnotation(PrimitiveType primitiveType) {
    return primitiveType.getOriginalType() != null;
  }

  @Override
  public ValueType getValueTypeFromAnnotation(PrimitiveType primitiveType) {
    switch (primitiveType.getOriginalType()) {
      case UTF8:
        return ValueType.STRING;
      case DECIMAL:
        return ValueType.DECIMAL;
      case DATE:
        return ValueType.DATE;
      case TIME_MILLIS:
        return ValueType.TIME_MILLIS;
      case TIME_MICROS:
        return ValueType.TIME_MICROS;
      case TIMESTAMP_MILLIS:
        if (primitiveType.toString().contains("(TIMESTAMP(MILLIS,false))")) {
          return ValueType.LOCAL_TIMESTAMP_MILLIS;
        }
        return ValueType.TIMESTAMP_MILLIS;
      case TIMESTAMP_MICROS:
        if (primitiveType.toString().contains("(TIMESTAMP(MICROS,false))")) {
          return ValueType.LOCAL_TIMESTAMP_MICROS;
        }
        return ValueType.TIMESTAMP_MICROS;
      default:
        throw new IllegalArgumentException("Unsupported original type: " + primitiveType.getOriginalType());
    }
  }

  private static void validatePrimitiveType(PrimitiveType primitiveType) {
    if (primitiveType.getOriginalType() == null) {
      throw new IllegalArgumentException("Unsupported primitive type: " + primitiveType.getPrimitiveTypeName());
    }

    if (primitiveType.getOriginalType() != OriginalType.DECIMAL) {
      throw new IllegalArgumentException("Unsupported original type: " + primitiveType.getOriginalType());
    }
  }

  @Override
  public int getPrecision(PrimitiveType primitiveType) {
    validatePrimitiveType(primitiveType);
    return primitiveType.getDecimalMetadata().getPrecision();
  }

  @Override
  public int getScale(PrimitiveType primitiveType) {
    validatePrimitiveType(primitiveType);
    return primitiveType.getDecimalMetadata().getScale();
  }
}
