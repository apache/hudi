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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Uses LogicalTypeAnnotation to extract value type, precision, and scale
 */
public class LogicalTypeParquetAdapter implements ParquetAdapter {

  private static Object getLogicalTypeAnnotation(PrimitiveType primitiveType) {
    try {
      Method m = primitiveType.getClass().getMethod("getLogicalTypeAnnotation");
      return m.invoke(primitiveType);
    } catch (NoSuchMethodException e) {
      // Parquet < 1.11 does not have this API (we should not be instantiated in that case)
      return null;
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Unable to access Parquet logical type annotation", e);
    }
  }

  private static int getIntProperty(Object obj, String methodName) {
    try {
      Method m = obj.getClass().getMethod(methodName);
      Object v = m.invoke(obj);
      if (!(v instanceof Number)) {
        throw new IllegalStateException("Expected numeric return from " + methodName + " but got " + v);
      }
      return ((Number) v).intValue();
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Unable to invoke " + methodName + " on " + obj.getClass().getName(), e);
    }
  }

  private static boolean getBooleanProperty(Object obj, String methodName) {
    try {
      Method m = obj.getClass().getMethod(methodName);
      Object v = m.invoke(obj);
      if (!(v instanceof Boolean)) {
        throw new IllegalStateException("Expected boolean return from " + methodName + " but got " + v);
      }
      return (Boolean) v;
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Unable to invoke " + methodName + " on " + obj.getClass().getName(), e);
    }
  }

  private static String getEnumName(Object obj, String methodName) {
    try {
      Method m = obj.getClass().getMethod(methodName);
      Object v = m.invoke(obj);
      return v == null ? null : v.toString();
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Unable to invoke " + methodName + " on " + obj.getClass().getName(), e);
    }
  }

  private static boolean isAnnotationType(Object annotation, String simpleName) {
    return annotation != null && annotation.getClass().getName().endsWith("$" + simpleName);
  }

  @Override
  public boolean hasAnnotation(PrimitiveType primitiveType) {
    return getLogicalTypeAnnotation(primitiveType) != null;
  }

  @Override
  public ValueType getValueTypeFromAnnotation(PrimitiveType primitiveType) {
    Object annotation = getLogicalTypeAnnotation(primitiveType);
    if (isAnnotationType(annotation, "StringLogicalTypeAnnotation")) {
      return ValueType.STRING;
    } else if (isAnnotationType(annotation, "DecimalLogicalTypeAnnotation")) {
      return ValueType.DECIMAL;
    } else if (isAnnotationType(annotation, "DateLogicalTypeAnnotation")) {
      return ValueType.DATE;
    } else if (isAnnotationType(annotation, "TimeLogicalTypeAnnotation")) {
      // TODO: decide if we need to support adjusted to UTC
      String unit = getEnumName(annotation, "getUnit");
      if ("MILLIS".equals(unit)) {
        return ValueType.TIME_MILLIS;
      } else if ("MICROS".equals(unit)) {
        return ValueType.TIME_MICROS;
      } else {
        throw new IllegalArgumentException("Unsupported time unit: " + unit);
      }
    } else if (isAnnotationType(annotation, "TimestampLogicalTypeAnnotation")) {
      String unit = getEnumName(annotation, "getUnit");
      boolean adjustedToUtc = getBooleanProperty(annotation, "isAdjustedToUTC");
      if ("MILLIS".equals(unit)) {
        return adjustedToUtc ? ValueType.TIMESTAMP_MILLIS : ValueType.LOCAL_TIMESTAMP_MILLIS;
      } else if ("MICROS".equals(unit)) {
        return adjustedToUtc ? ValueType.TIMESTAMP_MICROS : ValueType.LOCAL_TIMESTAMP_MICROS;
      } else if ("NANOS".equals(unit)) {
        return adjustedToUtc ? ValueType.TIMESTAMP_NANOS : ValueType.LOCAL_TIMESTAMP_NANOS;
      } else {
        throw new IllegalArgumentException("Unsupported timestamp unit: " + unit);
      }
    } else if (isAnnotationType(annotation, "UUIDLogicalTypeAnnotation")) {
      return ValueType.UUID;
    }

    throw new IllegalArgumentException("Unsupported logical type annotation: " + annotation);
  }

  private static void validatePrimitiveType(PrimitiveType primitiveType) {
    Object annotation = getLogicalTypeAnnotation(primitiveType);
    if (annotation == null) {
      throw new IllegalArgumentException("Unsupported primitive type: " + primitiveType.getPrimitiveTypeName());
    }

    if (!isAnnotationType(annotation, "DecimalLogicalTypeAnnotation")) {
      throw new IllegalArgumentException("Unsupported logical type annotation: " + annotation);
    }
  }

  @Override
  public int getPrecision(PrimitiveType primitiveType) {
    validatePrimitiveType(primitiveType);
    return getIntProperty(getLogicalTypeAnnotation(primitiveType), "getPrecision");
  }

  @Override
  public int getScale(PrimitiveType primitiveType) {
    validatePrimitiveType(primitiveType);
    return getIntProperty(getLogicalTypeAnnotation(primitiveType), "getScale");
  }
}
