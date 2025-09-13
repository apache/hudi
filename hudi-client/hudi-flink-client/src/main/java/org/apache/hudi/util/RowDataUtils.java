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

package org.apache.hudi.util;

import org.apache.hudi.common.util.collection.ArrayComparable;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.function.Function;

/**
 * Utils for get/set operations on {@link RowData}.
 */
public class RowDataUtils {
  /**
   * An implementation of {@code FieldGetter} which always return NULL.
   */
  public static final RowData.FieldGetter NULL_GETTER = new RowData.FieldGetter() {
    private static final long serialVersionUID = 1L;

    @Override
    public Object getFieldOrNull(RowData rowData) {
      return null;
    }
  };

  /**
   * Resolve the native Java object from given row data field value.
   *
   * <p>IMPORTANT: the logic references the row-data to avro conversion in {@code RowDataToAvroConverters.createConverter}
   * and {@code HoodieAvroUtils.convertValueForAvroLogicalTypes}.
   *
   * @param logicalType The logical type
   * @param utcTimezone whether to use UTC timezone for timestamp data type
   */
  public static Function<Object, Object> javaValFunc(LogicalType logicalType, boolean utcTimezone) {
    switch (logicalType.getTypeRoot()) {
      case NULL:
        return fieldVal -> null;
      case TINYINT:
        return fieldVal -> ((Byte) fieldVal).intValue();
      case SMALLINT:
        return fieldVal -> ((Short) fieldVal).intValue();
      case DATE:
        return fieldVal -> LocalDate.ofEpochDay((Integer) fieldVal);
      case CHAR:
      case VARCHAR:
        return Object::toString;
      case BINARY:
      case VARBINARY:
        return fieldVal -> ByteBuffer.wrap((byte[]) fieldVal);
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        int precision1 = precision(logicalType);
        if (precision1 <= 3) {
          return fieldVal -> ((TimestampData) fieldVal).toInstant().toEpochMilli();
        } else if (precision1 <= 6) {
          return fieldVal -> {
            Instant instant = ((TimestampData) fieldVal).toInstant();
            return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1000_000), instant.getNano() / 1000);
          };
        } else {
          throw new UnsupportedOperationException("Unsupported timestamp precision: " + precision1);
        }
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        int precision2 = precision(logicalType);
        if (precision2 <= 3) {
          return fieldVal -> utcTimezone ? ((TimestampData) fieldVal).getMillisecond() : ((TimestampData) fieldVal).toTimestamp().getTime();
        } else if (precision2 <= 6) {
          return fieldVal -> {
            Instant instant = utcTimezone ? ((TimestampData) fieldVal).toInstant() : ((TimestampData) fieldVal).toTimestamp().toInstant();
            return  Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1000_000), instant.getNano() / 1000);
          };
        } else {
          throw new UnsupportedOperationException("Unsupported timestamp precision: " + precision2);
        }
      case DECIMAL:
        return fieldVal -> ((DecimalData) fieldVal).toBigDecimal();
      case ARRAY:
        LogicalType elementType = ((ArrayType) logicalType).getElementType();
        ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        Function<Object, Object> elementFunc = javaValFunc(elementType, utcTimezone);
        return fieldVal -> {
          ArrayData arrayData = (ArrayData) fieldVal;
          Comparable[] values = new Comparable[arrayData.size()];
          for (int i = 0; i < arrayData.size(); i++) {
            values[i] = (Comparable<?>) elementFunc.apply(elementGetter.getElementOrNull(arrayData, i));
          }
          return new ArrayComparable(values);
        };
      default:
        return fieldVal -> fieldVal;
    }
  }

  /**
   * Convert the native Java object to the corresponding value of Flink type.
   *
   * @param value Java object
   *
   * @return Value of Flink type
   */
  public static Object convertValueToFlinkType(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return StringData.fromString((String) value);
    }
    if (value instanceof BigDecimal) {
      BigDecimal decimalVal = (BigDecimal) value;
      return DecimalData.fromBigDecimal(decimalVal, decimalVal.precision(), decimalVal.scale());
    }
    if (value instanceof Timestamp) {
      return TimestampData.fromTimestamp((Timestamp) value);
    }
    if (value instanceof LocalDate) {
      return (int)(((LocalDate) value).toEpochDay());
    }
    if (value instanceof ByteBuffer) {
      return ((ByteBuffer) value).array();
    }
    return value;
  }

  /**
   * Returns the precision of the given TIMESTAMP type.
   */
  public static int precision(LogicalType logicalType) {
    if (logicalType instanceof TimestampType) {
      return ((TimestampType) logicalType).getPrecision();
    } else if (logicalType instanceof LocalZonedTimestampType) {
      return ((LocalZonedTimestampType) logicalType).getPrecision();
    } else {
      throw new AssertionError("Unexpected type: " + logicalType);
    }
  }
}
