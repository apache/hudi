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

package org.apache.hudi.stats;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.metadata.HoodieIndexVersion;

import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.VarcharType;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;

public class SparkValueMetadataUtils {

  public static ValueMetadata getValueMetadata(DataType dataType, HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      return ValueMetadata.V1EmptyMetadata.get();
    }
    if (dataType == null) {
      return ValueMetadata.NULL_METADATA;
    }

    ValueType valueType = fromDataType(dataType);
    if (valueType == ValueType.DECIMAL) {
      return ValueMetadata.DecimalMetadata.create(((DecimalType) dataType).precision(), ((DecimalType) dataType).scale());
    } else {
      return new ValueMetadata(valueType);
    }
  }

  private static ValueType fromDataType(DataType dataType) {
    if (dataType instanceof NullType) {
      return ValueType.NULL;
    } else if (dataType instanceof BooleanType) {
      return ValueType.BOOLEAN;
    } else if (dataType instanceof IntegerType || dataType instanceof ShortType || dataType instanceof ByteType) {
      return ValueType.INT;
    } else if (dataType instanceof LongType) {
      return ValueType.LONG;
    } else if (dataType instanceof FloatType) {
      return ValueType.FLOAT;
    } else if (dataType instanceof DoubleType) {
      return ValueType.DOUBLE;
    } else if (dataType instanceof StringType || dataType instanceof CharType || dataType instanceof VarcharType) {
      return ValueType.STRING;
    }  else if (dataType instanceof TimestampType) {
      return ValueType.TIMESTAMP_MICROS;
    }  else if (dataType instanceof DecimalType) {
      return ValueType.DECIMAL;
    } else if (dataType instanceof DateType) {
      return ValueType.DATE;
    } else if (dataType instanceof BinaryType) {
      return ValueType.BYTES;
    } else if (SparkAdapterSupport$.MODULE$.sparkAdapter().isTimestampNTZType(dataType)) {
      return ValueType.LOCAL_TIMESTAMP_MICROS;
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  public static Comparable convertSparkToJava(ValueMetadata valueMetadata, Object value) {
    if (value == null) {
      return null;
    }

    switch (valueMetadata.getValueType()) {
      case V1:
        return (Comparable) value;
      case NULL:
        return null;
      case BOOLEAN:
        return (Boolean) value;
      case INT:
        return (Integer) value;
      case LONG:
        return (Long) value;
      case FLOAT:
        return (Float) value;
      case DOUBLE:
        return (Double) value;
      case STRING:
        return (String) value;
      case BYTES:
        return ValueType.castToBytes(value);
      case DECIMAL:
        return ((Decimal) value).toJavaBigDecimal();
      case DATE:
        return ValueType.castToDate(value, valueMetadata);
      case TIMESTAMP_MICROS:
        return ValueType.castToTimestampMicros(value, valueMetadata);
      case LOCAL_TIMESTAMP_MICROS:
        return ValueType.castToLocalTimestampMicros(value, valueMetadata);
      case FIXED:
      case UUID:
      case TIME_MILLIS:
      case TIME_MICROS:
      case TIMESTAMP_MILLIS:
      case TIMESTAMP_NANOS:
      case LOCAL_TIMESTAMP_MILLIS:
      case LOCAL_TIMESTAMP_NANOS:
      default:
        throw new IllegalStateException("Spark value metadata for expression index should never be " + valueMetadata.getValueType().name());
    }
  }

  public static Object convertJavaTypeToSparkType(Comparable<?> javaVal, boolean useJava8api) {
    if (!useJava8api) {
      if (javaVal instanceof Instant) {
        return Timestamp.from((Instant) javaVal);
      } else if (javaVal instanceof LocalDate) {
        return Date.valueOf((LocalDate) javaVal);
      }
    }
    return javaVal;
  }
}
