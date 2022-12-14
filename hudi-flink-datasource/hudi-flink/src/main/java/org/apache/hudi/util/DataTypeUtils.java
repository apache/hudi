/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/**
 * Utilities for {@link org.apache.flink.table.types.DataType}.
 */
public class DataTypeUtils {

  private static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMATTER =
       new DateTimeFormatterBuilder()
      .appendPattern("yyyy-[MM][M]-[dd][d]")
      .optionalStart()
      .appendPattern(" [HH][H]:[mm][m]:[ss][s]")
      .appendFraction(NANO_OF_SECOND, 0, 9, true)
      .optionalEnd()
      .toFormatter();
  /**
   * Returns whether the given type is TIMESTAMP type.
   */
  public static boolean isTimestampType(DataType type) {
    return type.getLogicalType().getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
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

  /**
   * Returns whether the given type is DATE type.
   */
  public static boolean isDateType(DataType type) {
    return type.getLogicalType().getTypeRoot() == LogicalTypeRoot.DATE;
  }

  /**
   * Returns whether the given type is DATETIME type.
   */
  public static boolean isDatetimeType(DataType type) {
    return isTimestampType(type) || isDateType(type);
  }

  /**
   * Projects the row fields with given names.
   */
  public static RowType.RowField[] projectRowFields(RowType rowType, String[] names) {
    int[] fieldIndices = Arrays.stream(names).mapToInt(rowType::getFieldIndex).toArray();
    return Arrays.stream(fieldIndices).mapToObj(i -> rowType.getFields().get(i)).toArray(RowType.RowField[]::new);
  }

  /**
   * Returns whether the given logical type belongs to the family.
   */
  public static boolean isFamily(LogicalType logicalType, LogicalTypeFamily family) {
    return logicalType.getTypeRoot().getFamilies().contains(family);
  }

  /**
   * Resolves the partition path string into value obj with given data type.
   */
  public static Object resolvePartition(String partition, DataType type) {
    if (partition == null) {
      return null;
    }

    LogicalTypeRoot typeRoot = type.getLogicalType().getTypeRoot();
    switch (typeRoot) {
      case CHAR:
      case VARCHAR:
        return partition;
      case BOOLEAN:
        return Boolean.parseBoolean(partition);
      case TINYINT:
        return Integer.valueOf(partition).byteValue();
      case SMALLINT:
        return Short.valueOf(partition);
      case INTEGER:
        return Integer.valueOf(partition);
      case BIGINT:
        return Long.valueOf(partition);
      case FLOAT:
        return Float.valueOf(partition);
      case DOUBLE:
        return Double.valueOf(partition);
      case DATE:
        return LocalDate.parse(partition);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return LocalDateTime.parse(partition);
      case DECIMAL:
        return new BigDecimal(partition);
      default:
        throw new RuntimeException(
            String.format(
                "Can not convert %s to type %s for partition value", partition, type));
    }
  }

  /**
   * Ensures the give columns of the row data type are not nullable(for example, the primary keys).
   *
   * @param dataType  The row data type, datatype logicaltype must be rowtype
   * @param pkColumns The primary keys
   * @return a new row data type if any column nullability is tweaked or the original data type
   */
  public static DataType ensureColumnsAsNonNullable(DataType dataType, @Nullable List<String> pkColumns) {
    if (pkColumns == null || pkColumns.isEmpty()) {
      return dataType;
    }
    LogicalType dataTypeLogicalType = dataType.getLogicalType();
    if (!(dataTypeLogicalType instanceof RowType)) {
      throw new RuntimeException("The datatype to be converted must be row type, but this type is :" + dataTypeLogicalType.getClass());
    }
    RowType rowType = (RowType) dataTypeLogicalType;
    List<DataType> originalFieldTypes = dataType.getChildren();
    List<String> fieldNames = rowType.getFieldNames();
    List<DataType> fieldTypes = new ArrayList<>();
    boolean tweaked = false;
    for (int i = 0; i < fieldNames.size(); i++) {
      if (pkColumns.contains(fieldNames.get(i)) && rowType.getTypeAt(i).isNullable()) {
        fieldTypes.add(originalFieldTypes.get(i).notNull());
        tweaked = true;
      } else {
        fieldTypes.add(originalFieldTypes.get(i));
      }
    }
    if (!tweaked) {
      return dataType;
    }
    List<DataTypes.Field> fields = new ArrayList<>();
    for (int i = 0; i < fieldNames.size(); i++) {
      fields.add(DataTypes.FIELD(fieldNames.get(i), fieldTypes.get(i)));
    }
    return DataTypes.ROW(fields.stream().toArray(DataTypes.Field[]::new)).notNull();
  }

  public static Long getAsLong(Object value, LogicalType logicalType) {
    if (isTimeType(logicalType)) {
      return toMills(toLocalDateTime(value.toString()));
    }
    return Long.parseLong(value.toString());
  }

  public static boolean isTimeType(LogicalType logicalType) {
    return logicalType.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
      || logicalType.getTypeRoot() == LogicalTypeRoot.DATE
      || logicalType.getTypeRoot() == LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE;
  }

  public static LocalDateTime toLocalDateTime(String timestampString) {
    try {
      return parseTimestampData(timestampString);
    } catch (DateTimeParseException e) {
      return LocalDateTime.parse(timestampString);
    }
  }

  public static LocalDateTime parseTimestampData(String dateStr) throws DateTimeException {
    // Precision is hardcoded to match signature of TO_TIMESTAMP
    //  https://issues.apache.org/jira/browse/FLINK-14925
    return parseTimestampData(dateStr, 3);
  }

  public static LocalDateTime parseTimestampData(String dateStr, int precision)
       throws DateTimeException {
    return fromTemporalAccessor(DEFAULT_TIMESTAMP_FORMATTER.parse(dateStr), precision);
  }

  public static long toMills(LocalDateTime dateTime) {
    return TimestampData.fromLocalDateTime(dateTime).getMillisecond();
  }

  private static LocalDateTime fromTemporalAccessor(TemporalAccessor accessor, int precision) {
    // complement year with 1970
    int year = accessor.isSupported(YEAR) ? accessor.get(YEAR) : 1970;
    // complement month with 1
    int month = accessor.isSupported(MONTH_OF_YEAR) ? accessor.get(MONTH_OF_YEAR) : 1;
    // complement day with 1
    int day = accessor.isSupported(DAY_OF_MONTH) ? accessor.get(DAY_OF_MONTH) : 1;
    // complement hour with 0
    int hour = accessor.isSupported(HOUR_OF_DAY) ? accessor.get(HOUR_OF_DAY) : 0;
    // complement minute with 0
    int minute = accessor.isSupported(MINUTE_OF_HOUR) ? accessor.get(MINUTE_OF_HOUR) : 0;
    // complement second with 0
    int second = accessor.isSupported(SECOND_OF_MINUTE) ? accessor.get(SECOND_OF_MINUTE) : 0;
    // complement nano_of_second with 0
    int nanoOfSecond = accessor.isSupported(NANO_OF_SECOND) ? accessor.get(NANO_OF_SECOND) : 0;

    if (precision == 0) {
      nanoOfSecond = 0;
    } else if (precision != 9) {
      nanoOfSecond = (int) floor(nanoOfSecond, powerX(10, 9 - precision));
    }

    return LocalDateTime.of(year, month, day, hour, minute, second, nanoOfSecond);
  }

  private static long floor(long a, long b) {
    long r = a % b;
    if (r < 0) {
      return a - r - b;
    } else {
      return a - r;
    }
  }

  private static long powerX(long a, long b) {
    long x = 1;
    while (b > 0) {
      x *= a;
      --b;
    }
    return x;
  }

}
