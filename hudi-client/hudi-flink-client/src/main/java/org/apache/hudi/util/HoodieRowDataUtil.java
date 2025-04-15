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

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.hudi.util.RowDataToAvroConverters.precision;

/**
 * Utils for get/set operations on {@link RowData}.
 */
public class HoodieRowDataUtil {

  private static final Map<Pair<Schema, String>, RowData.FieldGetter> FIELD_GETTER_CACHE = new ConcurrentHashMap<>();

  private static final Map<Schema, RowDataToAvroConverters.RowDataToAvroConverter> ROWDATA_CONVERTER_CACHE = new ConcurrentHashMap<>();

  private static final Conversions.DecimalConversion DECIMAL_CONVERSION = new Conversions.DecimalConversion();

  /**
   * Utils to get {@code RowDataToAvroConverter} from cache.
   *
   * @param schema schema of record
   * @param utcTimezone whether writing timestamp data with UTC timezone
   * @return a {@code RowDataToAvroConverter} from cache or newly created one if not existed in cache.
   */
  public static RowDataToAvroConverters.RowDataToAvroConverter getRowDataToAvroConverter(Schema schema, boolean utcTimezone) {
    return ROWDATA_CONVERTER_CACHE.computeIfAbsent(schema, s -> {
      LogicalType rowType = AvroSchemaConverter.convertToDataType(s).getLogicalType();
      return RowDataToAvroConverters.createConverter(rowType, utcTimezone);
    });
  }

  /**
   * Utils to get FieldGetter from cache.
   *
   * @param schema schema of record
   * @param fieldName name of the field
   * @return FieldGetter from cache or newly created one if not existed in cache.
   */
  public static RowData.FieldGetter getFieldGetter(Schema schema, String fieldName) {
    Pair<Schema, String> cacheKey = Pair.of(schema, fieldName);

    return FIELD_GETTER_CACHE.computeIfAbsent(cacheKey, schemaStringPair -> {
      Schema.Field field = schema.getField(fieldName);
      if (field == null) {
        throw new HoodieException(String.format("Column: %s does not exist in schema: %s", fieldName, schema));
      }
      int fieldPos = schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()).indexOf(fieldName);
      LogicalType fieldType = AvroSchemaConverter.convertToDataType(field.schema()).getLogicalType();
      return RowData.createFieldGetter(fieldType, fieldPos);
    });
  }

  public static Comparable<?> convertToNativeJavaType(Object fieldVal, LogicalType fieldType) {
    switch (fieldType.getTypeRoot()) {
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        int precision = precision(fieldType);
        if (precision <= 3) {
          return ((TimestampData) fieldVal).toInstant().toEpochMilli();
        } else if (precision <= 6) {
          Instant instant = ((TimestampData) fieldVal).toInstant();
          return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1000_000), instant.getNano() / 1000);
        } else {
          throw new UnsupportedOperationException("Unsupported timestamp precision: " + precision);
        }
      case DATE:
        return java.sql.Date.valueOf(LocalDate.ofEpochDay((Integer) fieldVal).toString());
      case CHAR:
      case VARCHAR:
        return fieldVal.toString();
      case BINARY:
      case VARBINARY:
        return ByteBuffer.wrap((byte[]) fieldVal);
      case DECIMAL:
        return ((DecimalData) fieldVal).toBigDecimal();
      default:
        return (Comparable<?>) fieldVal;
    }
  }
}
