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

import org.apache.hudi.adapter.DataTypeAdapter;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Tool class used to convert from Avro {@link GenericRecord} to {@link RowData}.
 *
 * <p>NOTE: reference from Flink release 1.12.0, should remove when Flink version upgrade to that.
 */
@Internal
public class AvroToRowDataConverters {

  /**
   * Runtime converter that converts Avro data structures into objects of Flink Table & SQL
   * internal data structures.
   */
  @FunctionalInterface
  public interface AvroToRowDataConverter extends Serializable {
    Object convert(Object object);
  }

  // -------------------------------------------------------------------------------------
  // Runtime Converters
  // -------------------------------------------------------------------------------------

  /**
   * Creates a row converter from the Flink row type using UTC timezone conversion.
   *
   * <p>Note that this RowType-only path cannot recover Hoodie-specific logical type metadata
   * that is not represented in Flink's {@link RowType}.
   */
  public static AvroToRowDataConverter createRowConverter(RowType rowType) {
    return createRowConverter(rowType, true);
  }

  /**
   * Creates a row converter from the Flink row type.
   *
   * <p>Note that this RowType-only path cannot recover Hoodie-specific logical type metadata
   * that is not represented in Flink's {@link RowType}.
   */
  public static AvroToRowDataConverter createRowConverter(RowType rowType, boolean utcTimezone) {
    return createRowConverter(HoodieSchemaConverter.convertToSchema(rowType), rowType, utcTimezone);
  }

  /**
   * Creates a row converter using Hoodie schema.
   */
  public static AvroToRowDataConverter createRowConverter(HoodieSchema hoodieSchema) {
    return createRowConverter(hoodieSchema, (RowType) HoodieSchemaConverter.convertToDataType(hoodieSchema).getLogicalType(), true);
  }

  /**
   * Creates a row converter using both Hoodie schema metadata and the target Flink row type.
   */
  public static AvroToRowDataConverter createRowConverter(HoodieSchema schema, RowType rowType, boolean utcTimezone) {
    HoodieSchema recordSchema = schema.getNonNullType();
    if (recordSchema.getType() == HoodieSchemaType.UNION) {
      // getNonNullType() unwraps simple nullable unions only. Complex unions can still reach
      // here when their Flink representation is a ROW, for example fields inside
      // ColumnStatsSchemas.METADATA_SCHEMA. In that case the RowType already captures the
      // target Flink shape, so use the first union branch only as the positional Hoodie schema
      // template for building nested field converters.
      recordSchema = recordSchema.getTypes().get(0);
    }
    final List<HoodieSchemaField> fields = recordSchema.getFields();
    final AvroToRowDataConverter[] fieldConverters = IntStream.range(0, rowType.getFieldCount())
        .mapToObj(i -> createNullableConverter(fields.get(i).schema(), rowType.getTypeAt(i), utcTimezone))
        .toArray(AvroToRowDataConverter[]::new);
    final int arity = rowType.getFieldCount();

    return avroObject -> {
      IndexedRecord record = (IndexedRecord) avroObject;
      GenericRowData row = new GenericRowData(arity);
      for (int i = 0; i < arity; ++i) {
        row.setField(i, fieldConverters[i].convert(record.get(i)));
      }
      return row;
    };
  }

  /**
   * Creates a runtime converter which is null safe.
   */
  private static AvroToRowDataConverter createNullableConverter(
      HoodieSchema schema,
      LogicalType type,
      boolean utcTimezone) {
    final AvroToRowDataConverter converter = createConverter(schema, type, utcTimezone);
    return avroObject -> {
      if (avroObject == null) {
        return null;
      }
      return converter.convert(avroObject);
    };
  }

  /**
   * Creates a runtime converter which assuming input object is not null.
   */
  public static AvroToRowDataConverter createConverter(LogicalType type, boolean utcTimezone) {
    return createConverter(HoodieSchemaConverter.convertToSchema(type), type, utcTimezone);
  }

  /**
   * Creates a runtime converter with both Hoodie schema and Flink logical type information.
   *
   * <p>The recursive converter construction keeps these two type views together: Flink
   * {@link LogicalType} decides the target RowData representation, while {@link HoodieSchema}
   * carries Hoodie-specific logical metadata such as VECTOR dimension and element type.
   */
  private static AvroToRowDataConverter createConverter(HoodieSchema schema, LogicalType type, boolean utcTimezone) {
    HoodieSchema nonNullSchema = schema.getNonNullType();

    switch (type.getTypeRoot()) {
      case NULL:
        return avroObject -> null;
      case TINYINT:
        return avroObject -> ((Integer) avroObject).byteValue();
      case SMALLINT:
        return avroObject -> ((Integer) avroObject).shortValue();
      case BOOLEAN: // boolean
      case INTEGER: // int
      case INTERVAL_YEAR_MONTH: // long
      case BIGINT: // long
      case INTERVAL_DAY_TIME: // long
      case FLOAT: // float
      case DOUBLE: // double
      case RAW:
        return avroObject -> avroObject;
      case DATE:
        return AvroToRowDataConverters::convertToDate;
      case TIME_WITHOUT_TIME_ZONE:
        return AvroToRowDataConverters::convertToTime;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return createTimestampConverter(((LocalZonedTimestampType) type).getPrecision(), true);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return createTimestampConverter(((TimestampType) type).getPrecision(), utcTimezone);
      case CHAR:
      case VARCHAR:
        return avroObject -> avroObject instanceof Utf8
            ? StringData.fromBytes(((Utf8) avroObject).getBytes())
            : StringData.fromString(avroObject.toString());
      case BINARY:
      case VARBINARY:
        return AvroToRowDataConverters::convertToBytes;
      case DECIMAL:
        return createDecimalConverter((DecimalType) type);
      case ARRAY:
        if (nonNullSchema.getType() == HoodieSchemaType.VECTOR) {
          HoodieSchema.Vector vectorSchema = (HoodieSchema.Vector) nonNullSchema;
          VectorConversionUtils.validateVectorLogicalType(vectorSchema, type);
          return createVectorConverter(vectorSchema);
        }
        return createArrayConverter(nonNullSchema.getElementType(), (ArrayType) type, utcTimezone);
      case ROW:
        return createRowConverter(nonNullSchema, (RowType) type, utcTimezone);
      case MAP:
      case MULTISET:
        return createMapConverter(nonNullSchema.getValueType(), type, utcTimezone);
      default:
        if (DataTypeAdapter.isVariantType(type)) {
          return createVariantConverter();
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  private static AvroToRowDataConverter createVectorConverter(HoodieSchema.Vector vectorSchema) {
    return avroObject -> VectorConversionUtils.createVectorArrayData(convertToBytes(avroObject), vectorSchema);
  }

  private static AvroToRowDataConverter createDecimalConverter(DecimalType decimalType) {
    final int precision = decimalType.getPrecision();
    final int scale = decimalType.getScale();
    return avroObject -> {
      final byte[] bytes;
      if (avroObject instanceof GenericFixed) {
        bytes = ((GenericFixed) avroObject).bytes();
      } else if (avroObject instanceof ByteBuffer) {
        ByteBuffer byteBuffer = (ByteBuffer) avroObject;
        bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
      } else {
        bytes = (byte[]) avroObject;
      }
      return DecimalData.fromUnscaledBytes(bytes, precision, scale);
    };
  }

  private static AvroToRowDataConverter createArrayConverter(
      HoodieSchema elementSchema,
      ArrayType arrayType,
      boolean utcTimezone) {
    final AvroToRowDataConverter elementConverter =
        createNullableConverter(elementSchema, arrayType.getElementType(), utcTimezone);
    final Class<?> elementClass =
        LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());

    return avroObject -> {
      final List<?> list = (List<?>) avroObject;
      final int length = list.size();
      final Object[] array = (Object[]) Array.newInstance(elementClass, length);
      for (int i = 0; i < length; ++i) {
        array[i] = elementConverter.convert(list.get(i));
      }
      return new GenericArrayData(array);
    };
  }

  private static AvroToRowDataConverter createMapConverter(
      HoodieSchema valueSchema,
      LogicalType type,
      boolean utcTimezone) {
    final AvroToRowDataConverter keyConverter =
        createConverter(DataTypes.STRING().getLogicalType(), utcTimezone);
    final AvroToRowDataConverter valueConverter =
        createNullableConverter(valueSchema, HoodieSchemaConverter.extractValueTypeToMap(type), utcTimezone);

    return avroObject -> {
      final Map<?, ?> map = (Map<?, ?>) avroObject;
      Map<Object, Object> result = new HashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object key = keyConverter.convert(entry.getKey());
        Object value = valueConverter.convert(entry.getValue());
        result.put(key, value);
      }
      return new GenericMapData(result);
    };
  }

  /**
   * Creates a converter for Flink 2.1+ VARIANT LogicalType. The converter receives an Avro
   * GenericRecord carrying metadata/value binary fields and produces a Flink
   * {@code BinaryVariant}.
   */
  private static AvroToRowDataConverter createVariantConverter() {
    return avroObject -> {
      IndexedRecord record = (IndexedRecord) avroObject;
      return DataTypeAdapter.createVariant(convertToBytes(record.get(1)), convertToBytes(record.get(0)));
    };
  }

  private static AvroToRowDataConverter createTimestampConverter(int precision, boolean utcTimezone) {
    final ChronoUnit chronoUnit;
    if (precision <= 3) {
      chronoUnit = ChronoUnit.MILLIS;
    } else if (precision <= 6) {
      chronoUnit = ChronoUnit.MICROS;
    } else {
      throw new IllegalArgumentException(
          "Avro does not support TIMESTAMP type with precision: "
              + precision
              + ", it only support precisions <= 6.");
    }
    return avroObject -> {
      final Instant instant;
      if (avroObject instanceof Long) {
        instant = Instant.EPOCH.plus((Long) avroObject, chronoUnit);
      } else if (avroObject instanceof Instant) {
        instant = (Instant) avroObject;
      } else {
        JodaConverter jodaConverter = JodaConverter.getConverter();
        if (jodaConverter != null) {
          // joda time has only millisecond precision
          instant = Instant.ofEpochMilli(jodaConverter.convertTimestamp(avroObject));
        } else {
          throw new IllegalArgumentException(
              "Unexpected object type for TIMESTAMP logical type. Received: " + avroObject);
        }
      }
      if (utcTimezone) {
        return TimestampData.fromInstant(instant);
      } else {
        return TimestampData.fromTimestamp(Timestamp.from(instant)); // this applies the local timezone
      }
    };
  }

  private static int convertToDate(Object object) {
    if (object instanceof Integer) {
      return (Integer) object;
    } else if (object instanceof LocalDate) {
      return (int) ((LocalDate) object).toEpochDay();
    } else {
      JodaConverter jodaConverter = JodaConverter.getConverter();
      if (jodaConverter != null) {
        return (int) jodaConverter.convertDate(object);
      } else {
        throw new IllegalArgumentException(
            "Unexpected object type for DATE logical type. Received: " + object);
      }
    }
  }

  private static int convertToTime(Object object) {
    final int millis;
    if (object instanceof Integer) {
      millis = (Integer) object;
    } else if (object instanceof LocalTime) {
      millis = ((LocalTime) object).get(ChronoField.MILLI_OF_DAY);
    } else {
      JodaConverter jodaConverter = JodaConverter.getConverter();
      if (jodaConverter != null) {
        millis = jodaConverter.convertTime(object);
      } else {
        throw new IllegalArgumentException(
            "Unexpected object type for TIME logical type. Received: " + object);
      }
    }
    return millis;
  }

  private static byte[] convertToBytes(Object object) {
    if (object instanceof GenericFixed) {
      return ((GenericFixed) object).bytes();
    } else if (object instanceof ByteBuffer) {
      ByteBuffer byteBuffer = (ByteBuffer) object;
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return bytes;
    } else {
      return (byte[]) object;
    }
  }

  /**
   * Encapsulates joda optional dependency. Instantiates this class only if joda is available on the
   * classpath.
   */
  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  static class JodaConverter {

    private static JodaConverter instance;

    public static JodaConverter getConverter() {
      if (instance != null) {
        return instance;
      }

      try {
        Class.forName(
            "org.joda.time.DateTime",
            false,
            Thread.currentThread().getContextClassLoader());
        instance = new JodaConverter();
      } catch (ClassNotFoundException e) {
        instance = null;
      }
      return instance;
    }

    public long convertDate(Object object) {
      final org.joda.time.LocalDate value = (org.joda.time.LocalDate) object;
      return LocalDate.of(
          value.getYear(), value.getMonthOfYear(), value.getDayOfMonth()).toEpochDay();
    }

    public int convertTime(Object object) {
      final org.joda.time.LocalTime value = (org.joda.time.LocalTime) object;
      return value.get(DateTimeFieldType.millisOfDay());
    }

    public long convertTimestamp(Object object) {
      final DateTime value = (DateTime) object;
      return value.toDate().getTime();
    }
  }
}
