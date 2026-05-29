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

package org.apache.hudi.keygen;

import org.apache.hudi.util.JavaScalaConverters;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class KeyGeneratorTestUtilities {

  public static final String NESTED_COL_SCHEMA = "{\"type\":\"record\", \"name\":\"nested_col\",\"fields\": ["
      + "{\"name\": \"prop1\",\"type\": [\"null\", \"string\"]},{\"name\": \"prop2\", \"type\": \"long\"}]}";
  public static final String EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"long\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"ts_ms\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"nested_col\",\"type\": [\"null\", " + NESTED_COL_SCHEMA + "]}"
      + "]}";

  private static final StructType FARE_STRUCT_TYPE = new StructType(new StructField[] {
      new StructField("amount", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("currency", DataTypes.StringType, false, Metadata.empty())
  });
  protected static final StructType TRIP_STRUCT_TYPE = new StructType(new StructField[] {
      new StructField("timestamp", DataTypes.LongType, false, Metadata.empty()),
      new StructField("_row_key", DataTypes.StringType, false, Metadata.empty()),
      new StructField("partition_path", DataTypes.StringType, true, Metadata.empty()),
      new StructField("trip_type", DataTypes.StringType, false, Metadata.empty()),
      new StructField("rider", DataTypes.StringType, false, Metadata.empty()),
      new StructField("driver", DataTypes.StringType, false, Metadata.empty()),
      new StructField("begin_lat", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("begin_lon", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("end_lat", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("end_lon", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("distance_in_meters", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("seconds_since_epoch", DataTypes.LongType, false, Metadata.empty()),
      new StructField("weight", DataTypes.FloatType, false, Metadata.empty()),
      new StructField("nation", DataTypes.BinaryType, false, Metadata.empty()),
      new StructField("current_date", DataTypes.DateType, false, Metadata.empty()),
      new StructField("current_ts", DataTypes.LongType, false, Metadata.empty()),
      new StructField("height", new DecimalType(10, 6), false, Metadata.empty()),
      new StructField("city_to_state", DataTypes.createMapType(
          DataTypes.StringType, DataTypes.StringType, false), false, Metadata.empty()),
      new StructField("fare", FARE_STRUCT_TYPE, false, Metadata.empty()),
      new StructField("tip_history", DataTypes.createArrayType(FARE_STRUCT_TYPE, false), false, Metadata.empty()),
      new StructField("_hoodie_is_deleted", DataTypes.BooleanType, false, Metadata.empty())
  });
  private static final StructType NESTED_TYPE = new StructType(new StructField[] {
      new StructField("prop1", DataTypes.StringType, true, Metadata.empty()),
      new StructField("prop2", DataTypes.LongType, false, Metadata.empty())
  });
  private static final StructType STRUCT_TYPE = new StructType(new StructField[] {
      new StructField("timestamp", DataTypes.LongType, false, Metadata.empty()),
      new StructField("_row_key", DataTypes.StringType, false, Metadata.empty()),
      new StructField("ts_ms", DataTypes.StringType, false, Metadata.empty()),
      new StructField("pii_col", DataTypes.StringType, false, Metadata.empty()),
      new StructField("nested_col", NESTED_TYPE, true, Metadata.empty())
  });

  public static GenericRecord getRecord() {
    return getRecord(getNestedColRecord("val1", 10L));
  }

  public static GenericRecord getNestedColRecord(String prop1Value, Long prop2Value) {
    GenericRecord nestedColRecord = new GenericData.Record(new Schema.Parser().parse(NESTED_COL_SCHEMA));
    nestedColRecord.put("prop1", prop1Value);
    nestedColRecord.put("prop2", prop2Value);
    return nestedColRecord;
  }

  public static GenericRecord getRecord(GenericRecord nestedColRecord) {
    GenericRecord avroRecord = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    avroRecord.put("timestamp", 4357686L);
    avroRecord.put("_row_key", "key1");
    avroRecord.put("ts_ms", "2020-03-21");
    avroRecord.put("pii_col", "pi");
    avroRecord.put("nested_col", nestedColRecord);
    return avroRecord;
  }

  public static Row getRow(GenericRecord genericRecord) {
    return getRow(genericRecord, STRUCT_TYPE);
  }

  public static Row getRow(GenericRecord genericRecord, StructType structType) {
    Row row = genericRecordToRow(genericRecord, structType);
    int fieldCount = structType.fieldNames().length;
    Object[] values = new Object[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      values[i] = row.get(i);
    }
    return new GenericRowWithSchema(values, structType);
  }

  public static InternalRow getInternalRow(Row row) {
    List<Object> values = IntStream.range(0, row.schema().fieldNames().length)
        .mapToObj(row::get).collect(Collectors.toList());
    return InternalRow.apply(JavaScalaConverters.convertJavaListToScalaList(values));
  }

  /**
   * Converts an Avro GenericRecord to a Spark SQL Row based on a given schema.
   * Only used by key generator tests.
   *
   * @param genericRecord the input Avro GenericRecord
   * @param schema the target Spark SQL StructType that defines the structure of the output Row
   * @return a Spark SQL Row containing the data from the GenericRecord
   */
  public static Row genericRecordToRow(GenericRecord genericRecord, StructType schema) {
    StructField[] fields = schema.fields();
    Object[] values = new Object[fields.length];

    for (int i = 0; i < fields.length; i++) {
      StructField field = fields[i];
      Object avroValue = genericRecord.get(field.name());
      values[i] = convertAvroValue(avroValue, field.dataType());
    }

    return new GenericRowWithSchema(values, schema);
  }

  /**
   * Recursively converts an Avro value to its corresponding Spark SQL data type.
   * Only used by key generator tests.
   *
   * @param value    the value to convert
   * @param dataType the target Spark SQL DataType
   * @return the converted value compatible with Spark SQL
   */
  private static Object convertAvroValue(Object value, DataType dataType) {
    if (value == null) {
      return null;
    }

    if (dataType instanceof StringType
        && (value instanceof Utf8 || value instanceof GenericData.EnumSymbol || value instanceof String)) {
      return value.toString();
    } else if (dataType instanceof DecimalType && value instanceof GenericData.Fixed) {
      DecimalType decimalType = (DecimalType) dataType;
      byte[] bytes = ((GenericData.Fixed) value).bytes();
      return new BigDecimal(new BigInteger(bytes), decimalType.scale());
    } else if (dataType instanceof StructType && value instanceof GenericRecord) {
      return genericRecordToRow((GenericRecord) value, (StructType) dataType);
    } else if (dataType instanceof ArrayType && value instanceof List) {
      ArrayType arrayType = (ArrayType) dataType;
      List<?> avroList = (List<?>) value;
      return avroList.stream()
          .map(item -> convertAvroValue(item, arrayType.elementType()))
          .collect(Collectors.toList());
    } else if (dataType instanceof MapType && value instanceof Map) {
      MapType mapType = (MapType) dataType;
      Map<?, ?> avroMap = (Map<?, ?>) value;
      return avroMap.entrySet().stream()
          .collect(Collectors.toMap(
              e -> e.getKey().toString(), // Convert key to String
              e -> convertAvroValue(e.getValue(), mapType.valueType())
          ));
    }
    return value;
  }
}