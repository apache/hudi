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

package org.apache.hudi.hadoop.utils;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.AvroSchemaUtils.appendFieldsToSchema;
import static org.apache.hudi.avro.AvroSchemaUtils.createNullableSchema;

public class HoodieRealtimeRecordReaderUtils {
  private static final Logger LOG = LogManager.getLogger(HoodieRealtimeRecordReaderUtils.class);

  /**
   * Reads the schema from the base file.
   */
  public static Schema readSchema(Configuration conf, Path filePath) {
    try {
      HoodieFileReader storageReader = HoodieFileReaderFactory.getFileReader(conf, filePath);
      return storageReader.getSchema();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read schema from " + filePath, e);
    }
  }

  /**
   * get the max compaction memory in bytes from JobConf.
   */
  public static long getMaxCompactionMemoryInBytes(JobConf jobConf) {
    // jobConf.getMemoryForMapTask() returns in MB
    return (long) Math
        .ceil(Double.parseDouble(jobConf.get(HoodieRealtimeConfig.COMPACTION_MEMORY_FRACTION_PROP,
            HoodieRealtimeConfig.DEFAULT_COMPACTION_MEMORY_FRACTION))
            * jobConf.getMemoryForMapTask() * 1024 * 1024L);
  }

  /**
   * Prints a JSON representation of the ArrayWritable for easier debuggability.
   */
  public static String arrayWritableToString(ArrayWritable writable) {
    if (writable == null) {
      return "null";
    }
    StringBuilder builder = new StringBuilder();
    Writable[] values = writable.get();
    builder.append("\"values_" + Math.random() + "_" + values.length + "\": {");
    int i = 0;
    for (Writable w : values) {
      if (w instanceof ArrayWritable) {
        builder.append(arrayWritableToString((ArrayWritable) w)).append(",");
      } else {
        builder.append("\"value" + i + "\":\"" + w + "\"").append(",");
        if (w == null) {
          builder.append("\"type" + i + "\":\"unknown\"").append(",");
        } else {
          builder.append("\"type" + i + "\":\"" + w.getClass().getSimpleName() + "\"").append(",");
        }
      }
      i++;
    }
    builder.deleteCharAt(builder.length() - 1);
    builder.append("}");
    return builder.toString();
  }

  /**
   * Generate a reader schema off the provided writeSchema, to just project out the provided columns.
   */
  public static Schema generateProjectionSchema(Schema writeSchema, Map<String, Schema.Field> schemaFieldsMap,
                                                List<String> fieldNames) {
    /**
     * Avro & Presto field names seems to be case sensitive (support fields differing only in case) whereas
     * Hive/Impala/SparkSQL(default) are case-insensitive. Spark allows this to be configurable using
     * spark.sql.caseSensitive=true
     *
     * For a RT table setup with no delta-files (for a latest file-slice) -> we translate parquet schema to Avro Here
     * the field-name case is dependent on parquet schema. Hive (1.x/2.x/CDH) translate column projections to
     * lower-cases
     *
     */
    List<Schema.Field> projectedFields = new ArrayList<>();
    for (String fn : fieldNames) {
      Schema.Field field = schemaFieldsMap.get(fn.toLowerCase());
      if (field == null) {
        throw new HoodieException("Field " + fn + " not found in log schema. Query cannot proceed! "
            + "Derived Schema Fields: " + new ArrayList<>(schemaFieldsMap.keySet()));
      } else {
        projectedFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()));
      }
    }

    Schema projectedSchema = Schema.createRecord(writeSchema.getName(), writeSchema.getDoc(),
        writeSchema.getNamespace(), writeSchema.isError());
    projectedSchema.setFields(projectedFields);
    return projectedSchema;
  }

  public static Map<String, Schema.Field> getNameToFieldMap(Schema schema) {
    return schema.getFields().stream().map(r -> Pair.of(r.name().toLowerCase(), r))
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
  }

  /**
   * Convert the projected read from delta record into an array writable.
   */
  public static Writable avroToArrayWritable(Object value, Schema schema) {

    if (value == null) {
      return null;
    }

    switch (schema.getType()) {
      case STRING:
        return new Text(value.toString());
      case BYTES:
        return new BytesWritable(((ByteBuffer)value).array());
      case INT:
        if (schema.getLogicalType() != null && schema.getLogicalType().getName().equals("date")) {
          return new DateWritable((Integer) value);
        }
        return new IntWritable((Integer) value);
      case LONG:
        return new LongWritable((Long) value);
      case FLOAT:
        return new FloatWritable((Float) value);
      case DOUBLE:
        return new DoubleWritable((Double) value);
      case BOOLEAN:
        return new BooleanWritable((Boolean) value);
      case NULL:
        return null;
      case RECORD:
        GenericRecord record = (GenericRecord) value;
        Writable[] recordValues = new Writable[schema.getFields().size()];
        int recordValueIndex = 0;
        for (Schema.Field field : schema.getFields()) {
          Object fieldVal = record.hasField(field.name()) ? record.get(field.name()) : null;
          recordValues[recordValueIndex++] = avroToArrayWritable(fieldVal, field.schema());
        }
        return new ArrayWritable(Writable.class, recordValues);
      case ENUM:
        return new Text(value.toString());
      case ARRAY:
        GenericArray arrayValue = (GenericArray) value;
        Writable[] arrayValues = new Writable[arrayValue.size()];
        int arrayValueIndex = 0;
        for (Object obj : arrayValue) {
          arrayValues[arrayValueIndex++] = avroToArrayWritable(obj, schema.getElementType());
        }
        // Hive 1.x will fail here, it requires values2 to be wrapped into another ArrayWritable
        return new ArrayWritable(Writable.class, arrayValues);
      case MAP:
        Map mapValue = (Map) value;
        Writable[] mapValues = new Writable[mapValue.size()];
        int mapValueIndex = 0;
        for (Object entry : mapValue.entrySet()) {
          Map.Entry mapEntry = (Map.Entry) entry;
          Writable[] nestedMapValues = new Writable[2];
          nestedMapValues[0] = new Text(mapEntry.getKey().toString());
          nestedMapValues[1] = avroToArrayWritable(mapEntry.getValue(), schema.getValueType());
          mapValues[mapValueIndex++] = new ArrayWritable(Writable.class, nestedMapValues);
        }
        // Hive 1.x will fail here, it requires values3 to be wrapped into another ArrayWritable
        return new ArrayWritable(Writable.class, mapValues);
      case UNION:
        List<Schema> types = schema.getTypes();
        if (types.size() != 2) {
          throw new IllegalArgumentException("Only support union with 2 fields");
        }
        Schema s1 = types.get(0);
        Schema s2 = types.get(1);
        if (s1.getType() == Schema.Type.NULL) {
          return avroToArrayWritable(value, s2);
        } else if (s2.getType() == Schema.Type.NULL) {
          return avroToArrayWritable(value, s1);
        } else {
          throw new IllegalArgumentException("Only support union with null");
        }
      case FIXED:
        if (schema.getLogicalType() != null && schema.getLogicalType().getName().equals("decimal")) {
          LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) LogicalTypes.fromSchema(schema);
          HiveDecimalWritable writable = new HiveDecimalWritable(((GenericFixed) value).bytes(),
              decimal.getScale());
          return HiveDecimalUtils.enforcePrecisionScale(writable,
              new DecimalTypeInfo(decimal.getPrecision(), decimal.getScale()));
        }
        return new BytesWritable(((GenericFixed) value).bytes());
      default:
        return null;
    }
  }

  /**
   * Given a comma separated list of field names and positions at which they appear on Hive, return
   * an ordered list of field names, that can be passed onto storage.
   */
  public static List<String> orderFields(String fieldNameCsv, String fieldOrderCsv, List<String> partitioningFields) {
    // Need to convert the following to Set first since Hive does not handle duplicate field names correctly but
    // handles duplicate fields orders correctly.
    // Fields Orders -> {@link https://github
    // .com/apache/hive/blob/f37c5de6c32b9395d1b34fa3c02ed06d1bfbf6eb/serde/src/java
    // /org/apache/hadoop/hive/serde2/ColumnProjectionUtils.java#L188}
    // Field Names -> {@link https://github.com/apache/hive/blob/f37c5de6c32b9395d1b34fa3c02ed06d1bfbf6eb/serde/src/java
    // /org/apache/hadoop/hive/serde2/ColumnProjectionUtils.java#L229}
    String[] fieldOrdersWithDups = fieldOrderCsv.isEmpty() ? new String[0] : fieldOrderCsv.split(",");
    Set<String> fieldOrdersSet = new LinkedHashSet<>(Arrays.asList(fieldOrdersWithDups));
    String[] fieldOrders = fieldOrdersSet.toArray(new String[0]);
    List<String> fieldNames = fieldNameCsv.isEmpty() ? new ArrayList<>() : Arrays.stream(fieldNameCsv.split(","))
        .filter(fn -> !partitioningFields.contains(fn)).collect(Collectors.toList());
    Set<String> fieldNamesSet = new LinkedHashSet<>(fieldNames);
    // Hive does not provide ids for partitioning fields, so check for lengths excluding that.
    if (fieldNamesSet.size() != fieldOrders.length) {
      throw new HoodieException(String
          .format("Error ordering fields for storage read. #fieldNames: %d, #fieldPositions: %d",
              fieldNames.size(), fieldOrders.length));
    }
    TreeMap<Integer, String> orderedFieldMap = new TreeMap<>();
    String[] fieldNamesArray = fieldNamesSet.toArray(new String[0]);
    for (int ox = 0; ox < fieldOrders.length; ox++) {
      orderedFieldMap.put(Integer.parseInt(fieldOrders[ox]), fieldNamesArray[ox]);
    }
    return new ArrayList<>(orderedFieldMap.values());
  }

  /**
   * Hive implementation of ParquetRecordReader results in partition columns not present in the original parquet file to
   * also be part of the projected schema. Hive expects the record reader implementation to return the row in its
   * entirety (with un-projected column having null values). As we use writerSchema for this, make sure writer schema
   * also includes partition columns
   *
   * @param schema Schema to be changed
   */
  public static Schema addPartitionFields(Schema schema, List<String> partitioningFields) {
    final Set<String> firstLevelFieldNames =
        schema.getFields().stream().map(Schema.Field::name).map(String::toLowerCase).collect(Collectors.toSet());
    List<String> fieldsToAdd = partitioningFields.stream().map(String::toLowerCase)
        .filter(x -> !firstLevelFieldNames.contains(x)).collect(Collectors.toList());

    return appendNullSchemaFields(schema, fieldsToAdd);
  }

  private static Schema appendNullSchemaFields(Schema schema, List<String> newFieldNames) {
    List<Schema.Field> newFields = new ArrayList<>();
    for (String newField : newFieldNames) {
      newFields.add(new Schema.Field(newField, createNullableSchema(Schema.Type.STRING), "", JsonProperties.NULL_VALUE));
    }
    return appendFieldsToSchema(schema, newFields);
  }
}
