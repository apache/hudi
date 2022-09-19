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

package org.apache.hudi.testutils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.AvroConversionUtils;
import org.apache.spark.package$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;

public class KeyGeneratorTestUtilities {

  public static final String NESTED_COL_SCHEMA = "{\"type\":\"record\", \"name\":\"nested_col\",\"fields\": ["
      + "{\"name\": \"prop1\",\"type\": [\"null\", \"string\"]},{\"name\": \"prop2\", \"type\": \"long\"}]}";
  public static final String EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"long\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"ts_ms\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"nested_col\",\"type\": [\"null\", " + NESTED_COL_SCHEMA + "]}"
      + "]}";

  public static final String TEST_STRUCTNAME = "test_struct_name";
  public static final String TEST_RECORD_NAMESPACE = "test_record_namespace";
  public static Schema schema = new Schema.Parser().parse(EXAMPLE_SCHEMA);
  public static StructType structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);

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
    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    record.put("timestamp", 4357686L);
    record.put("_row_key", "key1");
    record.put("ts_ms", "2020-03-21");
    record.put("pii_col", "pi");
    record.put("nested_col", nestedColRecord);
    return record;
  }

  public static Row getRow(GenericRecord record) {
    return getRow(record, schema, structType);
  }

  public static Row getRow(GenericRecord record, Schema schema, StructType structType) {
    Function1<GenericRecord, Row> converterFn = AvroConversionUtils.createConverterToRow(schema, structType);
    Row row = converterFn.apply(record);
    int fieldCount = structType.fieldNames().length;
    Object[] values = new Object[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      values[i] = row.get(i);
    }
    return new GenericRowWithSchema(values, structType);
  }

  public static InternalRow getInternalRow(Row row) {
    try {
      return getInternalRow(row, getEncoder(row.schema()));
    } catch (Exception e) {
      throw new IllegalStateException("Exception thrown while converting Row to InternalRow", e);
    }
  }

  private static ExpressionEncoder getEncoder(StructType schema) {
    List<Attribute> attributes = JavaConversions.asJavaCollection(schema.toAttributes()).stream()
        .map(Attribute::toAttribute).collect(Collectors.toList());
    return RowEncoder.apply(schema)
        .resolveAndBind(JavaConverters.asScalaBufferConverter(attributes).asScala().toSeq(),
            SimpleAnalyzer$.MODULE$);
  }

  public static InternalRow getInternalRow(Row row, ExpressionEncoder<Row> encoder) throws ClassNotFoundException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {
    return serializeRow(encoder, row);
  }

  private static InternalRow serializeRow(ExpressionEncoder encoder, Row row)
      throws InvocationTargetException, IllegalAccessException, NoSuchMethodException, ClassNotFoundException {
    // TODO remove reflection if Spark 2.x support is dropped
    if (package$.MODULE$.SPARK_VERSION().startsWith("2.")) {
      Method spark2method = encoder.getClass().getMethod("toRow", Object.class);
      return (InternalRow) spark2method.invoke(encoder, row);
    } else {
      Class<?> serializerClass = Class.forName("org.apache.spark.sql.catalyst.encoders.ExpressionEncoder$Serializer");
      Object serializer = encoder.getClass().getMethod("createSerializer").invoke(encoder);
      Method aboveSpark2method = serializerClass.getMethod("apply", Object.class);
      return (InternalRow) aboveSpark2method.invoke(serializer, row);
    }
  }

}
