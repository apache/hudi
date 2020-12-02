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

import org.apache.hudi.AvroConversionHelper;
import org.apache.hudi.AvroConversionUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import scala.Function1;

public class KeyGeneratorTestUtilities {

  public static String exampleSchema = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"long\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"ts_ms\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\"}]}";

  public static final String TEST_STRUCTNAME = "test_struct_name";
  public static final String TEST_RECORD_NAMESPACE = "test_record_namespace";
  public static Schema schema = new Schema.Parser().parse(exampleSchema);
  public static StructType structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);

  public GenericRecord getRecord() {
    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(exampleSchema));
    record.put("timestamp", 4357686);
    record.put("_row_key", "key1");
    record.put("ts_ms", "2020-03-21");
    record.put("pii_col", "pi");
    return record;
  }

  public static Row getRow(GenericRecord record) {
    return getRow(record, schema, structType);
  }

  public static Row getRow(GenericRecord record, Schema schema, StructType structType) {
    Function1<Object, Object> converterFn = AvroConversionHelper.createConverterToRow(schema, structType);
    Row row = (Row) converterFn.apply(record);
    int fieldCount = structType.fieldNames().length;
    Object[] values = new Object[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      values[i] = row.get(i);
    }
    return new GenericRowWithSchema(values, structType);
  }
}
