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

package org.apache.hudi.internal.schema;

import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

public class TestSerDeHelper {

  @Test
  public void testComplexSchema2Json() {
    InternalSchema internalSchema = new InternalSchema(Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(7, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(8, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false, "locations", Types.MapType.get(9, 10, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(11, false, "lat", Types.FloatType.get()), Types.Field.get(12, false, "long", Types.FloatType.get())), false)),
        Types.Field.get(4, true, "points", Types.ArrayType.get(13, true,
            Types.RecordType.get(Types.Field.get(14, false, "x", Types.LongType.get()), Types.Field.get(15, false, "y", Types.LongType.get())))),
        Types.Field.get(5, false,"doubles", Types.ArrayType.get(16, false, Types.DoubleType.get())),
        Types.Field.get(6, true, "properties", Types.MapType.get(17, 18, Types.StringType.get(), Types.StringType.get()))
    );
    // test schema2json
    String result = SerDeHelper.toJson(internalSchema);
    InternalSchema convertedSchema = SerDeHelper.fromJson(result).get();
    Assertions.assertEquals(internalSchema, convertedSchema);
    // test schemas2json
    String results = SerDeHelper.toJson(Arrays.asList(internalSchema));
    TreeMap<Long, InternalSchema> convertedSchemas = SerDeHelper.parseSchemas(results);
    Assertions.assertEquals(1, convertedSchemas.size());
  }

  @Test
  public void testPrimitive2Json() {
    Types.RecordType record = Types.RecordType.get(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "bool", Types.BooleanType.get()),
        Types.Field.get(1, "int", Types.IntType.get()),
        Types.Field.get(2, "long", Types.LongType.get()),
        Types.Field.get(3, "float", Types.FloatType.get()),
        Types.Field.get(4, "double", Types.DoubleType.get()),
        Types.Field.get(5, "date", Types.DateType.get()),
        Types.Field.get(6, "time", Types.TimeType.get()),
        Types.Field.get(7, "timestamp", Types.TimestampType.get()),
        Types.Field.get(8, "string", Types.StringType.get()),
        Types.Field.get(9, "uuid", Types.UUIDType.get()),
        Types.Field.get(10, "fixed", Types.FixedType.getFixed(10)),
        Types.Field.get(11, "binary", Types.BinaryType.get()),
        Types.Field.get(12, "decimal", Types.DecimalType.get(10, 2))
    }));
    InternalSchema internalSchema = new InternalSchema(record.fields());
    String result = SerDeHelper.toJson(internalSchema);
    InternalSchema convertedSchema = SerDeHelper.fromJson(result).get();
    Assertions.assertEquals(internalSchema, convertedSchema);
  }

  @Test
  public void testSearchSchema() {
    List schemas = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      schemas.add(new InternalSchema(i * 10,
          Arrays.asList(Types.Field.get(1, true, "schema" + i * 10, Types.LongType.get()))));
    }

    Assertions.assertEquals(InternalSchemaUtils.searchSchema(0, schemas).getRecord().fields().get(0),
        Types.Field.get(1, true, "schema" + 0, Types.LongType.get()));

    Assertions.assertEquals(InternalSchemaUtils.searchSchema(9, schemas).getRecord().fields().get(0),
        Types.Field.get(1, true, "schema" + 0, Types.LongType.get()));

    Assertions.assertEquals(InternalSchemaUtils.searchSchema(99, schemas).getRecord().fields().get(0),
        Types.Field.get(1, true, "schema" + 90, Types.LongType.get()));

    Assertions.assertEquals(InternalSchemaUtils.searchSchema(9999, schemas).getRecord().fields().get(0),
        Types.Field.get(1, true, "schema" + 990, Types.LongType.get()));
  }

  @Test
  public void testInheritSchemas() {
    List schemas = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      schemas.add(new InternalSchema(i,
          Arrays.asList(Types.Field.get(1, true, "schema" + i, Types.LongType.get()))));
    }
    String oldSchemas = SerDeHelper.toJson(schemas);
    InternalSchema newSchema = new InternalSchema(3,
        Arrays.asList(Types.Field.get(1, true, "schema" + 3, Types.LongType.get())));

    String finalResult = SerDeHelper.inheritSchemas(newSchema, oldSchemas);
    // convert back
    Assertions.assertEquals(SerDeHelper.parseSchemas(finalResult).size(), 3);
  }
}

