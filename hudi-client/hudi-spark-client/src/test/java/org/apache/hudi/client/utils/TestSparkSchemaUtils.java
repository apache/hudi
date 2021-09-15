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

package org.apache.hudi.client.utils;

import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.spark.sql.types.ArrayType$;
import org.apache.spark.sql.types.BooleanType$;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.MapType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.util.List;

public class TestSparkSchemaUtils {

  @Test
  public void testConstructSparkSchemaFromInternalSchema() {
    InternalSchema internalSchema = new InternalSchema(getNestRecordType().fields());
    StructType convertType = SparkSchemaUtils.constructSparkSchemaFromInternalSchema(internalSchema);
    Assertions.assertTrue(convertType.equals(getNestStructType()));
  }

  @Test
  public void testCollectColNamesFromStructType() {
    List<String> colNames = SparkSchemaUtils.collectColNamesFromSparkStruct(getNestStructType());
    InternalSchema internalSchema = new InternalSchema(getNestRecordType().fields());
    colNames.stream().forEach(f -> {
      Assertions.assertTrue(internalSchema.findIdByName(f) != -1);
    });
    //
    StructType convertType = SparkSchemaUtils.constructSparkSchemaFromInternalSchema(internalSchema);
    List convertColNames = SparkSchemaUtils.collectColNamesFromSparkStruct(convertType);
    Assertions.assertTrue(colNames.equals(convertColNames));
  }

  @Test
  public void testConvertStructTypeToInternalSchema() {
    InternalSchema internalSchema = new InternalSchema(getNestRecordType().fields());
    InternalSchema convertInternalSchema = SparkSchemaUtils.convertStructTypeToInternalSchema(getNestStructType());
    Assertions.assertTrue(internalSchema.equals(convertInternalSchema));
  }

  private StructType getNestStructType() {
    return new StructType()
        .add("id", IntegerType$.MODULE$, false)
        .add("data", StringType$.MODULE$, true)
        .add("preferences", new StructType().add("feature1", BooleanType$.MODULE$, false).add("feature2", BooleanType$.MODULE$, true), true)
        .add("doubles", ArrayType$.MODULE$
            .apply(DoubleType$.MODULE$, false), false)
        .add("locations", MapType$.MODULE$
            .apply(StringType$.MODULE$, new StructType().add("lat", FloatType$.MODULE$, false).add("long", FloatType$.MODULE$, false), false), false);
  }

  private Types.RecordType getNestRecordType() {
    return Types.RecordType.get(Types.Field.get(0, false, "id", Types.IntType.get()), Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences", Types.RecordType.get(Types.Field.get(5, false, "feature1", Types.BooleanType.get()), Types.Field.get(6, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false, "doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())), Types.Field.get(4, false, "locations",
            Types.MapType.get(8, 9, Types.StringType.get(), Types.RecordType.get(Types.Field.get(10, false, "lat", Types.FloatType.get()), Types.Field.get(11, false, "long", Types.FloatType.get())),
                false)));
  }
}
