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

package org.apache.hudi.utilities.transform;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFlatteningTransformer {

  @Test
  public void testFlatten() {
    FlatteningTransformer transformer = new FlatteningTransformer();

    // Init
    StructField[] nestedStructFields =
        new StructField[] {new StructField("nestedIntColumn", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("nestedStringColumn", DataTypes.StringType, true, Metadata.empty()),};

    StructField[] structFields =
        new StructField[] {new StructField("intColumn", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("stringColumn", DataTypes.StringType, true, Metadata.empty()),
            new StructField("nestedStruct", DataTypes.createStructType(nestedStructFields), true, Metadata.empty())};

    StructType schema = new StructType(structFields);
    String flattenedSql = transformer.flattenSchema(schema, null);

    assertEquals("intColumn as intColumn,stringColumn as stringColumn,"
        + "nestedStruct.nestedIntColumn as nestedStruct_nestedIntColumn,"
        + "nestedStruct.nestedStringColumn as nestedStruct_nestedStringColumn", flattenedSql);
  }
}
