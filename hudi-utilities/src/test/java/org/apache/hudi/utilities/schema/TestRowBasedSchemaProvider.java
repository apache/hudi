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

package org.apache.hudi.utilities.schema;

import org.apache.avro.Schema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for {@link RowBasedSchemaProvider}.
 */
class TestRowBasedSchemaProvider {

  @Test
  void testSourceSchemaIsDerivedFromRowStruct() {
    Schema sourceSchema = new RowBasedSchemaProvider(
        new StructType().add("id", DataTypes.LongType, false)).getSourceSchema();

    assertEquals(RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME, sourceSchema.getName());
    assertEquals(RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE, sourceSchema.getNamespace());
    assertNotNull(sourceSchema.getField("id"));
    assertEquals(Schema.Type.LONG, sourceSchema.getField("id").schema().getType());
  }
}
