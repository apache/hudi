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

package org.apache.hudi.internal.schema.convert;

import org.apache.hudi.avro.AvroSchemaTestUtils;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.avro.AvroSchemaTestUtils.createArrayField;
import static org.apache.hudi.avro.AvroSchemaTestUtils.createMapField;
import static org.apache.hudi.avro.AvroSchemaTestUtils.createNullablePrimitiveField;
import static org.apache.hudi.avro.AvroSchemaTestUtils.createNullableRecord;
import static org.apache.hudi.avro.AvroSchemaTestUtils.createPrimitiveField;
import static org.apache.hudi.avro.AvroSchemaTestUtils.createRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAvroInternalSchemaConverter {

  @Test
  public void testCollectColumnNames() {
    Schema simpleSchema = createRecord("simpleSchema",
        createPrimitiveField("field1", Schema.Type.INT),
        createPrimitiveField("field2", Schema.Type.STRING));

    List<String> fieldNames =  AvroInternalSchemaConverter.collectColNamesFromSchema(simpleSchema);
    List<String> expectedOutput = Arrays.asList("field1", "field2");
    assertEquals(expectedOutput.size(), fieldNames.size());
    assertTrue(fieldNames.containsAll(expectedOutput));


    Schema simpleSchemaWithNullable = createRecord("simpleSchemaWithNullable",
        createNullablePrimitiveField("field1", Schema.Type.INT),
        createPrimitiveField("field2", Schema.Type.STRING));
    fieldNames =  AvroInternalSchemaConverter.collectColNamesFromSchema(simpleSchemaWithNullable);
    expectedOutput = Arrays.asList("field1", "field2");
    assertEquals(expectedOutput.size(), fieldNames.size());
    assertTrue(fieldNames.containsAll(expectedOutput));

    Schema complexSchemaSingleLevel = createRecord("complexSchemaSingleLevel",
        AvroSchemaTestUtils.createNestedField("field1", Schema.Type.INT),
        createArrayField("field2", Schema.Type.STRING),
        createMapField("field3", Schema.Type.DOUBLE));
    fieldNames =  AvroInternalSchemaConverter.collectColNamesFromSchema(complexSchemaSingleLevel);
    expectedOutput = Arrays.asList("field1.nested", "field2.element", "field3.key", "field3.value");
    assertEquals(expectedOutput.size(), fieldNames.size());
    assertTrue(fieldNames.containsAll(expectedOutput));

    Schema deeplyNestedField = createRecord("deeplyNestedField",
        createPrimitiveField("field1", Schema.Type.INT),
        new Schema.Field("field2",
            createRecord("field2nest",
                createArrayField("field2nestarray",
                    createNullableRecord("field2nestarraynest",
                        createNullablePrimitiveField("field21", Schema.Type.INT),
                        createNullablePrimitiveField("field22", Schema.Type.INT))))),
        createNullablePrimitiveField("field3", Schema.Type.INT));
    fieldNames =  AvroInternalSchemaConverter.collectColNamesFromSchema(deeplyNestedField);
    expectedOutput = Arrays.asList("field1", "field2.field2nestarray.element.field21",
        "field2.field2nestarray.element.field22", "field3");
    assertEquals(expectedOutput.size(), fieldNames.size());
    assertTrue(fieldNames.containsAll(expectedOutput));
  }
}
