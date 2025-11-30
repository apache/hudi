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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createArrayField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createMapField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createNestedField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createNullablePrimitiveField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createNullableRecord;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createPrimitiveField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestInternalSchemaConverter {

  public static HoodieSchema getSimpleSchema() {
    return createRecord("simpleSchema",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        createPrimitiveField("field2", HoodieSchemaType.STRING));
  }

  public static List<String> getSimpleSchemaExpectedColumnNames() {
    return Arrays.asList("field1", "field2");
  }

  public static HoodieSchema getSimpleSchemaWithNullable() {
    return createRecord("simpleSchemaWithNullable",
        createNullablePrimitiveField("field1", HoodieSchemaType.INT),
        createPrimitiveField("field2", HoodieSchemaType.STRING));
  }

  public static HoodieSchema getComplexSchemaSingleLevel() {
    return createRecord("complexSchemaSingleLevel",
        createNestedField("field1", HoodieSchemaType.INT),
        createArrayField("field2", HoodieSchemaType.STRING),
        createMapField("field3", HoodieSchemaType.DOUBLE));
  }

  public static List<String> getComplexSchemaSingleLevelExpectedColumnNames() {
    return Arrays.asList("field1.nested", "field2.element", "field3.key", "field3.value");
  }

  public static HoodieSchema getDeeplyNestedFieldSchema() {
    return createRecord("deeplyNestedFieldSchema",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        HoodieSchemaField.of("field2",
            createRecord("field2nest",
                createArrayField("field2nestarray",
                    createNullableRecord("field2nestarraynest",
                        createNullablePrimitiveField("field21", HoodieSchemaType.INT),
                        createNullablePrimitiveField("field22", HoodieSchemaType.INT)))), null, null),
        createNullablePrimitiveField("field3", HoodieSchemaType.INT));
  }

  public static List<String> getDeeplyNestedFieldSchemaExpectedColumnNames() {
    return Arrays.asList("field1", "field2.field2nestarray.element.field21",
        "field2.field2nestarray.element.field22", "field3");
  }

  @Test
  public void testCollectColumnNames() {
    HoodieSchema simpleSchema =  getSimpleSchema();
    List<String> fieldNames =  InternalSchemaConverter.collectColNamesFromSchema(simpleSchema);
    List<String> expectedOutput = getSimpleSchemaExpectedColumnNames();
    assertEquals(expectedOutput.size(), fieldNames.size());
    assertTrue(fieldNames.containsAll(expectedOutput));


    HoodieSchema simpleSchemaWithNullable = getSimpleSchemaWithNullable();
    fieldNames =  InternalSchemaConverter.collectColNamesFromSchema(simpleSchemaWithNullable);
    expectedOutput = getSimpleSchemaExpectedColumnNames();
    assertEquals(expectedOutput.size(), fieldNames.size());
    assertTrue(fieldNames.containsAll(expectedOutput));

    HoodieSchema complexSchemaSingleLevel = getComplexSchemaSingleLevel();
    fieldNames =  InternalSchemaConverter.collectColNamesFromSchema(complexSchemaSingleLevel);
    expectedOutput = getComplexSchemaSingleLevelExpectedColumnNames();
    assertEquals(expectedOutput.size(), fieldNames.size());
    assertTrue(fieldNames.containsAll(expectedOutput));

    HoodieSchema deeplyNestedFieldSchema = getDeeplyNestedFieldSchema();
    fieldNames =  InternalSchemaConverter.collectColNamesFromSchema(deeplyNestedFieldSchema);
    expectedOutput = getDeeplyNestedFieldSchemaExpectedColumnNames();
    assertEquals(expectedOutput.size(), fieldNames.size());
    assertTrue(fieldNames.containsAll(expectedOutput));
  }
}
