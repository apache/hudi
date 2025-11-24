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

package org.apache.hudi.hive.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.hive.SchemaDifference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHiveSchemaUtil {

  /**
   * Testing converting array types to Hive field declaration strings.
   * <p>
   * Refer to the Parquet-113 spec: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
   */
  @Test
  public void testSchemaConvertArray() throws IOException {
    // A list of integers
    HoodieSchema schema = HoodieSchema.createRecord("message", null, null,
        Collections.singletonList(HoodieSchemaField.of("int_list", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT)))));

    String schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`int_list` ARRAY< int>", schemaString);

    // An array of arrays
    schema = HoodieSchema.createRecord("message", null, null,
        Collections.singletonList(HoodieSchemaField.of("int_list_list", HoodieSchema.createArray(HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT))))));

    schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`int_list_list` ARRAY< ARRAY< int>>", schemaString);

    // A list of structs with two fields
    schema = HoodieSchema.createRecord("ArrayOfTuples", null, null,
        Collections.singletonList(HoodieSchemaField.of("tuple_list", HoodieSchema.createArray(
            HoodieSchema.createRecord(null, null, null, Arrays.asList(HoodieSchemaField.of("str", HoodieSchema.create(HoodieSchemaType.BYTES)),
                HoodieSchemaField.of("num", HoodieSchema.create(HoodieSchemaType.INT))))))));

    schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`tuple_list` ARRAY< STRUCT< `str` : binary, `num` : int>>", schemaString);

    // A list of structs with a single field
    // For this case, since the inner group name is "array", we treat the
    // element type as a one-element struct.
    schema = HoodieSchema.createRecord("ArrayOfOneTuples", null, null,
        Collections.singletonList(HoodieSchemaField.of("one_tuple_list", HoodieSchema.createArray(
            HoodieSchema.createRecord(null, null, null,
                Collections.singletonList(HoodieSchemaField.of("str", HoodieSchema.create(HoodieSchemaType.BYTES))))))));

    schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`one_tuple_list` ARRAY< STRUCT< `str` : binary>>", schemaString);

    // A list of maps
    schema = HoodieSchema.createRecord("ArrayOfMaps", null, null,
        Collections.singletonList(HoodieSchemaField.of("map_list", HoodieSchema.createArray(
            HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.INT))))));

    schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`map_list` ARRAY< MAP< string, int>>", schemaString);
  }

  @ParameterizedTest
  @ValueSource(strings = {"TIMESTAMP_MICROS", "TIMESTAMP_MILLIS"})
  public void testSchemaConvertTimestamp(String type) throws IOException {
    HoodieSchema schema = HoodieSchema.createRecord("my_timestamp", null, null,
        Collections.singletonList(HoodieSchemaField.of("my_element", HoodieSchema.create(HoodieSchemaType.LONG))));
    String schemaString = HiveSchemaUtil.generateSchemaString(schema);
    // verify backward compatibility - int64 converted to bigint type
    assertEquals("`my_element` bigint", schemaString);
    // verify new functionality - int64 converted to timestamp type when 'supportTimestamp' is enabled
    schemaString = HiveSchemaUtil.generateSchemaString(schema, Collections.emptyList(), true);
    assertEquals("`my_element` TIMESTAMP", schemaString);
  }

  @ParameterizedTest
  @ValueSource(strings = {"TIMESTAMP_MICROS", "TIMESTAMP_MILLIS"})
  public void testSchemaDiffForTimestamp(String type) {
    HoodieSchema schema = HoodieSchema.createRecord("my_timestamp", null, null,
        Collections.singletonList(HoodieSchemaField.of("my_element", HoodieSchema.create(HoodieSchemaType.LONG))));
    // verify backward compatibility - int64 converted to bigint type
    SchemaDifference schemaDifference = HiveSchemaUtil.getSchemaDifference(schema,
        Collections.emptyMap(), Collections.emptyList(), false);
    assertEquals("bigint", schemaDifference.getAddColumnTypes().get("`my_element`"));
    schemaDifference = HiveSchemaUtil.getSchemaDifference(schema,
        schemaDifference.getAddColumnTypes(), Collections.emptyList(), false);
    assertTrue(schemaDifference.isEmpty());

    // verify schema difference is calculated correctly when supportTimestamp is enabled
    schemaDifference = HiveSchemaUtil.getSchemaDifference(schema,
        Collections.emptyMap(), Collections.emptyList(), true);
    assertEquals("TIMESTAMP", schemaDifference.getAddColumnTypes().get("`my_element`"));
    schemaDifference = HiveSchemaUtil.getSchemaDifference(schema,
        schemaDifference.getAddColumnTypes(), Collections.emptyList(), true);
    assertTrue(schemaDifference.isEmpty());
  }
}
