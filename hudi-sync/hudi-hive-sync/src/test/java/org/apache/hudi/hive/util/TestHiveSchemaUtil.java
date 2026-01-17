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
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
            HoodieSchema.createRecord("nested", null, null, Arrays.asList(HoodieSchemaField.of("str", HoodieSchema.create(HoodieSchemaType.BYTES)),
                HoodieSchemaField.of("num", HoodieSchema.create(HoodieSchemaType.INT))))))));

    schemaString = HiveSchemaUtil.generateSchemaString(schema);
    assertEquals("`tuple_list` ARRAY< STRUCT< `str` : binary, `num` : int>>", schemaString);

    // A list of structs with a single field
    // For this case, since the inner group name is "array", we treat the
    // element type as a one-element struct.
    schema = HoodieSchema.createRecord("ArrayOfOneTuples", null, null,
        Collections.singletonList(HoodieSchemaField.of("one_tuple_list", HoodieSchema.createArray(
            HoodieSchema.createRecord("nested", null, null,
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
  @EnumSource(value = HoodieSchema.TimePrecision.class)
  public void testSchemaConvertTimestamp(HoodieSchema.TimePrecision timePrecision) throws IOException {
    HoodieSchema schema = HoodieSchema.createRecord("my_timestamp", null, null,
        Collections.singletonList(HoodieSchemaField.of("my_element",
            timePrecision == HoodieSchema.TimePrecision.MICROS ? HoodieSchema.createTimestampMicros() : HoodieSchema.createTimestampMillis())));
    String schemaString = HiveSchemaUtil.generateSchemaString(schema);
    // verify backward compatibility - int64 converted to bigint type
    assertEquals("`my_element` bigint", schemaString);
    // verify new functionality - int64 converted to timestamp type when 'supportTimestamp' is enabled
    schemaString = HiveSchemaUtil.generateSchemaString(schema, Collections.emptyList(), true);
    assertEquals("`my_element` TIMESTAMP", schemaString);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieSchema.TimePrecision.class)
  public void testSchemaDiffForTimestamp(HoodieSchema.TimePrecision timePrecision) {
    HoodieSchema schema = HoodieSchema.createRecord("my_timestamp", null, null,
        Collections.singletonList(HoodieSchemaField.of("my_element",
            timePrecision == HoodieSchema.TimePrecision.MICROS ? HoodieSchema.createTimestampMicros() : HoodieSchema.createTimestampMillis())));
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

  @Test
  void testSchemaTypeConversion() throws IOException {
    HoodieSchema schema = HoodieSchema.createRecord("TestSchema", null, null,
        Arrays.asList(
            HoodieSchemaField.of("int_field", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("long_field", HoodieSchema.create(HoodieSchemaType.LONG)),
            HoodieSchemaField.of("float_field", HoodieSchema.create(HoodieSchemaType.FLOAT)),
            HoodieSchemaField.of("double_field", HoodieSchema.createNullable(HoodieSchemaType.DOUBLE)),
            HoodieSchemaField.of("string_field", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("enum_field", HoodieSchema.createEnum("enum", null, null,
                Arrays.asList("VALUE1", "VALUE2", "VALUE3"))),
            HoodieSchemaField.of("bytes_field", HoodieSchema.create(HoodieSchemaType.BYTES)),
            HoodieSchemaField.of("fixed_field", HoodieSchema.createFixed("fixed", null, null, 16)),
            HoodieSchemaField.of("boolean_field", HoodieSchema.createNullable(HoodieSchemaType.BOOLEAN)),
            HoodieSchemaField.of("timestamp_millis_field", HoodieSchema.createTimestampMillis()),
            HoodieSchemaField.of("timestamp_micros_field", HoodieSchema.createTimestampMicros()),
            HoodieSchemaField.of("timestamp_ntz_millis_field", HoodieSchema.createTimestampMillis()),
            HoodieSchemaField.of("timestamp_ntz_micros_field", HoodieSchema.createTimestampMicros()),
            HoodieSchemaField.of("date_field", HoodieSchema.createDate()),
            HoodieSchemaField.of("time_millis_field", HoodieSchema.createTimeMillis()),
            HoodieSchemaField.of("time_micros_field", HoodieSchema.createTimeMicros()),
            HoodieSchemaField.of("decimal_field", HoodieSchema.createDecimal(10, 2)),
            HoodieSchemaField.of("uuid_field", HoodieSchema.create(HoodieSchemaType.UUID))
        )
    );

    Map<String, String> actual = HiveSchemaUtil.convertSchemaToHiveSchema(schema, true);
    Map<String, String> expected = new HashMap<>();
    expected.put("`int_field`", "int");
    expected.put("`long_field`", "bigint");
    expected.put("`float_field`", "float");
    expected.put("`double_field`", "double");
    expected.put("`string_field`", "string");
    expected.put("`enum_field`", "string");
    expected.put("`bytes_field`", "binary");
    expected.put("`fixed_field`", "binary");
    expected.put("`boolean_field`", "boolean");
    expected.put("`timestamp_millis_field`", "TIMESTAMP");
    expected.put("`timestamp_micros_field`", "TIMESTAMP");
    expected.put("`timestamp_ntz_millis_field`", "TIMESTAMP");
    expected.put("`timestamp_ntz_micros_field`", "TIMESTAMP");
    expected.put("`date_field`", "DATE");
    expected.put("`time_millis_field`", "int");
    expected.put("`time_micros_field`", "bigint");
    expected.put("`decimal_field`", "DECIMAL(10 , 2)");
    expected.put("`uuid_field`", "binary");
    assertEquals(expected, actual);
  }
}
