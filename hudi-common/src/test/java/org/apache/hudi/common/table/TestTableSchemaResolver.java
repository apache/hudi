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

package org.apache.hudi.common.table;

import org.apache.avro.Schema;

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;

import org.apache.hudi.exception.HoodieIncompatibleSchemaException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTableSchemaResolver {

  @Test
  public void testRecreateSchemaWhenDropPartitionColumns() {
    Schema originSchema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);

    // case1
    Option<String[]> emptyPartitionFieldsOpt = Option.empty();
    Schema s1 = TableSchemaResolver.recreateSchemaWhenDropPartitionColumns(emptyPartitionFieldsOpt, originSchema);
    assertEquals(originSchema, s1);

    // case2
    String[] pts1 = new String[0];
    Schema s2 = TableSchemaResolver.recreateSchemaWhenDropPartitionColumns(Option.of(pts1), originSchema);
    assertEquals(originSchema, s2);

    // case3: partition_path is in originSchema
    String[] pts2 = {"partition_path"};
    Schema s3 = TableSchemaResolver.recreateSchemaWhenDropPartitionColumns(Option.of(pts2), originSchema);
    assertEquals(originSchema, s3);

    // case4: user_partition is not in originSchema
    String[] pts3 = {"user_partition"};
    Schema s4 = TableSchemaResolver.recreateSchemaWhenDropPartitionColumns(Option.of(pts3), originSchema);
    assertNotEquals(originSchema, s4);
    assertTrue(s4.getFields().stream().anyMatch(f -> f.name().equals("user_partition")));
    Schema.Field f = s4.getField("user_partition");
    assertEquals(f.schema(), AvroSchemaUtils.createNullableSchema(Schema.Type.STRING));

    // case5: user_partition is in originSchema, but partition_path is in originSchema
    String[] pts4 = {"user_partition", "partition_path"};
    try {
      TableSchemaResolver.recreateSchemaWhenDropPartitionColumns(Option.of(pts3), originSchema);
    } catch (HoodieIncompatibleSchemaException e) {
      assertTrue(e.getMessage().contains("Partial partition fields are still in the schema"));
    }
  }
}
