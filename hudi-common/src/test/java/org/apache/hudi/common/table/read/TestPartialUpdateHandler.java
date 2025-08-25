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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.TypedProperties;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.hudi.common.table.HoodieTableConfig.DEBEZIUM_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPartialUpdateHandler {
  @Test
  void testEmptyProperties() {
    TypedProperties props = new TypedProperties();
    Map<String, String> result = PartialUpdateHandler.parseMergeProperties(props);
    assertTrue(result.isEmpty());
  }

  @Test
  void testNonEmptyProperties() {
    TypedProperties props = new TypedProperties();
    props.put(RECORD_MERGE_PROPERTY_PREFIX + PARTIAL_UPDATE_UNAVAILABLE_VALUE, DEBEZIUM_UNAVAILABLE_VALUE);
    Map<String, String> result = PartialUpdateHandler.parseMergeProperties(props);
    assertTrue(result.containsKey(PARTIAL_UPDATE_UNAVAILABLE_VALUE));
    assertEquals(DEBEZIUM_UNAVAILABLE_VALUE, result.get(PARTIAL_UPDATE_UNAVAILABLE_VALUE));
  }

  @Test
  void testDirectMatch() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    assertTrue(PartialUpdateHandler.hasTargetType(stringSchema, Schema.Type.STRING));
  }

  @Test
  void testUnionWithTargetType() {
    Schema unionSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.STRING)
    );
    assertTrue(PartialUpdateHandler.hasTargetType(unionSchema, Schema.Type.STRING));
  }

  @Test
  void testUnionWithoutTargetType() {
    Schema unionSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.INT)
    );
    assertFalse(PartialUpdateHandler.hasTargetType(unionSchema, Schema.Type.STRING));
  }

  @Test
  void testNonUnionNonTargetType() {
    Schema intSchema = Schema.create(Schema.Type.INT);
    assertFalse(PartialUpdateHandler.hasTargetType(intSchema, Schema.Type.STRING));
  }
}
