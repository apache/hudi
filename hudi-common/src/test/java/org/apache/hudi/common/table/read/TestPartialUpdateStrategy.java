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
import org.apache.hudi.common.table.HoodieTableConfig;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPartialUpdateStrategy {
  @Test
  void testParseValidProperties() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.MERGE_PROPERTIES.key(), "a=1,b=2,c=3");
    Map<String, String> result = PartialUpdateStrategy.parseMergeProperties(props);

    assertEquals(3, result.size());
    assertEquals("1", result.get("a"));
    assertEquals("2", result.get("b"));
    assertEquals("3", result.get("c"));
  }

  @Test
  void testHandlesWhitespace() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.MERGE_PROPERTIES.key(), " a = 1 , b=  2 ,c=3 ");
    Map<String, String> result = PartialUpdateStrategy.parseMergeProperties(props);

    assertEquals(3, result.size());
    assertEquals("1", result.get("a"));
    assertEquals("2", result.get("b"));
    assertEquals("3", result.get("c"));
  }

  @Test
  void testIgnoresEmptyEntriesAndMissingEquals() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.MERGE_PROPERTIES.key(), ",a=1,,b,c=3");
    Map<String, String> result = PartialUpdateStrategy.parseMergeProperties(props);

    assertEquals(3, result.size());
    assertEquals("1", result.get("a"));
    assertEquals("3", result.get("c"));
    assertEquals("", result.get("b"));
  }

  @Test
  void testEmptyInputReturnsEmptyMap() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.MERGE_PROPERTIES.key(), "");
    Map<String, String> result = PartialUpdateStrategy.parseMergeProperties(props);
    assertTrue(result.isEmpty());
  }

  @Test
  void testMissingKeyReturnsEmptyMap() {
    TypedProperties props = new TypedProperties(); // no property set
    Map<String, String> result = PartialUpdateStrategy.parseMergeProperties(props);
    assertTrue(result.isEmpty());
  }

  @Test
  void testDirectMatch() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    assertTrue(PartialUpdateStrategy.hasTargetType(stringSchema, Schema.Type.STRING));
  }

  @Test
  void testUnionWithTargetType() {
    Schema unionSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.STRING)
    );
    assertTrue(PartialUpdateStrategy.hasTargetType(unionSchema, Schema.Type.STRING));
  }

  @Test
  void testUnionWithoutTargetType() {
    Schema unionSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.INT)
    );
    assertFalse(PartialUpdateStrategy.hasTargetType(unionSchema, Schema.Type.STRING));
  }

  @Test
  void testNonUnionNonTargetType() {
    Schema intSchema = Schema.create(Schema.Type.INT);
    assertFalse(PartialUpdateStrategy.hasTargetType(intSchema, Schema.Type.STRING));
  }
}
