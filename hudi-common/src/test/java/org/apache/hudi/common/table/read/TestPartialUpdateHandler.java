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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;

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
  void testParseValidProperties() {
    TypedProperties props = new TypedProperties();
    props.setProperty(RECORD_MERGE_PROPERTY_PREFIX + "a", "1");
    props.setProperty(RECORD_MERGE_PROPERTY_PREFIX + "b", "2");
    props.setProperty(RECORD_MERGE_PROPERTY_PREFIX + "c", "3");
    Map<String, String> result = PartialUpdateHandler.parseMergeProperties(props);

    assertEquals(3, result.size());
    assertEquals("1", result.get("a"));
    assertEquals("2", result.get("b"));
    assertEquals("3", result.get("c"));
  }

  @Test
  void testHandlesWhitespace() {
    TypedProperties props = new TypedProperties();
    props.setProperty(RECORD_MERGE_PROPERTY_PREFIX + "  a  ", "  1  ");
    props.setProperty(RECORD_MERGE_PROPERTY_PREFIX + "b", "  2  ");
    props.setProperty(RECORD_MERGE_PROPERTY_PREFIX + "c", "3 ");
    Map<String, String> result = PartialUpdateHandler.parseMergeProperties(props);

    assertEquals(3, result.size());
    assertEquals("1", result.get("a")); // Keys and values are trimmed
    assertEquals("2", result.get("b"));
    assertEquals("3", result.get("c"));
  }

  @Test
  void testIgnoresEmptyEntriesAndMissingEquals() {
    TypedProperties props = new TypedProperties();
    props.setProperty(RECORD_MERGE_PROPERTY_PREFIX + "a", "1");
    props.setProperty(RECORD_MERGE_PROPERTY_PREFIX + "b", ""); // Empty value
    props.setProperty(RECORD_MERGE_PROPERTY_PREFIX + "c", "3");
    // Properties with empty keys after prefix are ignored by extractWithPrefix
    props.setProperty(RECORD_MERGE_PROPERTY_PREFIX, "ignored"); // This will be ignored
    Map<String, String> result = PartialUpdateHandler.parseMergeProperties(props);

    assertEquals(3, result.size());
    assertEquals("1", result.get("a"));
    assertEquals("", result.get("b")); // Empty value is preserved
    assertEquals("3", result.get("c"));
  }

  @Test
  void testEmptyInputReturnsEmptyMap() {
    TypedProperties props = new TypedProperties();
    // No properties with the prefix
    Map<String, String> result = PartialUpdateHandler.parseMergeProperties(props);
    assertTrue(result.isEmpty());
  }

  @Test
  void testMissingKeyReturnsEmptyMap() {
    TypedProperties props = new TypedProperties(); // no property set
    Map<String, String> result = PartialUpdateHandler.parseMergeProperties(props);
    assertTrue(result.isEmpty());
  }

  @Test
  void testEmptyProperties() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.MERGE_PROPERTIES.key(), ",a=1,,b,c=3");
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
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertTrue(PartialUpdateHandler.hasTargetType(stringSchema, HoodieSchemaType.STRING));
  }

  @Test
  void testUnionWithTargetType() {
    HoodieSchema unionSchema = HoodieSchema.createUnion(
        HoodieSchema.create(HoodieSchemaType.NULL),
        HoodieSchema.create(HoodieSchemaType.BOOLEAN),
        HoodieSchema.create(HoodieSchemaType.STRING)
    );
    assertTrue(PartialUpdateHandler.hasTargetType(unionSchema, HoodieSchemaType.STRING));
  }

  @Test
  void testUnionWithoutTargetType() {
    HoodieSchema unionSchema = HoodieSchema.createUnion(
        HoodieSchema.create(HoodieSchemaType.NULL),
        HoodieSchema.create(HoodieSchemaType.BOOLEAN),
        HoodieSchema.create(HoodieSchemaType.INT)
    );
    assertFalse(PartialUpdateHandler.hasTargetType(unionSchema, HoodieSchemaType.STRING));
  }

  @Test
  void testNonUnionNonTargetType() {
    HoodieSchema intSchema = HoodieSchema.create(HoodieSchemaType.INT);
    assertFalse(PartialUpdateHandler.hasTargetType(intSchema, HoodieSchemaType.STRING));
  }
}
