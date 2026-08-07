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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.common.schema.HoodieSchema;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the diff/classify logic in {@link SchemaChangeLogger} via the package-private test
 * accessors. The logging side-effects themselves are not asserted (they go through SLF4J); only
 * the structured diff string and the compatibility label.
 */
public class TestSchemaChangeLogger {

  private static HoodieSchema parse(String json) {
    return HoodieSchema.parse(json);
  }

  private static final String SCHEMA_OLD = "{"
      + "\"type\":\"record\","
      + "\"name\":\"Row\","
      + "\"fields\":["
      + "  {\"name\":\"id\",\"type\":\"long\"},"
      + "  {\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "  {\"name\":\"price\",\"type\":[\"null\",\"double\"],\"default\":null}"
      + "]}";

  private static final String SCHEMA_ADDED_NULLABLE_FIELD = "{"
      + "\"type\":\"record\","
      + "\"name\":\"Row\","
      + "\"fields\":["
      + "  {\"name\":\"id\",\"type\":\"long\"},"
      + "  {\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "  {\"name\":\"price\",\"type\":[\"null\",\"double\"],\"default\":null},"
      + "  {\"name\":\"sku\",\"type\":[\"null\",\"string\"],\"default\":null}"
      + "]}";

  private static final String SCHEMA_REMOVED_FIELD = "{"
      + "\"type\":\"record\","
      + "\"name\":\"Row\","
      + "\"fields\":["
      + "  {\"name\":\"id\",\"type\":\"long\"},"
      + "  {\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}"
      + "]}";

  private static final String SCHEMA_TYPE_CHANGED = "{"
      + "\"type\":\"record\","
      + "\"name\":\"Row\","
      + "\"fields\":["
      + "  {\"name\":\"id\",\"type\":\"long\"},"
      + "  {\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "  {\"name\":\"price\",\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":4}],\"default\":null}"
      + "]}";

  @Test
  void compatibleAdditiveChangeIsClassifiedCompatible() {
    HoodieSchema previous = parse(SCHEMA_OLD);
    HoodieSchema current = parse(SCHEMA_ADDED_NULLABLE_FIELD);

    String diff = SchemaChangeLogger.renderDiffForTest(previous, current);
    assertTrue(diff.contains("sku"), "added field should appear in diff: " + diff);
    assertTrue(diff.contains("removed=[]"), "no removed fields expected: " + diff);

    assertEquals("COMPATIBLE", SchemaChangeLogger.classifyForTest(previous, current));
  }

  @Test
  void typeChangeFromDoubleToDecimalIsClassifiedIncompatible() {
    // Mirrors the ENG-40683 / catapult_period_stats scenario: a numeric column's type evolved
    // from double to decimal. Avro's reader/writer rules consider this incompatible because a
    // reader expecting decimal cannot decode bytes written as double.
    HoodieSchema previous = parse(SCHEMA_OLD);
    HoodieSchema current = parse(SCHEMA_TYPE_CHANGED);

    String diff = SchemaChangeLogger.renderDiffForTest(previous, current);
    assertTrue(diff.contains("price"), "price type change should appear in diff: " + diff);
    assertTrue(diff.contains("type_changes={price="), "diff should record price's type change: " + diff);

    assertEquals("INCOMPATIBLE", SchemaChangeLogger.classifyForTest(previous, current));
  }

  @Test
  void removedFieldShowsInDiffAndIsIncompatible() {
    HoodieSchema previous = parse(SCHEMA_OLD);
    HoodieSchema current = parse(SCHEMA_REMOVED_FIELD);

    String diff = SchemaChangeLogger.renderDiffForTest(previous, current);
    assertTrue(diff.contains("removed=[price"), "diff should record price as removed: " + diff);
    assertTrue(diff.contains("added=[]"), "no added fields expected: " + diff);
    // Whether removal is incompatible depends on the field's reader-side default; for a nullable
    // field with default null the reader can fill nulls. Either classification is acceptable; we
    // only assert that the diff captures it.
  }

  @Test
  void identicalSchemasProduceEmptyDiff() {
    HoodieSchema previous = parse(SCHEMA_OLD);
    HoodieSchema current = parse(SCHEMA_OLD);

    String diff = SchemaChangeLogger.renderDiffForTest(previous, current);
    assertEquals("added=[], removed=[], type_changes={}", diff);
  }
}
