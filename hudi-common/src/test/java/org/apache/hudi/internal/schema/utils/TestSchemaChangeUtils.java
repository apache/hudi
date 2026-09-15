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

package org.apache.hudi.internal.schema.utils;

import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link SchemaChangeUtils#parseTimestampLogicalTypeOverrides(String)}. Validation must
 * happen at parse time — the config is threaded through the writer schema deduction path and a
 * malformed value would otherwise surface deep inside deduceWriterSchema on the first commit.
 */
public class TestSchemaChangeUtils {

  @Test
  public void parseEmptyValueYieldsEmptyMap() {
    assertTrue(SchemaChangeUtils.parseTimestampLogicalTypeOverrides(null).isEmpty());
    assertTrue(SchemaChangeUtils.parseTimestampLogicalTypeOverrides("").isEmpty());
    assertTrue(SchemaChangeUtils.parseTimestampLogicalTypeOverrides("   ").isEmpty());
  }

  @Test
  public void parseValidSingleEntry() {
    Map<String, Type> overrides = SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:timestamp-millis");
    assertEquals(1, overrides.size());
    assertEquals(Types.TimestampMillisType.get(), overrides.get("ts"));
  }

  @Test
  public void parseAllFourTokens() {
    Map<String, Type> overrides = SchemaChangeUtils.parseTimestampLogicalTypeOverrides(
        "a:timestamp-micros,b:timestamp-millis,c:local-timestamp-micros,d:local-timestamp-millis");
    assertEquals(4, overrides.size());
    assertEquals(Types.TimestampType.get(), overrides.get("a"));
    assertEquals(Types.TimestampMillisType.get(), overrides.get("b"));
    assertEquals(Types.LocalTimestampMicrosType.get(), overrides.get("c"));
    assertEquals(Types.LocalTimestampMillisType.get(), overrides.get("d"));
  }

  @Test
  public void parseIsCaseInsensitive() {
    // Tokens are case-insensitive so an operator pasting "Timestamp-Millis" is not surprised.
    assertEquals(Types.TimestampMillisType.get(),
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:TIMESTAMP-MILLIS").get("ts"));
    assertEquals(Types.LocalTimestampMicrosType.get(),
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:Local-Timestamp-Micros").get("ts"));
  }

  @Test
  public void parseSupportsDottedNestedFieldNames() {
    // Nested field names use '.'; the parser splits on the LAST ':' so this works unchanged.
    Map<String, Type> overrides =
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("payload.event_time:timestamp-millis");
    assertEquals(1, overrides.size());
    assertEquals(Types.TimestampMillisType.get(), overrides.get("payload.event_time"));
  }

  @Test
  public void parseTrimmedWhitespaceAndSkipEmptySegments() {
    Map<String, Type> overrides = SchemaChangeUtils.parseTimestampLogicalTypeOverrides(
        "  a:timestamp-micros ,, b : timestamp-millis  ,");
    assertEquals(2, overrides.size());
    assertEquals(Types.TimestampType.get(), overrides.get("a"));
    assertEquals(Types.TimestampMillisType.get(), overrides.get("b"));
  }

  @Test
  public void parseRejectsMissingColon() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> SchemaChangeUtils.parseTimestampLogicalTypeOverrides("field_only"));
    assertTrue(ex.getMessage().contains("field_only"), "message should include the offending entry");
  }

  @Test
  public void parseRejectsMissingType() {
    assertThrows(IllegalArgumentException.class,
        () -> SchemaChangeUtils.parseTimestampLogicalTypeOverrides("field:"));
  }

  @Test
  public void parseRejectsMissingField() {
    assertThrows(IllegalArgumentException.class,
        () -> SchemaChangeUtils.parseTimestampLogicalTypeOverrides(":timestamp-micros"));
  }

  @Test
  public void parseRejectsUnknownToken() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> SchemaChangeUtils.parseTimestampLogicalTypeOverrides("field:not-a-real-type"));
    assertTrue(ex.getMessage().contains("not-a-real-type"), "message should include the bad token");
  }

  @Test
  public void parseResultIsUnmodifiable() {
    Map<String, Type> overrides = SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:timestamp-micros");
    assertThrows(UnsupportedOperationException.class,
        () -> overrides.put("other", Types.TimestampMillisType.get()));
  }

  @Test
  void gatedTimestampChangeCoversFlipsAndLongPromotions() {
    // Precision flips (either direction) are gated.
    assertTrue(SchemaChangeUtils.isGatedTimestampChange(Types.TimestampType.get(), Types.TimestampMillisType.get()));
    assertTrue(SchemaChangeUtils.isGatedTimestampChange(Types.LocalTimestampMicrosType.get(), Types.LocalTimestampMillisType.get()));
    // Promoting a bare long to any timestamp logical type (UTC or local) is gated.
    assertTrue(SchemaChangeUtils.isGatedTimestampChange(Types.LongType.get(), Types.TimestampType.get()));
    assertTrue(SchemaChangeUtils.isGatedTimestampChange(Types.LongType.get(), Types.TimestampMillisType.get()));
    assertTrue(SchemaChangeUtils.isGatedTimestampChange(Types.LongType.get(), Types.LocalTimestampMillisType.get()));
    assertTrue(SchemaChangeUtils.isGatedTimestampChange(Types.LongType.get(), Types.LocalTimestampMicrosType.get()));
    // Unrelated promotions and identical types are not gated.
    assertFalse(SchemaChangeUtils.isGatedTimestampChange(Types.LongType.get(), Types.StringType.get()));
    assertFalse(SchemaChangeUtils.isGatedTimestampChange(Types.IntType.get(), Types.TimestampType.get()));
    assertFalse(SchemaChangeUtils.isGatedTimestampChange(Types.TimestampType.get(), Types.TimestampType.get()));
  }

  @Test
  void typeUpdateAllowGatesLongToTimestampBehindTheOverride() {
    // Promoting a bare long to any timestamp logical type is allowed only when the gate is open.
    for (Type ts : new Type[] {Types.TimestampType.get(), Types.TimestampMillisType.get(),
        Types.LocalTimestampMillisType.get(), Types.LocalTimestampMicrosType.get()}) {
      assertTrue(SchemaChangeUtils.isTypeUpdateAllow(Types.LongType.get(), ts, true));
      assertFalse(SchemaChangeUtils.isTypeUpdateAllow(Types.LongType.get(), ts, false));
    }
    // Existing long widening is unaffected by the gate.
    assertTrue(SchemaChangeUtils.isTypeUpdateAllow(Types.LongType.get(), Types.DoubleType.get(), false));
  }
}
