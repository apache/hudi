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

package org.apache.hudi.common.schema.evolution;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaIdAssigner;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 3 façade tests. The on-disk JSON format is a hard backward-compatibility
 * boundary — old tables must remain readable, and tables written by the new code
 * must be readable by the legacy SerDe. These tests pin both directions:
 *
 * <ul>
 *   <li>Round-trip through {@link HoodieSchemaSerDe} preserves field ids and structure.</li>
 *   <li>JSON produced by the new façade parses cleanly via the legacy {@link SerDeHelper}.</li>
 *   <li>JSON produced by the legacy SerDeHelper parses cleanly via the new façade.</li>
 *   <li>The {@code LATEST_SCHEMA} / {@code SCHEMAS} constants alias the legacy keys exactly.</li>
 * </ul>
 */
public class TestHoodieSchemaSerDe {

  private static HoodieSchema sampleSchema(long schemaId) {
    HoodieSchema schema = HoodieSchema.createRecord(
        "Record", null, "ns", false,
        Arrays.asList(
            HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("b", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("c", HoodieSchema.create(HoodieSchemaType.LONG))));
    HoodieSchemaIdAssigner.assign(schema, 0);
    schema.setSchemaId(schemaId);
    schema.invalidateIdIndex();
    return schema;
  }

  @Test
  public void constantsAliasLegacyKeys() {
    // Commit-metadata payloads written by the legacy code use these exact keys —
    // any drift breaks backward compatibility.
    assertEquals(SerDeHelper.LATEST_SCHEMA, HoodieSchemaSerDe.LATEST_SCHEMA);
    assertEquals(SerDeHelper.SCHEMAS, HoodieSchemaSerDe.SCHEMAS);
  }

  @Test
  public void roundTripsSingleSchemaPreservingIds() {
    HoodieSchema schema = sampleSchema(7L);
    String json = HoodieSchemaSerDe.toJson(schema);

    Option<HoodieSchema> parsed = HoodieSchemaSerDe.fromJson(json);
    assertTrue(parsed.isPresent());
    HoodieSchema reparsed = parsed.get();

    assertEquals(0, reparsed.getField("a").get().fieldId());
    assertEquals(1, reparsed.getField("b").get().fieldId());
    assertEquals(2, reparsed.getField("c").get().fieldId());
    assertEquals(7L, reparsed.schemaId());
  }

  @Test
  public void newFacadeJsonIsReadableByLegacySerDe() {
    HoodieSchema schema = sampleSchema(11L);
    String json = HoodieSchemaSerDe.toJson(schema);

    // The legacy parser must accept the bytes — any byte-format drift here would
    // mean tables written by the new code are unreadable to old readers.
    Option<InternalSchema> legacyParsed = SerDeHelper.fromJson(json);
    assertTrue(legacyParsed.isPresent());
    assertEquals(11L, legacyParsed.get().schemaId());
    // Field id of "a" survives the new-write -> legacy-read path.
    assertNotNull(legacyParsed.get().findField("a"));
    assertEquals(0, legacyParsed.get().findField("a").fieldId());
  }

  @Test
  public void legacySerDeJsonIsReadableByNewFacade() {
    // Reverse direction: a blob the legacy code wrote must still parse through
    // the new façade. This is the path tables-on-disk-from-old-Hudi take after
    // upgrade.
    HoodieSchema schema = sampleSchema(13L);
    InternalSchema legacy = HoodieSchemaInternalSchemaBridge.toInternalSchema(schema);
    String legacyJson = SerDeHelper.toJson(legacy);

    Option<HoodieSchema> parsed = HoodieSchemaSerDe.fromJson(legacyJson);
    assertTrue(parsed.isPresent());
    assertEquals(13L, parsed.get().schemaId());
    assertEquals(0, parsed.get().getField("a").get().fieldId());
    assertEquals(2, parsed.get().getField("c").get().fieldId());
  }

  @Test
  public void parseHistorySchemasReturnsMapByVersionId() {
    HoodieSchema v1 = sampleSchema(100L);
    HoodieSchema v2 = sampleSchema(200L);

    String historyJson = HoodieSchemaSerDe.toJsonHistory(Arrays.asList(v1, v2));
    TreeMap<Long, HoodieSchema> parsed = HoodieSchemaSerDe.parseHistorySchemas(historyJson);

    assertEquals(2, parsed.size());
    assertTrue(parsed.containsKey(100L));
    assertTrue(parsed.containsKey(200L));
    // Each entry's field ids are preserved.
    assertEquals(0, parsed.get(100L).getField("a").get().fieldId());
    assertEquals(2, parsed.get(200L).getField("c").get().fieldId());
  }

  @Test
  public void inheritHistoryAppendsNewSchemaToExistingBlob() {
    HoodieSchema v1 = sampleSchema(100L);
    String firstHistory = HoodieSchemaSerDe.toJsonHistory(Arrays.asList(v1));

    HoodieSchema v2 = sampleSchema(200L);
    String mergedHistory = HoodieSchemaSerDe.inheritHistory(v2, firstHistory);

    TreeMap<Long, HoodieSchema> parsed = HoodieSchemaSerDe.parseHistorySchemas(mergedHistory);
    assertEquals(2, parsed.size());
    assertTrue(parsed.containsKey(100L));
    assertTrue(parsed.containsKey(200L));
  }

  @Test
  public void emptyOrNullJsonReturnsEmptyOption() {
    assertFalse(HoodieSchemaSerDe.fromJson(null).isPresent());
    assertFalse(HoodieSchemaSerDe.fromJson("").isPresent());
  }
}
