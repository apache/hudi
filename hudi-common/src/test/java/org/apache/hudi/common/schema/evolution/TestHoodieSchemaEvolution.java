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
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for {@link HoodieSchemaEvolutionUtils} (write-path reconcile)
 * and {@link HoodieSchemaMerger} (read-path file/query merge), pinning the
 * load-bearing semantics:
 *
 * <ul>
 *   <li>Adding a column on the write path mints a fresh id and preserves existing ones.</li>
 *   <li>Renamed columns surface in the merger's rename map and the result honors
 *       {@code useColNameFromFileSchema}.</li>
 *   <li>Field ids cross every façade boundary intact.</li>
 * </ul>
 */
public class TestHoodieSchemaEvolution {

  private static HoodieSchema record(HoodieSchemaField... fields) {
    HoodieSchema schema = HoodieSchema.createRecord(
        "Record", null, "ns", false, Arrays.asList(fields));
    HoodieSchemaIdAssigner.assign(schema, 0);
    schema.invalidateIdIndex();
    return schema;
  }

  private static HoodieSchemaField intField(String name) {
    return HoodieSchemaField.of(name, HoodieSchema.create(HoodieSchemaType.INT));
  }

  private static HoodieSchemaField nullableIntField(String name) {
    return HoodieSchemaField.of(name, HoodieSchema.createNullable(HoodieSchemaType.INT), null, HoodieSchema.NULL_VALUE);
  }

  private static HoodieSchemaField stringField(String name) {
    return HoodieSchemaField.of(name, HoodieSchema.create(HoodieSchemaType.STRING));
  }

  // ---- HoodieSchemaEvolutionUtils.reconcileSchema ----

  @Test
  public void reconcileSchemaAddsNewIncomingColumn() {
    HoodieSchema oldTable = record(intField("a"), intField("b"));
    HoodieSchema incoming = record(intField("a"), intField("b"), intField("c"));

    HoodieSchema result = HoodieSchemaEvolutionUtils.reconcileSchema(incoming, oldTable, false);

    assertTrue(result.getField("c").isPresent());
    // Pre-existing fields keep their ids.
    assertEquals(0, result.getField("a").get().fieldId());
    assertEquals(1, result.getField("b").get().fieldId());
    // The new field gets an id strictly greater than the original max.
    assertTrue(result.getField("c").get().fieldId() > 1);
  }

  @Test
  public void reconcileSchemaShortCircuitsWhenNoChange() {
    HoodieSchema oldTable = record(intField("a"), intField("b"));
    HoodieSchema incoming = record(intField("a"), intField("b"));

    HoodieSchema result = HoodieSchemaEvolutionUtils.reconcileSchema(incoming, oldTable, false);

    assertEquals(0, result.getField("a").get().fieldId());
    assertEquals(1, result.getField("b").get().fieldId());
    assertFalse(result.getField("c").isPresent());
  }

  @Test
  public void reconcileSchemaMakesMissingFieldsNullable() {
    // oldTable has c (required), incoming doesn't carry c. With the flag on, c
    // becomes nullable in the result.
    HoodieSchema oldTable = record(intField("a"), intField("b"), intField("c"));
    HoodieSchema incoming = record(intField("a"), intField("b"));

    HoodieSchema result = HoodieSchemaEvolutionUtils.reconcileSchema(incoming, oldTable, true);

    assertTrue(result.getField("c").isPresent());
    assertTrue(result.getField("c").get().isNullable());
    // Surviving id stable.
    assertEquals(2, result.getField("c").get().fieldId());
  }

  // ---- HoodieSchemaMerger ----

  @Test
  public void mergerReturnsQueryShapeWhenSchemasMatch() {
    HoodieSchema fileSchema = record(intField("a"), intField("b"));
    HoodieSchema querySchema = record(intField("a"), intField("b"));

    HoodieSchema merged = new HoodieSchemaMerger(fileSchema, querySchema, false, true).mergeSchema();

    assertTrue(merged.getField("a").isPresent());
    assertTrue(merged.getField("b").isPresent());
    assertEquals(0, merged.getField("a").get().fieldId());
    assertEquals(1, merged.getField("b").get().fieldId());
  }

  @Test
  public void mergerSurfacesRenameUsingFileSchemaName() {
    // Query schema renames "b" to "renamed_b" but keeps id=1. File schema still
    // calls it "b". With useColNameFromFileSchema=true (default), the merged
    // schema reports the file's name for that field.
    HoodieSchema fileSchema = record(intField("a"), intField("b"));
    HoodieSchema querySchema = record(intField("a"), intField("renamed_b"));
    // Force same id for "b" / "renamed_b" so the merger detects a rename.
    querySchema.getField("renamed_b").get().getAvroField().addProp(HoodieSchema.FIELD_ID_PROP, 1);
    querySchema.invalidateIdIndex();

    HoodieSchema merged = new HoodieSchemaMerger(fileSchema, querySchema, false, true, true).mergeSchema();

    // useColNameFromFileSchema=true: merged uses file name.
    assertTrue(merged.getField("b").isPresent());
    assertFalse(merged.getField("renamed_b").isPresent());
    assertEquals(1, merged.getField("b").get().fieldId());
  }

  @Test
  public void mergerReturnsRenameMapWhenUsingQuerySchemaName() {
    HoodieSchema fileSchema = record(intField("a"), intField("b"));
    HoodieSchema querySchema = record(intField("a"), intField("renamed_b"));
    querySchema.getField("renamed_b").get().getAvroField().addProp(HoodieSchema.FIELD_ID_PROP, 1);
    querySchema.invalidateIdIndex();

    // useColNameFromFileSchema=false: merged uses query name and reports the
    // rename in the map. Downstream record rewriters use this to translate.
    Pair<HoodieSchema, Map<String, String>> result =
        new HoodieSchemaMerger(fileSchema, querySchema, false, true, false).mergeSchemaGetRenamed();

    HoodieSchema merged = result.getLeft();
    Map<String, String> renames = result.getRight();
    assertTrue(merged.getField("renamed_b").isPresent());
    assertFalse(renames.isEmpty());
    assertEquals("b", renames.get("renamed_b"));
  }
}
