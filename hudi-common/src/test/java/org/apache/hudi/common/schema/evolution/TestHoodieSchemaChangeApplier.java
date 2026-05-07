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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates {@link HoodieSchemaChangeApplier} against the seven user-facing
 * column-evolution operations.
 *
 * <p>The most important invariant is <b>id stability</b>: a field that had id N in
 * the input schema must still have id N in the output schema, regardless of which
 * operation was applied. This is what makes column-rename-without-data-rewrite
 * possible.</p>
 */
public class TestHoodieSchemaChangeApplier {

  private static HoodieSchema baseSchema() {
    HoodieSchema schema = HoodieSchema.createRecord(
        "Record", null, "ns", false,
        Arrays.asList(
            HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("b", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("c", HoodieSchema.create(HoodieSchemaType.LONG))));
    HoodieSchemaIdAssigner.assign(schema, 0);
    schema.invalidateIdIndex();
    return schema;
  }

  @Test
  public void addsColumnAtEndAndAssignsNewId() {
    HoodieSchema schema = baseSchema();
    HoodieSchemaChangeApplier applier = new HoodieSchemaChangeApplier(schema);

    HoodieSchema result = applier.applyAddChange(
        "d", HoodieSchema.create(HoodieSchemaType.STRING), null, "", ColumnPositionType.NO_OPERATION);

    // Existing ids preserved.
    assertEquals(0, result.getField("a").get().fieldId());
    assertEquals(1, result.getField("b").get().fieldId());
    assertEquals(2, result.getField("c").get().fieldId());
    // New column gets the next free id.
    assertEquals(3, result.getField("d").get().fieldId());
    assertEquals(3, result.maxColumnId());
  }

  @Test
  public void deletesColumnAndKeepsRemainingIdsStable() {
    HoodieSchema schema = baseSchema();
    HoodieSchemaChangeApplier applier = new HoodieSchemaChangeApplier(schema);

    HoodieSchema result = applier.applyDeleteChange("b");

    assertFalse(result.getField("b").isPresent());
    // Surviving fields keep their original ids — this is the whole point of column-id-based evolution.
    assertEquals(0, result.getField("a").get().fieldId());
    assertEquals(2, result.getField("c").get().fieldId());
  }

  @Test
  public void renamesColumnPreservingId() {
    HoodieSchema schema = baseSchema();
    HoodieSchemaChangeApplier applier = new HoodieSchemaChangeApplier(schema);

    HoodieSchema result = applier.applyRenameChange("b", "renamed_b");

    assertFalse(result.getField("b").isPresent());
    assertTrue(result.getField("renamed_b").isPresent());
    // Same id as the old "b" — this is the load-bearing invariant for schema-on-read evolution.
    assertEquals(1, result.getField("renamed_b").get().fieldId());
    assertEquals(0, result.getField("a").get().fieldId());
    assertEquals(2, result.getField("c").get().fieldId());
  }

  @Test
  public void promotesIntToLongPreservingId() {
    HoodieSchema schema = baseSchema();
    HoodieSchemaChangeApplier applier = new HoodieSchemaChangeApplier(schema);

    HoodieSchema result = applier.applyColumnTypeChange("a", HoodieSchema.create(HoodieSchemaType.LONG));

    assertEquals(HoodieSchemaType.LONG, result.getField("a").get().schema().getType());
    assertEquals(0, result.getField("a").get().fieldId());
  }

  @Test
  public void changesNullabilityFromRequiredToOptional() {
    HoodieSchema schema = baseSchema();
    HoodieSchemaChangeApplier applier = new HoodieSchemaChangeApplier(schema);

    HoodieSchema result = applier.applyColumnNullabilityChange("a", true);

    assertTrue(result.getField("a").get().isNullable());
    assertEquals(0, result.getField("a").get().fieldId());
  }

  @Test
  public void updatesColumnComment() {
    HoodieSchema schema = baseSchema();
    HoodieSchemaChangeApplier applier = new HoodieSchemaChangeApplier(schema);

    HoodieSchema result = applier.applyColumnCommentChange("a", "the answer column");

    assertEquals("the answer column", result.getField("a").get().doc().orElse(null));
    assertEquals(0, result.getField("a").get().fieldId());
  }

  @Test
  public void chainedOperationsKeepIdsStable() {
    // Add → rename → delete on the same starting schema. After all three operations,
    // the surviving original fields must still have their original ids and the new
    // field must have an id strictly greater than the original maxColumnId.
    HoodieSchema schema = baseSchema();
    int originalMax = schema.maxColumnId();

    HoodieSchema afterAdd = new HoodieSchemaChangeApplier(schema).applyAddChange(
        "d", HoodieSchema.create(HoodieSchemaType.DOUBLE), null, "", ColumnPositionType.NO_OPERATION);
    HoodieSchema afterRename = new HoodieSchemaChangeApplier(afterAdd).applyRenameChange("b", "bb");
    HoodieSchema afterDelete = new HoodieSchemaChangeApplier(afterRename).applyDeleteChange("c");

    assertEquals(0, afterDelete.getField("a").get().fieldId());
    assertEquals(1, afterDelete.getField("bb").get().fieldId());
    assertFalse(afterDelete.getField("c").isPresent());
    assertTrue(afterDelete.getField("d").get().fieldId() > originalMax);
  }

  @Test
  public void resultSchemaCanCarryANewVersionId() {
    // The applier produces a structurally-correct new schema. Stamping a new schema
    // version id on it (typically derived from the commit instant) is the caller's
    // responsibility — the applier never sets schemaId itself.
    HoodieSchema schema = baseSchema();
    schema.setSchemaId(99L);
    HoodieSchema result = new HoodieSchemaChangeApplier(schema).applyDeleteChange("c");

    result.setSchemaId(100L);
    assertEquals(100L, result.schemaId());
    // Ids on surviving fields are preserved regardless of schema version stamping.
    assertEquals(0, result.getField("a").get().fieldId());
    assertEquals(1, result.getField("b").get().fieldId());
  }
}
