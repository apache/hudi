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

package org.apache.hudi.common.schema.evolution.legacy.action;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.evolution.HoodieSchemaInternalSchemaBridge;
import org.apache.hudi.common.schema.evolution.HoodieSchemaMerger;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.schema.evolution.legacy.InternalSchema;
import org.apache.hudi.common.schema.types.Types;
import org.apache.hudi.common.schema.evolution.legacy.utils.SchemaChangeUtils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HoodieSchemaMerger} via the legacy DDL action algebra
 * (TableChanges + SchemaChangeUtils) — this exercises the full evolution
 * pipeline, with the merger as the final assertion.
 */
public class TestMergeSchema {

  @Test
  public void testPrimitiveMerge() {
    Types.RecordType record = Types.RecordType.get(Arrays.asList(
        Types.Field.get(0, "col1", Types.BooleanType.get()),
        Types.Field.get(1, "col2", Types.IntType.get()),
        Types.Field.get(2, "col3", Types.LongType.get()),
        Types.Field.get(3, "col4", Types.FloatType.get())));

    InternalSchema oldSchema = new InternalSchema(record);
    // add c1 after 'col1', and c2 before 'col3'
    TableChanges.ColumnAddChange addChange = TableChanges.ColumnAddChange.get(oldSchema);
    addChange.addColumns("c1", Types.BooleanType.get(), "add c1 after col1");
    addChange.addPositionChange("c1", "col1", "after");
    addChange.addColumns("c2", Types.IntType.get(), "add c2 before col3");
    addChange.addPositionChange("c2", "col3", "before");
    InternalSchema newAddSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, addChange);
    TableChanges.ColumnDeleteChange deleteChange = TableChanges.ColumnDeleteChange.get(newAddSchema);
    deleteChange.deleteColumn("col1");
    deleteChange.deleteColumn("col3");
    InternalSchema newDeleteSchema = SchemaChangeUtils.applyTableChanges2Schema(newAddSchema, deleteChange);
    assertEquals(newAddSchema.getMaxColumnId(), newDeleteSchema.getMaxColumnId());

    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(newDeleteSchema);
    updateChange.updateColumnType("col2", Types.LongType.get())
        .updateColumnComment("col2", "alter col2 comments")
        .renameColumn("col2", "colx") // rename col2 to colx to assert later on renamed field
        .addPositionChange("col2", "col4", "after");
    InternalSchema updateSchema = SchemaChangeUtils.applyTableChanges2Schema(newDeleteSchema, updateChange);

    // add col1 again
    TableChanges.ColumnAddChange addChange1 = TableChanges.ColumnAddChange.get(updateSchema);
    addChange1.addColumns("col1", Types.BooleanType.get(), "add new col1");
    InternalSchema finalSchema = SchemaChangeUtils.applyTableChanges2Schema(updateSchema, addChange1);
    // The merger now lives on HoodieSchema. Bridge the legacy InternalSchema
    // fixtures (built via the DDL action algebra still under exercise here)
    // and run the merge through the new façade.
    HoodieSchema oldHoodie = HoodieSchemaInternalSchemaBridge.toHoodieSchema(oldSchema, "Record");
    HoodieSchema finalHoodie = HoodieSchemaInternalSchemaBridge.toHoodieSchema(finalSchema, "Record");

    // merge schema by using columnType from query schema
    Pair<HoodieSchema, Map<String, String>> mergeWithRenamed = new HoodieSchemaMerger(
        oldHoodie, finalHoodie, true, false, false).mergeSchemaGetRenamed();
    assertEquals("col2", mergeWithRenamed.getRight().get("colx"));
    InternalSchema checkedSchema = new InternalSchema(Types.RecordType.get(Arrays.asList(
        Types.Field.get(4, true, "c1", Types.BooleanType.get(), "add c1 after col1"),
        Types.Field.get(5, true, "c2", Types.IntType.get(), "add c2 before col3"),
        Types.Field.get(3, true, "col4", Types.FloatType.get()),
        Types.Field.get(1, true, "col2", Types.LongType.get(), "alter col2 comments"),
        Types.Field.get(6, true, "col1suffix", Types.BooleanType.get(), "add new col1"))));
    // merged schema without renamed fields
    HoodieSchema mergeSchema = new HoodieSchemaMerger(oldHoodie, finalHoodie, true, false).mergeSchema();
    assertEquals(checkedSchema, HoodieSchemaInternalSchemaBridge.toInternalSchema(mergeSchema));

    // merge schema by using columnType from file schema
    HoodieSchema mergeSchema1 = new HoodieSchemaMerger(oldHoodie, finalHoodie, true, true).mergeSchema();
    InternalSchema checkedSchema1 = new InternalSchema(Types.RecordType.get(Arrays.asList(
        Types.Field.get(4, true, "c1", Types.BooleanType.get(), "add c1 after col1"),
        Types.Field.get(5, true, "c2", Types.IntType.get(), "add c2 before col3"),
        Types.Field.get(3, true, "col4", Types.FloatType.get()),
        Types.Field.get(1, true, "col2", Types.IntType.get(), "alter col2 comments"),
        Types.Field.get(6, true, "col1suffix", Types.BooleanType.get(), "add new col1"))));
    assertEquals(checkedSchema1, HoodieSchemaInternalSchemaBridge.toInternalSchema(mergeSchema1));
  }
}

