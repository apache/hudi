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

package org.apache.hudi.internal.schema.action;

import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;

import org.apache.hudi.internal.schema.utils.SchemaChangeUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class TestMergeSchema {

  @Test
  public void testPrimitiveMerge() {
    Types.RecordType record = Types.RecordType.get(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "col1", Types.BooleanType.get()),
        Types.Field.get(1, "col2", Types.IntType.get()),
        Types.Field.get(2, "col3", Types.LongType.get()),
        Types.Field.get(3, "col4", Types.FloatType.get())}));

    InternalSchema oldSchema = new InternalSchema(record.fields());
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

    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(newDeleteSchema);
    updateChange.updateColumnType("col2", Types.LongType.get())
        .updateColumnComment("col2", "alter col2 comments")
        .renameColumn("col2", "colx").addPositionChange("col2",
        "col4", "after");
    InternalSchema updateSchema = SchemaChangeUtils.applyTableChanges2Schema(newDeleteSchema, updateChange);

    // add col1 again
    TableChanges.ColumnAddChange addChange1 = TableChanges.ColumnAddChange.get(updateSchema);
    addChange1.addColumns("col1", Types.BooleanType.get(), "add new col1");
    InternalSchema finalSchema = SchemaChangeUtils.applyTableChanges2Schema(updateSchema, addChange1);
    InternalSchema mergeSchema = InternalSchemaUtils.mergeSchema(oldSchema, finalSchema);
  }
}

