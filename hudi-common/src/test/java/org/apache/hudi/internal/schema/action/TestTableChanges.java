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

import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;

import org.apache.hudi.internal.schema.Types.StringType;
import org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType;
import org.apache.hudi.internal.schema.utils.SchemaChangeUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;

public class TestTableChanges {

  @Test
  public void testPrimitiveAdd() {
    Types.RecordType record = Types.RecordType.get(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "col1", Types.BooleanType.get()),
        Types.Field.get(1, "col2", Types.IntType.get()),
        Types.Field.get(2, "col3", Types.LongType.get()),
        Types.Field.get(3, "col4", Types.FloatType.get())}));

    Types.RecordType checkRecord = Types.RecordType.get(Arrays.asList(new Types.Field[]  {
        Types.Field.get(0, "col1", Types.BooleanType.get()),
        Types.Field.get(4, true, "c1", Types.BooleanType.get(), "add c1 after col1"),
        Types.Field.get(1, "col2", Types.IntType.get()),
        Types.Field.get(5, true, "c2", Types.IntType.get(), "add c2 before col3"),
        Types.Field.get(2, "col3", Types.LongType.get()),
        Types.Field.get(3, "col4", Types.FloatType.get())}));

    InternalSchema oldSchema = new InternalSchema(record.fields());
    // add c1 after 'col1', and c2 before 'col3'
    TableChanges.ColumnAddChange addChange = TableChanges.ColumnAddChange.get(oldSchema);
    addChange.addColumns("c1", Types.BooleanType.get(), "add c1 after col1");
    // check repeated add.
    Assertions.assertThrows(HoodieSchemaException.class, () -> addChange.addColumns("c1", Types.BooleanType.get(), "add c1 after col1"));
    addChange.addPositionChange("c1", "col1", "after");
    addChange.addColumns("c2", Types.IntType.get(), "add c2 before col3");
    addChange.addPositionChange("c2", "col3", "before");
    InternalSchema newSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, addChange);
    Assertions.assertEquals(newSchema.getRecord(), checkRecord);
  }

  @Test
  public void testNestAdd() {
    InternalSchema oldSchema = new InternalSchema(Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(7, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(8, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false, "locations", Types.MapType.get(9, 10, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(11, false, "lat", Types.FloatType.get()), Types.Field.get(12, false, "long", Types.FloatType.get())), false)),
        Types.Field.get(4, true, "points", Types.ArrayType.get(13, true,
            Types.RecordType.get(Types.Field.get(14, false, "x", Types.LongType.get()), Types.Field.get(15, false, "y", Types.LongType.get())))),
        Types.Field.get(5, false,"doubles", Types.ArrayType.get(16, false, Types.DoubleType.get())),
        Types.Field.get(6, true, "properties", Types.MapType.get(17, 18, Types.StringType.get(), Types.StringType.get()))
    );

    TableChanges.ColumnAddChange addChange = TableChanges.ColumnAddChange.get(oldSchema);
    // add c1 first
    addChange.addColumns("c1", Types.StringType.get(), "add c1 first");
    addChange.addPositionChange("c1", "id", "before");
    //add preferences.cx before preferences.feature2
    addChange.addColumns("preferences", "cx", Types.BooleanType.get(), "add preferences.cx before preferences.feature2");
    // check repeated add.
    Assertions.assertThrows(HoodieSchemaException.class, () -> addChange.addColumns("preferences", "cx", Types.BooleanType.get(), "add preferences.cx before preferences.feature2"));
    addChange.addPositionChange("preferences.cx", "preferences.feature2", "before");
    // add locations.value.lax before locations.value.long
    addChange.addColumns("locations.value", "lax", Types.BooleanType.get(), "add locations.value.lax before locations.value.long");
    addChange.addPositionChange("locations.value.lax", "locations.value.long", "before");
    //
    // add points.element.z after points.element.y
    addChange.addColumns("points.element", "z", Types.BooleanType.get(), "add points.element.z after points.element.y");
    addChange.addPositionChange("points.element.z", "points.element.y", "after");
    InternalSchema newSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, addChange);
    InternalSchema checkedSchema = new InternalSchema(
        Types.Field.get(19, true, "c1", Types.StringType.get(), "add c1 first"),
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(7, false, "feature1", Types.BooleanType.get()),
                Types.Field.get(20, true, "cx", Types.BooleanType.get(), "add preferences.cx before preferences.feature2"),
                Types.Field.get(8, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false, "locations", Types.MapType.get(9, 10, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(11, false, "lat", Types.FloatType.get()),
                Types.Field.get(21, true, "lax", Types.BooleanType.get(), "add locations.value.lax before locations.value.long"),
                Types.Field.get(12, false, "long", Types.FloatType.get())), false)),
        Types.Field.get(4, true, "points", Types.ArrayType.get(13, true,
            Types.RecordType.get(Types.Field.get(14, false, "x", Types.LongType.get()),
                Types.Field.get(15, false, "y", Types.LongType.get()),
                Types.Field.get(22, true, "z", Types.BooleanType.get(), "add points.element.z after points.element.y")))),
        Types.Field.get(5, false,"doubles", Types.ArrayType.get(16, false, Types.DoubleType.get())),
        Types.Field.get(6, true, "properties", Types.MapType.get(17, 18, Types.StringType.get(), Types.StringType.get()))
    );
    Assertions.assertEquals(newSchema.getRecord(), checkedSchema.getRecord());
  }

  @Test
  public void testPrimitiveDelete() {
    Types.RecordType record = Types.RecordType.get(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "col1", Types.BooleanType.get()),
        Types.Field.get(1, "col2", Types.IntType.get()),
        Types.Field.get(2, "col3", Types.LongType.get()),
        Types.Field.get(3, "col4", Types.FloatType.get())}));
    InternalSchema oldSchema = new InternalSchema(record.fields());
    TableChanges.ColumnDeleteChange deleteChange = TableChanges.ColumnDeleteChange.get(oldSchema);
    deleteChange.deleteColumn("col1");
    // check repeated delete.
    // deletechange can handle deleting the same column multiple times, only keep one operation.
    deleteChange.deleteColumn("col1");
    deleteChange.deleteColumn("col3");
    InternalSchema newSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, deleteChange);
    Types.RecordType checkRecord = Types.RecordType.get(Arrays.asList(new Types.Field[] {
        Types.Field.get(1, "col2", Types.IntType.get()),
        Types.Field.get(3, "col4", Types.FloatType.get())}));
    Assertions.assertEquals(newSchema.getRecord(), checkRecord);
  }

  @Test
  public void testNestDelete() {
    InternalSchema oldSchema = new InternalSchema(Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(5, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(6, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false, "locations", Types.MapType.get(7, 8, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(9, false, "lat", Types.FloatType.get()), Types.Field.get(10, false, "long", Types.FloatType.get())), false)),
        Types.Field.get(4, true, "points", Types.ArrayType.get(11, true,
            Types.RecordType.get(Types.Field.get(12, false, "x", Types.LongType.get()), Types.Field.get(13, false, "y", Types.LongType.get()))))
    );
    TableChanges.ColumnDeleteChange deleteChange = TableChanges.ColumnDeleteChange.get(oldSchema);
    deleteChange.deleteColumn("data");
    deleteChange.deleteColumn("preferences.feature2");
    deleteChange.deleteColumn("preferences.feature2");
    deleteChange.deleteColumn("locations.value.lat");
    deleteChange.deleteColumn("points.element.y");
    InternalSchema newSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, deleteChange);
    InternalSchema checkedSchema = new InternalSchema(Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(5, false, "feature1",
                Types.BooleanType.get()))),
        Types.Field.get(3, false, "locations", Types.MapType.get(7, 8, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(10, false, "long", Types.FloatType.get())), false)),
        Types.Field.get(4, true, "points", Types.ArrayType.get(11, true,
            Types.RecordType.get(Types.Field.get(12, false, "x", Types.LongType.get()))))
    );
    Assertions.assertEquals(newSchema.getRecord(), checkedSchema.getRecord());
  }

  @Test
  public void testPrimitiveUpdate() {
    Types.RecordType record = Types.RecordType.get(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "col1", Types.BooleanType.get()),
        Types.Field.get(1, "col2", Types.IntType.get()),
        Types.Field.get(2, "col3", Types.LongType.get()),
        Types.Field.get(3, "col4", Types.FloatType.get())}));
    InternalSchema oldSchema = new InternalSchema(record.fields());
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(oldSchema);
    updateChange.updateColumnType("col2", Types.LongType.get())
        .updateColumnComment("col2", "alter col2 comments")
        .renameColumn("col2", "colx").addPositionChange("col2", "col4", "after");
    InternalSchema newSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, updateChange);
    Types.RecordType checkedRecord = Types.RecordType.get(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "col1", Types.BooleanType.get()),
        Types.Field.get(2, "col3", Types.LongType.get()),
        Types.Field.get(3, "col4", Types.FloatType.get()),
        Types.Field.get(1, true, "colx", Types.LongType.get(), "alter col2 comments")}));
    Assertions.assertEquals(newSchema.getRecord(), checkedRecord);
  }

  @Test
  public void testNestUpdate() {
    InternalSchema oldSchema = new InternalSchema(Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(5, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(6, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false, "locations", Types.MapType.get(7, 8, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(9, false, "lat", Types.FloatType.get()), Types.Field.get(10, false, "long", Types.FloatType.get())), false)),
        Types.Field.get(4, true, "points", Types.ArrayType.get(11, true,
            Types.RecordType.get(Types.Field.get(12, false, "x", Types.LongType.get()), Types.Field.get(13, false, "y", Types.LongType.get()))))
    );
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(oldSchema);
    updateChange
        .updateColumnNullability("id", true)
        .renameColumn("id", "idx")
        .addPositionChange("data", "points", "after");
    updateChange
        .updateColumnComment("preferences.feature1", "add feature1 comment")
        .renameColumn("preferences.feature1", "f1")
        .addPositionChange("preferences.feature1", "preferences.feature1", "first");
    updateChange.updateColumnComment("locations.value.lat", "add lat comment")
        .renameColumn("locations.value.lat", "lax")
        .addPositionChange("locations.value.lat", "locations.value.lat", "first");
    updateChange.renameColumn("points.element.x", "z")
        .addPositionChange("points.element.x", "points.element.y", "after");
    InternalSchema newSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, updateChange);
    InternalSchema checkSchema = new InternalSchema(Types.Field.get(0, true, "idx", Types.IntType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(5, false, "f1",
                Types.BooleanType.get(), "add feature1 comment"), Types.Field.get(6, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false, "locations", Types.MapType.get(7, 8, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(9, false, "lax", Types.FloatType.get(), "add lat comment"), Types.Field.get(10, false, "long", Types.FloatType.get())), false)),
        Types.Field.get(4, true, "points", Types.ArrayType.get(11, true,
            Types.RecordType.get(Types.Field.get(13, false, "y", Types.LongType.get()), Types.Field.get(12, false, "z", Types.LongType.get())))),
        Types.Field.get(1, true, "data", Types.StringType.get())
    );
    Assertions.assertEquals(newSchema.getRecord(), checkSchema.getRecord());
  }

  @Test
  public void testChangeApplier() {
    // We add test here to verify the logic of applyAddChange and applyReOrderColPositionChange
    InternalSchema oldSchema = new InternalSchema(Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(7, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(8, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false, "locations", Types.MapType.get(9, 10, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(11, false, "lat", Types.FloatType.get()), Types.Field.get(12, false, "long", Types.FloatType.get())), false)),
        Types.Field.get(4, true, "points", Types.ArrayType.get(13, true,
            Types.RecordType.get(Types.Field.get(14, false, "x", Types.LongType.get()), Types.Field.get(15, false, "y", Types.LongType.get())))),
        Types.Field.get(5, false,"doubles", Types.ArrayType.get(16, false, Types.DoubleType.get())),
        Types.Field.get(6, true, "properties", Types.MapType.get(17, 18, Types.StringType.get(), Types.StringType.get()))
    );

    // add c1 first
    InternalSchema newSchema = addOperationForSchemaChangeApplier(oldSchema, "c1", StringType.get(), "add c1 first",
        "id", ColumnPositionType.BEFORE);
    //add preferences.cx before preferences.feature2
    newSchema = addOperationForSchemaChangeApplier(newSchema, "preferences.cx", Types.BooleanType.get(), "add preferences.cx before preferences.feature2",
        "preferences.feature2", ColumnPositionType.BEFORE);
    // check repeated add.
    InternalSchema currSchema = newSchema;
    Assertions.assertThrows(HoodieSchemaException.class, () -> addOperationForSchemaChangeApplier(currSchema, "preferences.cx", Types.BooleanType.get(),
        "add preferences.cx before preferences.feature2"));
    // add locations.value.lax before locations.value.long
    newSchema = addOperationForSchemaChangeApplier(newSchema, "locations.value.lax", Types.BooleanType.get(), "add locations.value.lax before locations.value.long");
    newSchema = reOrderOperationForSchemaChangeApplier(newSchema, "locations.value.lax", "locations.value.long", ColumnPositionType.BEFORE);
    //
    // add points.element.z after points.element.y
    newSchema = addOperationForSchemaChangeApplier(newSchema, "points.element.z", Types.BooleanType.get(), "add points.element.z after points.element.y", "points.element.y", ColumnPositionType.AFTER);
    InternalSchema checkedSchema = new InternalSchema(
        Types.Field.get(19, true, "c1", Types.StringType.get(), "add c1 first"),
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(7, false, "feature1", Types.BooleanType.get()),
                Types.Field.get(20, true, "cx", Types.BooleanType.get(), "add preferences.cx before preferences.feature2"),
                Types.Field.get(8, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false, "locations", Types.MapType.get(9, 10, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(11, false, "lat", Types.FloatType.get()),
                Types.Field.get(21, true, "lax", Types.BooleanType.get(), "add locations.value.lax before locations.value.long"),
                Types.Field.get(12, false, "long", Types.FloatType.get())), false)),
        Types.Field.get(4, true, "points", Types.ArrayType.get(13, true,
            Types.RecordType.get(Types.Field.get(14, false, "x", Types.LongType.get()),
                Types.Field.get(15, false, "y", Types.LongType.get()),
                Types.Field.get(22, true, "z", Types.BooleanType.get(), "add points.element.z after points.element.y")))),
        Types.Field.get(5, false,"doubles", Types.ArrayType.get(16, false, Types.DoubleType.get())),
        Types.Field.get(6, true, "properties", Types.MapType.get(17, 18, Types.StringType.get(), Types.StringType.get()))
    );
    Assertions.assertEquals(newSchema.getRecord(), checkedSchema.getRecord());
  }

  private static InternalSchema addOperationForSchemaChangeApplier(
      InternalSchema schema,
      String colName,
      Type colType,
      String doc,
      String position,
      TableChange.ColumnPositionChange.ColumnPositionType positionType) {
    InternalSchemaChangeApplier applier = new InternalSchemaChangeApplier(schema);
    return applier.applyAddChange(colName, colType, doc, position, positionType);
  }

  private static InternalSchema reOrderOperationForSchemaChangeApplier(
      InternalSchema schema,
      String colName,
      String position,
      TableChange.ColumnPositionChange.ColumnPositionType positionType) {
    InternalSchemaChangeApplier applier = new InternalSchemaChangeApplier(schema);
    return applier.applyReOrderColPositionChange(colName, position, positionType);
  }

  private static InternalSchema addOperationForSchemaChangeApplier(
      InternalSchema schema,
      String colName,
      Type colType,
      String doc) {
    return addOperationForSchemaChangeApplier(schema, colName, colType, doc, "",
        ColumnPositionType.NO_OPERATION);
  }
}

