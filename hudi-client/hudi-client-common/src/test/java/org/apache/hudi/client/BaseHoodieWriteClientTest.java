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

package org.apache.hudi.client;

import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.TableChange;
import org.apache.hudi.internal.schema.action.TableChanges;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.Arrays;

@RunWith(JUnitPlatform.class)
class BaseHoodieWriteClientTest {
  @Test
  public void testGetVerifiedSchemaWhenDoAddChangeWithSimPleRecord() {
    Types.RecordType record = getSimpleRecordType();
    InternalSchema originSchema = new InternalSchema(record.fields());
    // add newCol1
    InternalSchema latestSchema = new InternalSchema(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "bool", Types.BooleanType.get()),
        Types.Field.get(1, "int", Types.IntType.get()),
        Types.Field.get(2, "long", Types.LongType.get()),
        Types.Field.get(3, "float", Types.FloatType.get()),
        Types.Field.get(4, "double", Types.DoubleType.get()),
        Types.Field.get(6, true,"newCol1", Types.StringType.get(), "test add newCol1"),
        Types.Field.get(5, true,"string", Types.StringType.get(), "this is string")
    }));
    // add newCol2
    TableChanges.ColumnAddChange addChange = TableChanges.ColumnAddChange.get(originSchema);
    addChange.addColumns("newCol2", Types.StringType.get(), "test add newCol2");
    addChange.addPositionChange("newCol2", "double", TableChange.ColumnPositionChange.ColumnPositionType.AFTER);

    InternalSchema returnedSchema = BaseHoodieWriteClient.getVerifiedInternalSchema(addChange, latestSchema, originSchema);
    InternalSchema checkedSchema = new InternalSchema(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "bool", Types.BooleanType.get()),
        Types.Field.get(1, "int", Types.IntType.get()),
        Types.Field.get(2, "long", Types.LongType.get()),
        Types.Field.get(3, "float", Types.FloatType.get()),
        Types.Field.get(4, "double", Types.DoubleType.get()),
        Types.Field.get(7, true,"newCol2", Types.StringType.get(), "test add newCol2"),
        Types.Field.get(6, true,"newCol1", Types.StringType.get(), "test add newCol1"),
        Types.Field.get(5, true,"string", Types.StringType.get(), "this is string")
    }));
    Assertions.assertEquals(checkedSchema, returnedSchema);
  }

  @Test
  public void testGetVerifiedSchemaWhenDoUpdateChangeWithSimPleRecord() {
    Types.RecordType record = getSimpleRecordType();
    InternalSchema originSchema = new InternalSchema(record.fields());
    // change name double to doubleNew
    InternalSchema latestSchema = new InternalSchema(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "bool", Types.BooleanType.get()),
        Types.Field.get(1, "int", Types.IntType.get()),
        Types.Field.get(2, "long", Types.LongType.get()),
        Types.Field.get(3, "float", Types.FloatType.get()),
        Types.Field.get(4, "doubleNew", Types.DoubleType.get()),
        Types.Field.get(5, true,"string", Types.StringType.get(), "this is string")
    }));
    // change name bool to boolbool
    // change string's comment
    // change int's type
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(originSchema);
    updateChange.renameColumn("bool", "boolbool");
    updateChange.updateColumnComment("string", "this is new doc of string");
    updateChange.updateColumnType("int", Types.LongType.get());

    InternalSchema returnedSchema = BaseHoodieWriteClient.getVerifiedInternalSchema(updateChange, latestSchema, originSchema);
    InternalSchema checkedSchema = new InternalSchema(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "boolbool", Types.BooleanType.get()),
        Types.Field.get(1, "int", Types.LongType.get()),
        Types.Field.get(2, "long", Types.LongType.get()),
        Types.Field.get(3, "float", Types.FloatType.get()),
        Types.Field.get(4, "doubleNew", Types.DoubleType.get()),
        Types.Field.get(5, true,"string", Types.StringType.get(), "this is new doc of string")
    }));
    Assertions.assertEquals(checkedSchema, returnedSchema);
  }

  @Test
  public void testGetVerifiedSchemaWhenDoDeleteChangeWithSimPleRecord() {
    Types.RecordType record = getSimpleRecordType();
    InternalSchema originSchema = new InternalSchema(record.fields());
    // delete double
    InternalSchema latestSchema = new InternalSchema(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "bool", Types.BooleanType.get()),
        Types.Field.get(1, "int", Types.IntType.get()),
        Types.Field.get(2, "long", Types.LongType.get()),
        Types.Field.get(3, "float", Types.FloatType.get()),
        Types.Field.get(5, true,"string", Types.StringType.get(), "this is string")
    }));
    // delete string
    TableChanges.ColumnDeleteChange deleteChange = TableChanges.ColumnDeleteChange.get(originSchema);
    deleteChange.deleteColumn("string");

    InternalSchema returnedSchema = BaseHoodieWriteClient.getVerifiedInternalSchema(deleteChange, latestSchema, originSchema);
    InternalSchema checkedSchema = new InternalSchema(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "bool", Types.BooleanType.get()),
        Types.Field.get(1, "int", Types.IntType.get()),
        Types.Field.get(2, "long", Types.LongType.get()),
        Types.Field.get(3, "float", Types.FloatType.get()),
    }));
    Assertions.assertEquals(checkedSchema, returnedSchema);
  }

  @Test
  public void testGetVerifiedSchemaWhenDoAddChangeWithNestRecord() {
    Types.RecordType record = getNestRecordType();
    InternalSchema originSchema = new InternalSchema(record.fields());
    // add newCol
    InternalSchema latestSchema = new InternalSchema(
        Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(12, true, "newCol", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
          Types.RecordType.get(
            Types.Field.get(5, false, "feature1", Types.BooleanType.get()),
            Types.Field.get(6, true, "feature2", Types.IntType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
          Types.RecordType.get(
            Types.Field.get(10, false, "lat", Types.FloatType.get()),
            Types.Field.get(11, false, "long", Types.FloatType.get(), "this is long column")
          ), false))).fields());
    // add preferences.feature3
    // add locations.value.int
    TableChanges.ColumnAddChange addChange = TableChanges.ColumnAddChange.get(originSchema);
    addChange.addColumns("preferences", "feature3", Types.StringType.get(), "test add preferences.feature3");
    addChange.addPositionChange("preferences.feature3", "", TableChange.ColumnPositionChange.ColumnPositionType.FIRST);
    addChange.addColumns("locations.value", "int", Types.StringType.get(), "test add locations.key.int");
    addChange.addPositionChange("locations.value.int", "locations.value.lat", TableChange.ColumnPositionChange.ColumnPositionType.AFTER);

    InternalSchema returnedSchema = BaseHoodieWriteClient.getVerifiedInternalSchema(addChange, latestSchema, originSchema);
    InternalSchema checkedSchema = new InternalSchema(
        Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(12, true, "newCol", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
          Types.RecordType.get(
            Types.Field.get(13, true, "feature3", Types.StringType.get(), "test add preferences.feature3"),
            Types.Field.get(5, false, "feature1", Types.BooleanType.get()),
            Types.Field.get(6, true, "feature2", Types.IntType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
          Types.RecordType.get(
            Types.Field.get(10, false, "lat", Types.FloatType.get()),
            Types.Field.get(14, true, "int", Types.StringType.get(), "test add locations.key.int"),
            Types.Field.get(11, false, "long", Types.FloatType.get(), "this is long column")
          ), false))).fields());
    Assertions.assertEquals(checkedSchema, returnedSchema);
  }

  @Test
  public void testGetVerifiedSchemaWhenDoUpdateChangeWithNestRecord() {
    Types.RecordType record = getNestRecordType();
    InternalSchema originSchema = new InternalSchema(record.fields());
    // change id's type
    InternalSchema latestSchema = new InternalSchema(
        Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.LongType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
          Types.RecordType.get(
            Types.Field.get(5, false, "feature1", Types.BooleanType.get()),
            Types.Field.get(6, true, "feature2", Types.IntType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
          Types.RecordType.get(
            Types.Field.get(10, false, "lat", Types.FloatType.get()),
            Types.Field.get(11, false, "long", Types.FloatType.get(), "this is long column")
          ), false))).fields());
    // change update preferences.feature2's type
    // change locations.value.long's comment
    // rename preferences.feature1 to feature3
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(originSchema);
    updateChange.updateColumnType("preferences.feature2", Types.LongType.get());
    updateChange.updateColumnComment("locations.value.long", "this is new doc of long column");
    updateChange.renameColumn("preferences.feature1", "feature3");

    InternalSchema returnedSchema = BaseHoodieWriteClient.getVerifiedInternalSchema(updateChange, latestSchema, originSchema);
    InternalSchema checkedSchema = new InternalSchema(
        Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.LongType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
          Types.RecordType.get(
            Types.Field.get(5, false, "feature3", Types.BooleanType.get()),
            Types.Field.get(6, true, "feature2", Types.LongType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
          Types.RecordType.get(
            Types.Field.get(10, false, "lat", Types.FloatType.get()),
            Types.Field.get(11, false, "long", Types.FloatType.get(), "this is new doc of long column")), false))).fields());
    Assertions.assertEquals(checkedSchema, returnedSchema);
  }

  @Test
  public void testGetVerifiedSchemaWhenDoDeleteChangeWithNestRecord() {
    Types.RecordType record = getNestRecordType();
    InternalSchema originSchema = new InternalSchema(record.fields());
    // delete data
    InternalSchema latestSchema = new InternalSchema(
        Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(2, true, "preferences",
          Types.RecordType.get(
            Types.Field.get(5, false, "feature1", Types.BooleanType.get()),
            Types.Field.get(6, true, "feature2", Types.IntType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
          Types.RecordType.get(
            Types.Field.get(10, false, "lat", Types.FloatType.get()),
            Types.Field.get(11, false, "long", Types.FloatType.get(), "this is long column")
          ), false))).fields());

    TableChanges.ColumnDeleteChange deleteChange = TableChanges.ColumnDeleteChange.get(originSchema);
    deleteChange.deleteColumn("preferences.feature2");
    deleteChange.deleteColumn("locations.value.long");

    InternalSchema returnedSchema = BaseHoodieWriteClient.getVerifiedInternalSchema(deleteChange, latestSchema, originSchema);
    InternalSchema checkedSchema = new InternalSchema(
        Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(2, true, "preferences",
          Types.RecordType.get(
            Types.Field.get(5, false, "feature1", Types.BooleanType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
          Types.RecordType.get(
            Types.Field.get(10, false, "lat", Types.FloatType.get())), false))).fields());
    Assertions.assertEquals(checkedSchema, returnedSchema);
  }

  public Types.RecordType getSimpleRecordType() {
    return Types.RecordType.get(Arrays.asList(new Types.Field[] {
      Types.Field.get(0, "bool", Types.BooleanType.get()),
      Types.Field.get(1, "int", Types.IntType.get()),
      Types.Field.get(2, "long", Types.LongType.get()),
      Types.Field.get(3, "float", Types.FloatType.get()),
      Types.Field.get(4, "double", Types.DoubleType.get()),
      Types.Field.get(5, true,"string", Types.StringType.get(), "this is string")
    }));
  }

  public Types.RecordType getNestRecordType() {
    return Types.RecordType.get(Types.Field.get(0, false, "id", Types.IntType.get()),
      Types.Field.get(1, true, "data", Types.StringType.get()),
      Types.Field.get(2, true, "preferences",
        Types.RecordType.get(Types.Field.get(5, false, "feature1",
          Types.BooleanType.get()), Types.Field.get(6, true, "feature2", Types.IntType.get()))),
      Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
      Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
        Types.RecordType.get(Types.Field.get(10, false, "lat", Types.FloatType.get()), Types.Field.get(11, false, "long", Types.FloatType.get(), "this is long column")), false))
    );
  }
}