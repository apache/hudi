/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.reader;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link RowDataRecordCloner}.
 */
public class TestRowDataRecordCloner {

  @Test
  public void testCloneSimpleRowData() {
    // Create a simple row type with string and int fields
    RowType rowType = RowType.of(
        new RowType.RowField("name", new VarCharType()).getType(),
        new RowType.RowField("age", new IntType()).getType()
    );

    RowDataRecordCloner cloner = new RowDataRecordCloner(rowType);

    // Create original row
    GenericRowData original = GenericRowData.of(
        StringData.fromString("John"),
        25
    );

    // Clone the row
    RowData cloned = cloner.clone(original);

    // Verify the cloned data equals the original
    assertEquals(original.getString(0), cloned.getString(0));
    assertEquals(original.getInt(1), cloned.getInt(1));

    // Verify it's a different object (deep copy)
    assertNotSame(original, cloned);
  }

  @Test
  public void testClonePreservesIndependence() {
    // Create a row type
    RowType rowType = RowType.of(
        new RowType.RowField("value", new IntType()).getType()
    );

    RowDataRecordCloner cloner = new RowDataRecordCloner(rowType);

    // Create mutable row
    GenericRowData original = GenericRowData.of(100);

    // Clone it
    RowData cloned = cloner.clone(original);

    // Verify initial values match
    assertEquals(100, original.getInt(0));
    assertEquals(100, cloned.getInt(0));

    // Modify the original
    original.setField(0, 200);

    // Verify the clone is unaffected
    assertEquals(200, original.getInt(0));
    assertEquals(100, cloned.getInt(0));
  }

  @Test
  public void testCloneMultipleFields() {
    RowType rowType = RowType.of(
        new RowType.RowField("id", new IntType()).getType(),
        new RowType.RowField("name", new VarCharType()).getType(),
        new RowType.RowField("score", new IntType()).getType(),
        new RowType.RowField("city", new VarCharType()).getType()
    );

    RowDataRecordCloner cloner = new RowDataRecordCloner(rowType);

    GenericRowData original = GenericRowData.of(
        1,
        StringData.fromString("Alice"),
        95,
        StringData.fromString("Seattle")
    );

    RowData cloned = cloner.clone(original);

    // Verify all fields are correctly cloned
    assertEquals(1, cloned.getInt(0));
    assertEquals(StringData.fromString("Alice"), cloned.getString(1));
    assertEquals(95, cloned.getInt(2));
    assertEquals(StringData.fromString("Seattle"), cloned.getString(3));

    // Verify independence
    assertNotSame(original, cloned);
  }

  @Test
  public void testCloneNullFields() {
    RowType rowType = RowType.of(
        new RowType.RowField("name", new VarCharType()).getType(),
        new RowType.RowField("age", new IntType()).getType()
    );

    RowDataRecordCloner cloner = new RowDataRecordCloner(rowType);

    // Create row with null field
    GenericRowData original = GenericRowData.of(null, 30);

    RowData cloned = cloner.clone(original);

    // Verify null is preserved
    assertTrue(cloned.isNullAt(0));
    assertEquals(30, cloned.getInt(1));

    assertNotSame(original, cloned);
  }

  @Test
  public void testCloneMultipleTimesProducesIndependentCopies() {
    RowType rowType = RowType.of(
        new RowType.RowField("counter", new IntType()).getType()
    );

    RowDataRecordCloner cloner = new RowDataRecordCloner(rowType);

    GenericRowData original = GenericRowData.of(1);

    // Create multiple clones
    RowData clone1 = cloner.clone(original);
    RowData clone2 = cloner.clone(original);
    RowData clone3 = cloner.clone(original);

    // All should have the same value
    assertEquals(1, clone1.getInt(0));
    assertEquals(1, clone2.getInt(0));
    assertEquals(1, clone3.getInt(0));

    // But be different objects
    assertNotSame(clone1, clone2);
    assertNotSame(clone2, clone3);
    assertNotSame(clone1, clone3);
  }

  @Test
  public void testCloneWithStringData() {
    RowType rowType = RowType.of(
        new RowType.RowField("message", new VarCharType()).getType()
    );

    RowDataRecordCloner cloner = new RowDataRecordCloner(rowType);

    GenericRowData original = GenericRowData.of(
        StringData.fromString("Hello, World!")
    );

    RowData cloned = cloner.clone(original);

    assertEquals(StringData.fromString("Hello, World!"), cloned.getString(0));
    assertNotSame(original, cloned);
  }

  @Test
  public void testCloneEmptyRow() {
    // Row type with no fields
    RowType rowType = RowType.of();

    RowDataRecordCloner cloner = new RowDataRecordCloner(rowType);

    GenericRowData original = new GenericRowData(0);

    RowData cloned = cloner.clone(original);

    assertEquals(0, cloned.getArity());
    assertNotSame(original, cloned);
  }

  @Test
  public void testClonePreservesRowKind() {
    RowType rowType = RowType.of(
        new RowType.RowField("value", new IntType()).getType()
    );

    RowDataRecordCloner cloner = new RowDataRecordCloner(rowType);

    GenericRowData original = GenericRowData.of(42);
    original.setRowKind(org.apache.flink.types.RowKind.UPDATE_AFTER);

    RowData cloned = cloner.clone(original);

    assertEquals(42, cloned.getInt(0));
    assertEquals(org.apache.flink.types.RowKind.UPDATE_AFTER, cloned.getRowKind());
    assertNotSame(original, cloned);
  }

  @Test
  public void testClonerIsSerializable() {
    RowType rowType = RowType.of(
        new RowType.RowField("id", new IntType()).getType()
    );

    RowDataRecordCloner cloner = new RowDataRecordCloner(rowType);

    // Verify it implements Serializable through RecordCloner interface
    assertTrue(cloner instanceof java.io.Serializable);
  }

  @Test
  public void testMultipleClonesFromSameCloner() {
    RowType rowType = RowType.of(
        new RowType.RowField("name", new VarCharType()).getType(),
        new RowType.RowField("value", new IntType()).getType()
    );

    RowDataRecordCloner cloner = new RowDataRecordCloner(rowType);

    // Create different original rows
    GenericRowData row1 = GenericRowData.of(StringData.fromString("A"), 1);
    GenericRowData row2 = GenericRowData.of(StringData.fromString("B"), 2);
    GenericRowData row3 = GenericRowData.of(StringData.fromString("C"), 3);

    // Clone using the same cloner instance
    RowData cloned1 = cloner.clone(row1);
    RowData cloned2 = cloner.clone(row2);
    RowData cloned3 = cloner.clone(row3);

    // Verify each clone matches its original
    assertEquals(StringData.fromString("A"), cloned1.getString(0));
    assertEquals(1, cloned1.getInt(1));

    assertEquals(StringData.fromString("B"), cloned2.getString(0));
    assertEquals(2, cloned2.getInt(1));

    assertEquals(StringData.fromString("C"), cloned3.getString(0));
    assertEquals(3, cloned3.getInt(1));
  }
}
