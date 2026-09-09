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

package org.apache.hudi.table.format.cow.vector;

import org.apache.flink.table.data.columnar.vector.heap.HeapIntVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapLongVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for the Flink 2.1-compatible accessors added on {@link HeapArrayVector},
 * {@link HeapMapColumnVector} and {@link HeapRowColumnVector} when vendoring Flink 2.1's
 * nested-Parquet reader (FLINK-35702).
 *
 * <p>The accessors are wrappers over the existing public fields so legacy callers continue to
 * work. These tests exist solely to pin down that wrapper contract — runtime correctness of the
 * Dremel-style read path is exercised end-to-end by integration tests in
 * {@code ITTestHoodieDataSource} (testParquetComplexTypes / testParquetComplexNestedRowTypes /
 * testParquetArrayMapOfRowTypes / testParquetNullChildColumnsRowTypes).
 */
class TestHeapColumnVectorAccessors {

  // -----------------------------------------------------------------------------------------------
  // HeapArrayVector
  // -----------------------------------------------------------------------------------------------

  @Test
  void heapArrayVectorAccessorsReflectPublicFields() {
    HeapIntVector child = new HeapIntVector(4);
    HeapArrayVector vector = new HeapArrayVector(2, child);

    long[] offsets = {0L, 2L};
    long[] lengths = {2L, 2L};
    HeapLongVector replacementChild = new HeapLongVector(4);

    vector.setOffsets(offsets);
    vector.setLengths(lengths);
    vector.setChild(replacementChild);
    vector.setSize(2);

    assertArrayEquals(offsets, vector.getOffsets());
    assertArrayEquals(lengths, vector.getLengths());
    assertSame(replacementChild, vector.getChild());
    assertEquals(2, vector.getSize());

    // Backing public fields are kept in sync — preserves backward compatibility.
    assertSame(offsets, vector.offsets);
    assertSame(lengths, vector.lengths);
    assertSame(replacementChild, vector.child);
  }

  // -----------------------------------------------------------------------------------------------
  // HeapMapColumnVector
  // -----------------------------------------------------------------------------------------------

  @Test
  void heapMapColumnVectorConstructorInitializesOffsetsAndLengths() {
    HeapIntVector keys = new HeapIntVector(4);
    HeapIntVector values = new HeapIntVector(4);

    HeapMapColumnVector vector = new HeapMapColumnVector(3, keys, values);

    assertEquals(3, vector.getOffsets().length);
    assertEquals(3, vector.getLengths().length);
  }

  @Test
  void heapMapColumnVectorAccessorsReflectInternalState() {
    HeapIntVector keys = new HeapIntVector(4);
    HeapIntVector values = new HeapIntVector(4);
    HeapMapColumnVector vector = new HeapMapColumnVector(2, keys, values);

    long[] offsets = {0L, 2L};
    long[] lengths = {2L, 2L};
    HeapLongVector newKeys = new HeapLongVector(4);
    HeapLongVector newValues = new HeapLongVector(4);

    vector.setOffsets(offsets);
    vector.setLengths(lengths);
    vector.setKeys(newKeys);
    vector.setValues(newValues);
    vector.setSize(2);

    assertArrayEquals(offsets, vector.getOffsets());
    assertArrayEquals(lengths, vector.getLengths());
    assertSame(newKeys, vector.getKeys());
    assertSame(newValues, vector.getValues());
    // The Flink-2.1-style ColumnVector accessors return the same underlying child.
    assertSame(newKeys, vector.getKeyColumnVector());
    assertSame(newValues, vector.getValueColumnVector());
    assertEquals(2, vector.getSize());
  }

  // -----------------------------------------------------------------------------------------------
  // HeapRowColumnVector
  // -----------------------------------------------------------------------------------------------

  @Test
  void heapRowColumnVectorFieldsAccessorsReflectPublicVectors() {
    HeapIntVector intField = new HeapIntVector(2);
    HeapLongVector longField = new HeapLongVector(2);
    HeapRowColumnVector vector = new HeapRowColumnVector(2, intField, longField);

    WritableColumnVector[] originalFields = vector.getFields();
    assertEquals(2, originalFields.length);
    assertSame(intField, originalFields[0]);
    assertSame(longField, originalFields[1]);
    // Backing public field is kept in sync — preserves backward compatibility.
    assertSame(originalFields, vector.vectors);

    HeapIntVector replacement = new HeapIntVector(2);
    WritableColumnVector[] replacementFields = {replacement, longField};
    vector.setFields(replacementFields);

    assertSame(replacementFields, vector.getFields());
    assertSame(replacementFields, vector.vectors);
  }
}
