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

package org.apache.hudi.table.format.cow.vector.type;

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link ParquetGroupField}.
 */
public class TestParquetGroupField {

  @Test
  void testChildrenWithAllNonNullEntriesAreRetained() {
    ParquetField c0 = new ParquetPrimitiveField(new IntType(), true, descriptor(), 0);
    ParquetField c1 = new ParquetPrimitiveField(new VarCharType(), true, descriptor(), 1);
    List<ParquetField> children = Arrays.asList(c0, c1);

    ParquetGroupField group = new ParquetGroupField(rowType(), 0, 1, true, children);

    assertEquals(2, group.getChildren().size());
    assertSame(c0, group.getChildren().get(0));
    assertSame(c1, group.getChildren().get(1));
  }

  @Test
  void testChildrenMayContainNullForSchemaEvolution() {
    // A ROW field present in the requested Flink schema but absent from the Parquet file is
    // represented by a null slot in `children`. The group must allow this (Hudi-specific
    // extension over Flink's ImmutableList-backed equivalent).
    ParquetField present = new ParquetPrimitiveField(new IntType(), true, descriptor(), 0);
    List<ParquetField> children = Arrays.asList(present, null);

    ParquetGroupField group = new ParquetGroupField(rowType(), 0, 1, true, children);

    assertEquals(2, group.getChildren().size());
    assertNotNull(group.getChildren().get(0));
    assertNull(group.getChildren().get(1));
  }

  @Test
  void testChildrenListIsUnmodifiable() {
    ParquetField child = new ParquetPrimitiveField(new IntType(), true, descriptor(), 0);
    ParquetGroupField group =
        new ParquetGroupField(rowType(), 0, 1, true, Collections.singletonList(child));

    assertThrows(UnsupportedOperationException.class, () -> group.getChildren().add(null));
    assertThrows(UnsupportedOperationException.class, () -> group.getChildren().remove(0));
  }

  @Test
  void testChildrenListIsDefensivelyCopied() {
    // Mutations to the caller-supplied list must not be visible through the group.
    ParquetField child = new ParquetPrimitiveField(new IntType(), true, descriptor(), 0);
    List<ParquetField> mutable = new ArrayList<>();
    mutable.add(child);

    ParquetGroupField group = new ParquetGroupField(rowType(), 0, 1, true, mutable);
    mutable.add(null);

    assertEquals(1, group.getChildren().size());
  }

  @Test
  void testNullChildrenListThrows() {
    assertThrows(
        NullPointerException.class,
        () -> new ParquetGroupField(rowType(), 0, 1, true, null));
  }

  @Test
  void testEmptyChildrenListIsAllowed() {
    ParquetGroupField group =
        new ParquetGroupField(rowType(), 0, 1, true, Collections.emptyList());

    assertEquals(0, group.getChildren().size());
  }

  @Test
  void testFieldMetadataIsExposed() {
    ParquetGroupField group =
        new ParquetGroupField(rowType(), 2, 5, false, Collections.emptyList());

    assertEquals(2, group.getRepetitionLevel());
    assertEquals(5, group.getDefinitionLevel());
    assertFalse(group.isRequired());
  }

  private static LogicalType rowType() {
    return RowType.of(new IntType());
  }

  private static ColumnDescriptor descriptor() {
    PrimitiveType primitive = Types.required(PrimitiveTypeName.INT32).named("f");
    return new ColumnDescriptor(new String[] {"f"}, primitive, 0, 0);
  }
}
