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

package org.apache.hudi.table.format.cow.vector.reader;

import org.apache.hudi.table.format.cow.utils.NestedPositionUtil;
import org.apache.hudi.table.format.cow.vector.HeapArrayVector;
import org.apache.hudi.table.format.cow.vector.HeapMapColumnVector;
import org.apache.hudi.table.format.cow.vector.HeapRowColumnVector;
import org.apache.hudi.table.format.cow.vector.position.CollectionPosition;
import org.apache.hudi.table.format.cow.vector.position.LevelDelegation;
import org.apache.hudi.table.format.cow.vector.position.RowPosition;
import org.apache.hudi.table.format.cow.vector.type.ParquetField;
import org.apache.hudi.table.format.cow.vector.type.ParquetGroupField;
import org.apache.hudi.table.format.cow.vector.type.ParquetPrimitiveField;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.parquet.vector.reader.ColumnReader;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.heap.AbstractHeapVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ColumnReader used to read a {@code Group} type in Parquet ({@code Map}, {@code Array}, {@code
 * Row}). Resolves nested structures using Dremel striping/assembly; see <a
 * href="https://github.com/julienledem/redelm/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper">the
 * striping and assembly algorithms from the Dremel paper</a>.
 *
 * <p>Vendored from Apache Flink 2.1 (FLINK-35702, {@code
 * org.apache.flink.formats.parquet.vector.reader.NestedColumnReader}). Differences vs. upstream:
 *
 * <ul>
 *   <li>Uses Hudi-local {@code HeapRowColumnVector}/{@code HeapMapColumnVector}/{@code
 *       HeapArrayVector} instead of the Flink-private {@code HeapRowVector}/{@code
 *       HeapMapVector}/{@code HeapArrayVector}.
 *   <li>Supports Hudi's schema-evolution contract: a {@code ParquetGroupField} representing a
 *       {@link RowType} may contain {@code null} children — meaning the corresponding logical
 *       field is absent from the Parquet file. Those slots are passed through unchanged and do
 *       not contribute to the row's repetition/definition-level stream.
 * </ul>
 */
public class NestedColumnReader implements ColumnReader<WritableColumnVector> {

  private final Map<ColumnDescriptor, NestedPrimitiveColumnReader> columnReaders;
  private final boolean isUtcTimestamp;

  private final PageReadStore pages;

  private final ParquetField field;

  public NestedColumnReader(boolean isUtcTimestamp, PageReadStore pages, ParquetField field) {
    this.isUtcTimestamp = isUtcTimestamp;
    this.pages = pages;
    this.field = field;
    this.columnReaders = new HashMap<>();
  }

  @Override
  public void readToVector(int readNumber, WritableColumnVector vector) throws IOException {
    readData(field, readNumber, vector, false);
  }

  private Tuple2<LevelDelegation, WritableColumnVector> readData(
      ParquetField field, int readNumber, ColumnVector vector, boolean inside) throws IOException {
    if (field.getType() instanceof RowType) {
      return readRow((ParquetGroupField) field, readNumber, vector, inside);
    } else if (field.getType() instanceof MapType || field.getType() instanceof MultisetType) {
      return readMap((ParquetGroupField) field, readNumber, vector, inside);
    } else if (field.getType() instanceof ArrayType) {
      return readArray((ParquetGroupField) field, readNumber, vector, inside);
    } else {
      return readPrimitive((ParquetPrimitiveField) field, readNumber, vector);
    }
  }

  private Tuple2<LevelDelegation, WritableColumnVector> readRow(
      ParquetGroupField field, int readNumber, ColumnVector vector, boolean inside)
      throws IOException {
    HeapRowColumnVector heapRowVector = (HeapRowColumnVector) vector;
    LevelDelegation levelDelegation = null;
    List<ParquetField> children = field.getChildren();
    WritableColumnVector[] childrenVectors = heapRowVector.getFields();
    WritableColumnVector[] finalChildrenVectors = new WritableColumnVector[childrenVectors.length];
    for (int i = 0; i < children.size(); i++) {
      ParquetField child = children.get(i);
      if (child == null) {
        // Hudi schema-evolution: the logical field is not present in the Parquet file. The slot
        // vector was pre-populated with nulls by ParquetSplitReaderUtil#createWritableColumnVector
        // (ROW branch); keep it as is and skip contributing to the level stream.
        finalChildrenVectors[i] = childrenVectors[i];
        continue;
      }
      Tuple2<LevelDelegation, WritableColumnVector> tuple =
          readData(child, readNumber, childrenVectors[i], true);
      levelDelegation = tuple.f0;
      finalChildrenVectors[i] = tuple.f1;
    }
    if (levelDelegation == null) {
      throw new FlinkRuntimeException(
          String.format("Row field does not have any non-null children: %s.", field));
    }

    RowPosition rowPosition =
        NestedPositionUtil.calculateRowOffsets(
            field,
            levelDelegation.getDefinitionLevel(),
            levelDelegation.getRepetitionLevel());

    // If row was inside the structure, then we need to renew the vector to reset the
    // capacity.
    if (inside) {
      heapRowVector = new HeapRowColumnVector(rowPosition.getPositionsCount(), finalChildrenVectors);
    } else {
      heapRowVector.setFields(finalChildrenVectors);
    }

    if (rowPosition.getIsNull() != null) {
      setFieldNullFlag(rowPosition.getIsNull(), heapRowVector);
    }

    // Hudi-specific: collapse a present row whose every child is null into a null row, so that a
    // SQL value like `row(null, null)` round-trips to NULL on read. This was the behaviour of the
    // legacy RowColumnReader (deleted alongside the Dremel rewire) and existing Hudi tables rely
    // on it. Diverges from Flink 2.1, which would surface it as Row(null, null). Pinned by the
    // integration test ITTestHoodieDataSource#testParquetNullChildColumnsRowTypes.
    int rowCount = rowPosition.getPositionsCount();
    for (int j = 0; j < rowCount; j++) {
      if (heapRowVector.isNullAt(j)) {
        continue;
      }
      boolean allChildrenNull = true;
      for (WritableColumnVector child : finalChildrenVectors) {
        if (!child.isNullAt(j)) {
          allChildrenNull = false;
          break;
        }
      }
      if (allChildrenNull) {
        heapRowVector.setNullAt(j);
      }
    }
    return Tuple2.of(levelDelegation, heapRowVector);
  }

  private Tuple2<LevelDelegation, WritableColumnVector> readMap(
      ParquetGroupField field, int readNumber, ColumnVector vector, boolean inside)
      throws IOException {
    HeapMapColumnVector mapVector = (HeapMapColumnVector) vector;
    mapVector.reset();
    List<ParquetField> children = field.getChildren();
    Preconditions.checkArgument(
        children.size() == 2,
        "Maps must have two type parameters, found %s",
        children.size());
    Tuple2<LevelDelegation, WritableColumnVector> keyTuple =
        readData(children.get(0), readNumber, mapVector.getKeyColumnVector(), true);
    Tuple2<LevelDelegation, WritableColumnVector> valueTuple =
        readData(children.get(1), readNumber, mapVector.getValueColumnVector(), true);

    LevelDelegation levelDelegation = keyTuple.f0;

    CollectionPosition collectionPosition =
        NestedPositionUtil.calculateCollectionOffsets(
            field,
            levelDelegation.getDefinitionLevel(),
            levelDelegation.getRepetitionLevel());

    // If map was inside the structure, then we need to renew the vector to reset the
    // capacity.
    if (inside) {
      mapVector = new HeapMapColumnVector(collectionPosition.getValueCount(), keyTuple.f1, valueTuple.f1);
    } else {
      mapVector.setKeys(keyTuple.f1);
      mapVector.setValues(valueTuple.f1);
    }

    if (collectionPosition.getIsNull() != null) {
      setFieldNullFlag(collectionPosition.getIsNull(), mapVector);
    }

    mapVector.setLengths(collectionPosition.getLength());
    mapVector.setOffsets(collectionPosition.getOffsets());

    return Tuple2.of(levelDelegation, mapVector);
  }

  private Tuple2<LevelDelegation, WritableColumnVector> readArray(
      ParquetGroupField field, int readNumber, ColumnVector vector, boolean inside)
      throws IOException {
    HeapArrayVector arrayVector = (HeapArrayVector) vector;
    arrayVector.reset();
    List<ParquetField> children = field.getChildren();
    Preconditions.checkArgument(
        children.size() == 1,
        "Arrays must have a single type parameter, found %s",
        children.size());
    Tuple2<LevelDelegation, WritableColumnVector> tuple =
        readData(children.get(0), readNumber, arrayVector.getChild(), true);

    LevelDelegation levelDelegation = tuple.f0;
    CollectionPosition collectionPosition =
        NestedPositionUtil.calculateCollectionOffsets(
            field,
            levelDelegation.getDefinitionLevel(),
            levelDelegation.getRepetitionLevel());

    // If array was inside the structure, then we need to renew the vector to reset the
    // capacity.
    if (inside) {
      arrayVector = new HeapArrayVector(collectionPosition.getValueCount(), tuple.f1);
    } else {
      arrayVector.setChild(tuple.f1);
    }

    if (collectionPosition.getIsNull() != null) {
      setFieldNullFlag(collectionPosition.getIsNull(), arrayVector);
    }
    arrayVector.setLengths(collectionPosition.getLength());
    arrayVector.setOffsets(collectionPosition.getOffsets());
    return Tuple2.of(levelDelegation, arrayVector);
  }

  private Tuple2<LevelDelegation, WritableColumnVector> readPrimitive(
      ParquetPrimitiveField field, int readNumber, ColumnVector vector) throws IOException {
    ColumnDescriptor descriptor = field.getDescriptor();
    NestedPrimitiveColumnReader reader = columnReaders.get(descriptor);
    if (reader == null) {
      reader =
          new NestedPrimitiveColumnReader(
              descriptor,
              pages.getPageReader(descriptor),
              isUtcTimestamp,
              descriptor.getPrimitiveType(),
              field.getType());
      columnReaders.put(descriptor, reader);
    }
    WritableColumnVector writableColumnVector =
        reader.readAndNewVector(readNumber, (WritableColumnVector) vector);
    return Tuple2.of(reader.getLevelDelegation(), writableColumnVector);
  }

  private static void setFieldNullFlag(boolean[] nullFlags, AbstractHeapVector vector) {
    for (int index = 0; index < vector.getLen() && index < nullFlags.length; index++) {
      if (nullFlags[index]) {
        vector.setNullAt(index);
      }
    }
  }
}
