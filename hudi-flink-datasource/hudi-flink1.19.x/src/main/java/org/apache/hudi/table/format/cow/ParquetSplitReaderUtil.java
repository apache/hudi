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

package org.apache.hudi.table.format.cow;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.table.format.cow.vector.HeapArrayVector;
import org.apache.hudi.table.format.cow.vector.HeapDecimalVector;
import org.apache.hudi.table.format.cow.vector.HeapMapColumnVector;
import org.apache.hudi.table.format.cow.vector.HeapRowColumnVector;
import org.apache.hudi.table.format.cow.vector.reader.EmptyColumnReader;
import org.apache.hudi.table.format.cow.vector.reader.FixedLenBytesColumnReader;
import org.apache.hudi.table.format.cow.vector.reader.Int64TimestampColumnReader;
import org.apache.hudi.table.format.cow.vector.reader.NestedColumnReader;
import org.apache.hudi.table.format.cow.vector.reader.ParquetColumnarRowSplitReader;
import org.apache.hudi.table.format.cow.vector.type.ParquetField;
import org.apache.hudi.table.format.cow.vector.type.ParquetGroupField;
import org.apache.hudi.table.format.cow.vector.type.ParquetPrimitiveField;

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.vector.reader.BooleanColumnReader;
import org.apache.flink.formats.parquet.vector.reader.ByteColumnReader;
import org.apache.flink.formats.parquet.vector.reader.BytesColumnReader;
import org.apache.flink.formats.parquet.vector.reader.ColumnReader;
import org.apache.flink.formats.parquet.vector.reader.DoubleColumnReader;
import org.apache.flink.formats.parquet.vector.reader.FloatColumnReader;
import org.apache.flink.formats.parquet.vector.reader.IntColumnReader;
import org.apache.flink.formats.parquet.vector.reader.LongColumnReader;
import org.apache.flink.formats.parquet.vector.reader.ShortColumnReader;
import org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.columnar.vector.heap.HeapBooleanVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapByteVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapDoubleVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapFloatVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapIntVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapLongVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapShortVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapTimestampVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.utils.DateTimeUtils.toInternal;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.parquet.Preconditions.checkArgument;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Util for generating {@link ParquetColumnarRowSplitReader}.
 *
 * <p>Uses the Dremel-style nested reader ported from Apache Flink 2.1 (FLINK-35702). For primitive
 * top-level columns we keep Hudi's specialized readers — {@link Int64TimestampColumnReader},
 * {@link FixedLenBytesColumnReader}, and the Hudi {@link HeapDecimalVector} — unchanged. For
 * nested types (ARRAY / MAP / MULTISET / ROW) we build a {@link ParquetField} tree once per
 * split via {@link #buildFieldsList(List, List, MessageColumnIO)} and delegate reading to
 * {@link NestedColumnReader}.
 *
 * <p>Schema evolution: missing top-level fields are still handled by the caller
 * ({@link ParquetColumnarRowSplitReader} patches them with null vectors). Missing fields
 * <em>inside</em> a Row are handled here — {@link #constructField} returns {@code null} for a
 * child that isn't physically present, and the corresponding child in the pre-allocated vector
 * is filled with nulls via {@link #createVectorFromConstant} so the Dremel assembler can
 * passthrough the slot (see {@link NestedColumnReader#readToVector}).
 */
public class ParquetSplitReaderUtil {

  /** Util for generating partitioned {@link ParquetColumnarRowSplitReader}. */
  public static ParquetColumnarRowSplitReader genPartColumnarRowReader(
      boolean utcTimestamp,
      boolean caseSensitive,
      Configuration conf,
      String[] fullFieldNames,
      DataType[] fullFieldTypes,
      Map<String, Object> partitionSpec,
      int[] selectedFields,
      int batchSize,
      Path path,
      long splitStart,
      long splitLength,
      FilterPredicate filterPredicate,
      UnboundRecordFilter recordFilter) throws IOException {

    ValidationUtils.checkState(Arrays.stream(selectedFields).noneMatch(x -> x == -1),
        "One or more specified columns does not exist in the hudi table.");

    List<String> selNonPartNames = Arrays.stream(selectedFields)
        .mapToObj(i -> fullFieldNames[i])
        .filter(n -> !partitionSpec.containsKey(n))
        .collect(Collectors.toList());

    int[] selParquetFields = Arrays.stream(selectedFields)
        .filter(i -> !partitionSpec.containsKey(fullFieldNames[i]))
        .toArray();

    ParquetColumnarRowSplitReader.ColumnBatchGenerator gen = readVectors -> {
      // create and initialize the row batch
      ColumnVector[] vectors = new ColumnVector[selectedFields.length];
      for (int i = 0; i < vectors.length; i++) {
        String name = fullFieldNames[selectedFields[i]];
        LogicalType type = fullFieldTypes[selectedFields[i]].getLogicalType();
        vectors[i] = createVector(readVectors, selNonPartNames, name, type, partitionSpec, batchSize);
      }
      return new VectorizedColumnBatch(vectors);
    };

    return new ParquetColumnarRowSplitReader(
        utcTimestamp,
        caseSensitive,
        conf,
        Arrays.stream(selParquetFields)
            .mapToObj(i -> fullFieldTypes[i].getLogicalType())
            .toArray(LogicalType[]::new),
        selNonPartNames.toArray(new String[0]),
        gen,
        batchSize,
        new org.apache.hadoop.fs.Path(path.toUri()),
        splitStart,
        splitLength,
        filterPredicate,
        recordFilter);
  }

  private static ColumnVector createVector(
      ColumnVector[] readVectors,
      List<String> selNonPartNames,
      String name,
      LogicalType type,
      Map<String, Object> partitionSpec,
      int batchSize) {
    if (partitionSpec.containsKey(name)) {
      return createVectorFromConstant(type, partitionSpec.get(name), batchSize);
    }
    ColumnVector readVector = readVectors[selNonPartNames.indexOf(name)];
    if (readVector == null) {
      // when the read vector is null, use a constant null vector instead
      readVector = createVectorFromConstant(type, null, batchSize);
    }
    return readVector;
  }

  /**
   * Builds a constant-filled column vector for either a partition column (non-null value) or a
   * missing-column slot (null value). Used both at the batch-generator level for partition
   * injection and at the row-reader level for fields absent from the Parquet file.
   */
  public static ColumnVector createVectorFromConstant(
      LogicalType type, Object value, int batchSize) {
    switch (type.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
        HeapBytesVector bsv = new HeapBytesVector(batchSize);
        if (value == null) {
          bsv.fillWithNulls();
        } else {
          bsv.fill(value instanceof byte[]
              ? (byte[]) value
              : getUTF8Bytes(value.toString()));
        }
        return bsv;
      case BOOLEAN:
        HeapBooleanVector bv = new HeapBooleanVector(batchSize);
        if (value == null) {
          bv.fillWithNulls();
        } else {
          bv.fill((boolean) value);
        }
        return bv;
      case TINYINT:
        HeapByteVector byteVector = new HeapByteVector(batchSize);
        if (value == null) {
          byteVector.fillWithNulls();
        } else {
          byteVector.fill(((Number) value).byteValue());
        }
        return byteVector;
      case SMALLINT:
        HeapShortVector sv = new HeapShortVector(batchSize);
        if (value == null) {
          sv.fillWithNulls();
        } else {
          sv.fill(((Number) value).shortValue());
        }
        return sv;
      case INTEGER:
        HeapIntVector iv = new HeapIntVector(batchSize);
        if (value == null) {
          iv.fillWithNulls();
        } else {
          iv.fill(((Number) value).intValue());
        }
        return iv;
      case BIGINT:
        HeapLongVector lv = new HeapLongVector(batchSize);
        if (value == null) {
          lv.fillWithNulls();
        } else {
          lv.fill(((Number) value).longValue());
        }
        return lv;
      case DECIMAL:
        HeapDecimalVector decv = new HeapDecimalVector(batchSize);
        if (value == null) {
          decv.fillWithNulls();
        } else {
          DecimalType decimalType = (DecimalType) type;
          int precision = decimalType.getPrecision();
          int scale = decimalType.getScale();
          DecimalData decimal = Preconditions.checkNotNull(
              DecimalData.fromBigDecimal((BigDecimal) value, precision, scale));
          decv.fill(decimal.toUnscaledBytes());
        }
        return decv;
      case FLOAT:
        HeapFloatVector fv = new HeapFloatVector(batchSize);
        if (value == null) {
          fv.fillWithNulls();
        } else {
          fv.fill(((Number) value).floatValue());
        }
        return fv;
      case DOUBLE:
        HeapDoubleVector dv = new HeapDoubleVector(batchSize);
        if (value == null) {
          dv.fillWithNulls();
        } else {
          dv.fill(((Number) value).doubleValue());
        }
        return dv;
      case DATE:
        if (value instanceof LocalDate) {
          value = Date.valueOf((LocalDate) value);
        }
        return createVectorFromConstant(
            new IntType(),
            value == null ? null : toInternal((Date) value),
            batchSize);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        HeapTimestampVector tv = new HeapTimestampVector(batchSize);
        if (value == null) {
          tv.fillWithNulls();
        } else {
          tv.fill(TimestampData.fromLocalDateTime((LocalDateTime) value));
        }
        return tv;
      case ARRAY:
        if (value != null) {
          throw new UnsupportedOperationException("Unsupported create array with default value.");
        }
        HeapArrayVector arrayVector = new HeapArrayVector(batchSize);
        arrayVector.fillWithNulls();
        return arrayVector;
      case MAP:
      case MULTISET:
        if (value != null) {
          throw new UnsupportedOperationException(
              "Unsupported create " + type.getTypeRoot() + " with default value.");
        }
        HeapMapColumnVector mapVector = new HeapMapColumnVector(batchSize, null, null);
        mapVector.fillWithNulls();
        return mapVector;
      case ROW:
        if (value != null) {
          throw new UnsupportedOperationException("Unsupported create row with default value.");
        }
        RowType rowType = (RowType) type;
        WritableColumnVector[] childVectors = new WritableColumnVector[rowType.getFieldCount()];
        for (int i = 0; i < childVectors.length; i++) {
          childVectors[i] =
              (WritableColumnVector) createVectorFromConstant(rowType.getTypeAt(i), null, batchSize);
        }
        HeapRowColumnVector rowVector = new HeapRowColumnVector(batchSize, childVectors);
        rowVector.fillWithNulls();
        return rowVector;
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  private static List<ColumnDescriptor> filterDescriptors(
      int depth, Type type, List<ColumnDescriptor> columns) throws ParquetRuntimeException {
    List<ColumnDescriptor> filtered = new ArrayList<>();
    for (ColumnDescriptor descriptor : columns) {
      if (depth >= descriptor.getPath().length) {
        throw new InvalidSchemaException("Expect depth " + depth + " for schema: " + descriptor);
      }
      if (type.getName().equals(descriptor.getPath()[depth])) {
        filtered.add(descriptor);
      }
    }
    ValidationUtils.checkState(filtered.size() > 0, "Corrupted Parquet schema");
    return filtered;
  }

  /**
   * Creates a {@link ColumnReader} for one top-level requested field. For primitive types the
   * Hudi-specialized reader path is used. For nested types ({@code ARRAY}, {@code MAP},
   * {@code MULTISET}, {@code ROW}) the Dremel-style {@link NestedColumnReader} is used, driven by
   * the supplied pre-built {@link ParquetField} tree.
   *
   * @param field the {@link ParquetField} tree for this column, built by
   *     {@link #buildFieldsList(List, List, MessageColumnIO)}. Required (non-null) for nested
   *     types; ignored for primitives.
   */
  public static ColumnReader createColumnReader(
      boolean utcTimestamp,
      LogicalType fieldType,
      Type physicalType,
      List<ColumnDescriptor> descriptors,
      PageReadStore pages,
      @Nullable ParquetField field) throws IOException {
    switch (fieldType.getTypeRoot()) {
      case ARRAY:
      case MAP:
      case MULTISET:
      case ROW:
        Preconditions.checkNotNull(
            field, "ParquetField must be provided for nested type: %s", fieldType);
        return new NestedColumnReader(utcTimestamp, pages, field);
      default:
        return createPrimitiveColumnReader(utcTimestamp, fieldType, physicalType, descriptors, pages);
    }
  }

  /**
   * Backward-compat entry point kept for callers that don't project nested types and therefore
   * never need a {@link ParquetField} tree. Forwards to the {@link ParquetField}-aware overload
   * with a null field; nested types now go through that overload directly.
   *
   * @deprecated use {@link #createColumnReader(boolean, LogicalType, Type, List, PageReadStore,
   *     ParquetField)} so nested types take the Dremel path.
   */
  @Deprecated
  public static ColumnReader createColumnReader(
      boolean utcTimestamp,
      LogicalType fieldType,
      Type physicalType,
      List<ColumnDescriptor> descriptors,
      PageReadStore pages) throws IOException {
    return createColumnReader(utcTimestamp, fieldType, physicalType, descriptors, pages, null);
  }

  private static ColumnReader createPrimitiveColumnReader(
      boolean utcTimestamp,
      LogicalType fieldType,
      Type physicalType,
      List<ColumnDescriptor> columns,
      PageReadStore pages) throws IOException {
    List<ColumnDescriptor> descriptors = filterDescriptors(0, physicalType, columns);
    ColumnDescriptor descriptor = descriptors.get(0);
    PageReader pageReader = pages.getPageReader(descriptor);
    switch (fieldType.getTypeRoot()) {
      case BOOLEAN:
        return new BooleanColumnReader(descriptor, pageReader);
      case TINYINT:
        return new ByteColumnReader(descriptor, pageReader);
      case DOUBLE:
        return new DoubleColumnReader(descriptor, pageReader);
      case FLOAT:
        return new FloatColumnReader(descriptor, pageReader);
      case INTEGER:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
        return new IntColumnReader(descriptor, pageReader);
      case BIGINT:
        return new LongColumnReader(descriptor, pageReader);
      case SMALLINT:
        return new ShortColumnReader(descriptor, pageReader);
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
        return new BytesColumnReader(descriptor, pageReader);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
          case INT64:
            int precision = fieldType instanceof TimestampType
                ? ((TimestampType) fieldType).getPrecision()
                : ((LocalZonedTimestampType) fieldType).getPrecision();
            return new Int64TimestampColumnReader(utcTimestamp, descriptor, pageReader, precision);
          case INT96:
            return new TimestampColumnReader(utcTimestamp, descriptor, pageReader);
          default:
            throw new AssertionError(
                "Unexpected physical type for TIMESTAMP: "
                    + descriptor.getPrimitiveType().getPrimitiveTypeName());
        }
      case DECIMAL:
        switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
          case INT32:
            return new IntColumnReader(descriptor, pageReader);
          case INT64:
            return new LongColumnReader(descriptor, pageReader);
          case BINARY:
            return new BytesColumnReader(descriptor, pageReader);
          case FIXED_LEN_BYTE_ARRAY:
            return new FixedLenBytesColumnReader(descriptor, pageReader);
          default:
            throw new AssertionError(
                "Unexpected physical type for DECIMAL: "
                    + descriptor.getPrimitiveType().getPrimitiveTypeName());
        }
      default:
        throw new UnsupportedOperationException(fieldType + " is not supported now.");
    }
  }

  /**
   * Creates the writable column vector that the reader will write into. The returned vector shape
   * matches {@code fieldType}; for ROW types missing physical fields are slotted with null-filled
   * vectors (sourced from {@link #createVectorFromConstant}) so that the Dremel assembler in
   * {@link NestedColumnReader} can pass them through unchanged.
   */
  public static WritableColumnVector createWritableColumnVector(
      int batchSize,
      LogicalType fieldType,
      Type physicalType,
      List<ColumnDescriptor> descriptors) {
    return createWritableColumnVector(batchSize, fieldType, physicalType, descriptors, 0);
  }

  private static WritableColumnVector createWritableColumnVector(
      int batchSize,
      LogicalType fieldType,
      Type physicalType,
      List<ColumnDescriptor> columns,
      int depth) {
    List<ColumnDescriptor> descriptors = filterDescriptors(depth, physicalType, columns);
    PrimitiveType primitiveType = descriptors.get(0).getPrimitiveType();
    PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
    switch (fieldType.getTypeRoot()) {
      case BOOLEAN:
        checkArgument(
            typeName == PrimitiveType.PrimitiveTypeName.BOOLEAN,
            getPrimitiveTypeCheckFailureMessage(typeName, fieldType));
        return new HeapBooleanVector(batchSize);
      case TINYINT:
        checkArgument(
            typeName == PrimitiveType.PrimitiveTypeName.INT32,
            getPrimitiveTypeCheckFailureMessage(typeName, fieldType));
        return new HeapByteVector(batchSize);
      case DOUBLE:
        checkArgument(
            typeName == PrimitiveType.PrimitiveTypeName.DOUBLE,
            getPrimitiveTypeCheckFailureMessage(typeName, fieldType));
        return new HeapDoubleVector(batchSize);
      case FLOAT:
        checkArgument(
            typeName == PrimitiveType.PrimitiveTypeName.FLOAT,
            getPrimitiveTypeCheckFailureMessage(typeName, fieldType));
        return new HeapFloatVector(batchSize);
      case INTEGER:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
        checkArgument(
            typeName == PrimitiveType.PrimitiveTypeName.INT32,
            getPrimitiveTypeCheckFailureMessage(typeName, fieldType));
        return new HeapIntVector(batchSize);
      case BIGINT:
        checkArgument(
            typeName == PrimitiveType.PrimitiveTypeName.INT64,
            getPrimitiveTypeCheckFailureMessage(typeName, fieldType));
        return new HeapLongVector(batchSize);
      case SMALLINT:
        checkArgument(
            typeName == PrimitiveType.PrimitiveTypeName.INT32,
            getPrimitiveTypeCheckFailureMessage(typeName, fieldType));
        return new HeapShortVector(batchSize);
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
        checkArgument(
            typeName == PrimitiveType.PrimitiveTypeName.BINARY,
            getPrimitiveTypeCheckFailureMessage(typeName, fieldType));
        return new HeapBytesVector(batchSize);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        checkArgument(primitiveType.getOriginalType() != OriginalType.TIME_MICROS,
            getOriginalTypeCheckFailureMessage(primitiveType.getOriginalType(), fieldType));
        return new HeapTimestampVector(batchSize);
      case DECIMAL:
        checkArgument(
            (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                    || typeName == PrimitiveType.PrimitiveTypeName.BINARY)
                && primitiveType.getOriginalType() == OriginalType.DECIMAL,
            getPrimitiveTypeCheckFailureMessage(typeName, fieldType));
        return new HeapDecimalVector(batchSize);
      case ARRAY:
        ArrayType arrayType = (ArrayType) fieldType;
        return new HeapArrayVector(
            batchSize,
            createWritableColumnVector(
                batchSize, arrayType.getElementType(), physicalType, descriptors, depth));
      case MAP: {
        MapType mapType = (MapType) fieldType;
        GroupType repeatedType = unwrapMapRepeatedType(physicalType);
        return new HeapMapColumnVector(
            batchSize,
            createWritableColumnVector(
                batchSize, mapType.getKeyType(), repeatedType.getType(0), descriptors, depth + 2),
            createWritableColumnVector(
                batchSize, mapType.getValueType(), repeatedType.getType(1), descriptors, depth + 2));
      }
      case MULTISET: {
        MultisetType multisetType = (MultisetType) fieldType;
        GroupType repeatedType = unwrapMapRepeatedType(physicalType);
        return new HeapMapColumnVector(
            batchSize,
            createWritableColumnVector(
                batchSize,
                multisetType.getElementType(),
                repeatedType.getType(0),
                descriptors,
                depth + 2),
            createWritableColumnVector(
                batchSize,
                new IntType(false),
                repeatedType.getType(1),
                descriptors,
                depth + 2));
      }
      case ROW:
        RowType rowType = (RowType) fieldType;
        GroupType groupType = physicalType.asGroupType();
        WritableColumnVector[] columnVectors = new WritableColumnVector[rowType.getFieldCount()];
        for (int i = 0; i < columnVectors.length; i++) {
          int fieldIndex = getFieldIndexInPhysicalType(rowType.getFields().get(i).getName(), groupType);
          if (fieldIndex < 0) {
            // Schema evolution: logical field is absent from the Parquet file. Slot a null-filled
            // vector of the correct shape; NestedColumnReader.readRow will pass it through when the
            // matching ParquetField child is null.
            columnVectors[i] =
                (WritableColumnVector) createVectorFromConstant(rowType.getTypeAt(i), null, batchSize);
          } else {
            columnVectors[i] =
                createWritableColumnVector(
                    batchSize,
                    rowType.getTypeAt(i),
                    groupType.getType(fieldIndex),
                    descriptors,
                    depth + 1);
          }
        }
        return new HeapRowColumnVector(batchSize, columnVectors);
      default:
        throw new UnsupportedOperationException(fieldType + " is not supported now.");
    }
  }

  /**
   * Peels one {@code repeated group key_value} wrapper off a MAP / MULTISET physical type, matching
   * Parquet's canonical 3-level map encoding.
   */
  private static GroupType unwrapMapRepeatedType(Type physicalType) {
    return physicalType.asGroupType().getType(0).asGroupType();
  }

  // ------------------------------------------------------------------------------------------
  // ParquetField tree construction (vendored from Apache Flink 2.1 ParquetSplitReaderUtil)
  //
  // The only Hudi-specific divergence is in `constructField`: the ROW branch tolerates children
  // missing from the Parquet file by emitting a null ParquetField child (upstream throws). This
  // matches the Hudi schema-evolution contract and is the companion to the null-child branch in
  // `NestedColumnReader#readRow` and the null-vector slot in `createWritableColumnVector#ROW`.
  // ------------------------------------------------------------------------------------------

  /**
   * Builds {@link ParquetField} trees — one per top-level projected logical column — that feed
   * {@link NestedColumnReader}. The returned list mirrors the input {@code children} positionally;
   * primitive top-level fields produce {@code null} entries (callers don't need a tree for those).
   */
  public static List<ParquetField> buildFieldsList(
      List<RowType.RowField> children, List<String> fieldNames, MessageColumnIO columnIO) {
    List<ParquetField> list = new ArrayList<>();
    for (int i = 0; i < children.size(); i++) {
      RowType.RowField child = children.get(i);
      if (isNestedType(child.getType())) {
        list.add(constructField(child, lookupColumnByName(columnIO, fieldNames.get(i))));
      } else {
        list.add(null);
      }
    }
    return list;
  }

  private static boolean isNestedType(LogicalType type) {
    return type instanceof RowType
        || type instanceof ArrayType
        || type instanceof MapType
        || type instanceof MultisetType;
  }

  @Nullable
  private static ParquetField constructField(RowType.RowField rowField, ColumnIO columnIO) {
    boolean required = columnIO.getType().getRepetition() == REQUIRED;
    int repetitionLevel = columnIO.getRepetitionLevel();
    int definitionLevel = columnIO.getDefinitionLevel();
    LogicalType type = rowField.getType();
    String fieldName = rowField.getName();
    if (type instanceof RowType) {
      GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
      RowType rowType = (RowType) type;
      List<RowType.RowField> childFields = rowType.getFields();
      List<ParquetField> fieldsList = new ArrayList<>(childFields.size());
      for (RowType.RowField childField : childFields) {
        // Hudi schema evolution: a logical child may be absent from the Parquet file. In that
        // case we emit a null ParquetField so that NestedColumnReader.readRow passes through the
        // pre-filled null vector instead of recursing.
        ColumnIO childIo = lookupColumnByNameOrNull(groupColumnIO, childField.getName());
        if (childIo == null) {
          fieldsList.add(null);
        } else {
          fieldsList.add(constructField(childField, childIo));
        }
      }
      return new ParquetGroupField(
          type,
          repetitionLevel,
          definitionLevel,
          required,
          Collections.unmodifiableList(fieldsList));
    }

    if (type instanceof MapType) {
      GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
      GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
      MapType mapType = (MapType) type;
      ParquetField keyField =
          constructField(
              new RowType.RowField("", mapType.getKeyType()), keyValueColumnIO.getChild(0));
      ParquetField valueField =
          constructField(
              new RowType.RowField("", mapType.getValueType()), keyValueColumnIO.getChild(1));
      return new ParquetGroupField(
          type,
          repetitionLevel,
          definitionLevel,
          required,
          Collections.unmodifiableList(Arrays.asList(keyField, valueField)));
    }

    if (type instanceof MultisetType) {
      GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
      GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
      MultisetType multisetType = (MultisetType) type;
      ParquetField keyField =
          constructField(
              new RowType.RowField("", multisetType.getElementType()),
              keyValueColumnIO.getChild(0));
      ParquetField valueField =
          constructField(
              new RowType.RowField("", new IntType()), keyValueColumnIO.getChild(1));
      return new ParquetGroupField(
          type,
          repetitionLevel,
          definitionLevel,
          required,
          Collections.unmodifiableList(Arrays.asList(keyField, valueField)));
    }

    if (type instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) type;
      ColumnIO elementTypeColumnIO;
      if (columnIO instanceof GroupColumnIO) {
        GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
        if (!StringUtils.isNullOrWhitespaceOnly(fieldName)) {
          while (!Objects.equals(groupColumnIO.getName(), fieldName)) {
            groupColumnIO = (GroupColumnIO) groupColumnIO.getChild(0);
          }
          elementTypeColumnIO = groupColumnIO;
        } else {
          if (arrayType.getElementType() instanceof RowType) {
            elementTypeColumnIO = groupColumnIO;
          } else {
            elementTypeColumnIO = groupColumnIO.getChild(0);
          }
        }
      } else if (columnIO instanceof PrimitiveColumnIO) {
        elementTypeColumnIO = columnIO;
      } else {
        throw new FlinkRuntimeException(String.format("Unknown ColumnIO, %s", columnIO));
      }

      ParquetField elementField =
          constructField(
              new RowType.RowField("", arrayType.getElementType()),
              getArrayElementColumn(elementTypeColumnIO));
      if (repetitionLevel == elementField.getRepetitionLevel()) {
        repetitionLevel = columnIO.getParent().getRepetitionLevel();
      }
      return new ParquetGroupField(
          type,
          repetitionLevel,
          definitionLevel,
          required,
          Collections.singletonList(elementField));
    }

    PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
    return new ParquetPrimitiveField(
        type, required, primitiveColumnIO.getColumnDescriptor(), primitiveColumnIO.getId());
  }

  /**
   * Parquet column names are case-insensitive in Flink's lookup. Matches upstream
   * {@code ParquetSplitReaderUtil.lookupColumnByName}; throws when absent.
   */
  public static ColumnIO lookupColumnByName(GroupColumnIO groupColumnIO, String columnName) {
    ColumnIO columnIO = lookupColumnByNameOrNull(groupColumnIO, columnName);
    if (columnIO != null) {
      return columnIO;
    }
    throw new FlinkRuntimeException(
        "Can not find column io for parquet reader. Column name: " + columnName);
  }

  /**
   * Case-insensitive column lookup that returns {@code null} when no match is found — the
   * Hudi-specific companion to {@link #lookupColumnByName}, used by {@link #constructField} to
   * emit null {@link ParquetField} children for fields absent from the Parquet file.
   */
  @Nullable
  private static ColumnIO lookupColumnByNameOrNull(
      GroupColumnIO groupColumnIO, String columnName) {
    ColumnIO columnIO = groupColumnIO.getChild(columnName);
    if (columnIO != null) {
      return columnIO;
    }
    for (int i = 0; i < groupColumnIO.getChildrenCount(); i++) {
      if (groupColumnIO.getChild(i).getName().equalsIgnoreCase(columnName)) {
        return groupColumnIO.getChild(i);
      }
    }
    return null;
  }

  public static GroupColumnIO getMapKeyValueColumn(GroupColumnIO groupColumnIO) {
    while (groupColumnIO.getChildrenCount() == 1) {
      groupColumnIO = (GroupColumnIO) groupColumnIO.getChild(0);
    }
    return groupColumnIO;
  }

  public static ColumnIO getArrayElementColumn(ColumnIO columnIO) {
    while (columnIO instanceof GroupColumnIO && !columnIO.getType().isRepetition(REPEATED)) {
      columnIO = ((GroupColumnIO) columnIO).getChild(0);
    }

    // Three-level list: skip the synthetic `element` / `list` wrapper when present.
    if (columnIO instanceof GroupColumnIO
        && columnIO.getType().getLogicalTypeAnnotation() == null
        && ((GroupColumnIO) columnIO).getChildrenCount() == 1
        && !columnIO.getName().equals("array")
        && !columnIO.getName().equals(columnIO.getParent().getName() + "_tuple")) {
      return ((GroupColumnIO) columnIO).getChild(0);
    }
    return columnIO;
  }

  /**
   * Returns the field index with given physical row type {@code groupType} and field name
   * {@code fieldName}.
   *
   * @return the physical field index or -1 if the field does not exist
   */
  private static int getFieldIndexInPhysicalType(String fieldName, GroupType groupType) {
    return groupType.containsField(fieldName) ? groupType.getFieldIndex(fieldName) : -1;
  }

  private static String getPrimitiveTypeCheckFailureMessage(
      PrimitiveType.PrimitiveTypeName primitiveType, LogicalType fieldType) {
    return String.format(
        "Unexpected type exception. Primitive type: %s. Field type: %s.",
        primitiveType, fieldType.getTypeRoot().name());
  }

  private static String getOriginalTypeCheckFailureMessage(
      OriginalType originalType, LogicalType fieldType) {
    return String.format(
        "Unexpected type exception. Original type: %s. Field type: %s.",
        originalType, fieldType.getTypeRoot().name());
  }

  /**
   * Returns a synthetic null-column reader to fill missing top-level fields. Kept as a convenience
   * for callers that need to mirror Hudi's original behaviour where a missing column produces an
   * explicit null-valued reader rather than being omitted from the batch.
   */
  public static ColumnReader<WritableColumnVector> emptyColumnReader() {
    return new EmptyColumnReader();
  }
}
