/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.hudi.client.utils.SparkInternalSchemaConverter;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.Types.Field;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

public class Spark31HoodieVectorizedParquetRecordReader extends VectorizedParquetRecordReader {

  // save the col type change info.
  private final Map<Integer, Pair<DataType, DataType>> typeChangeInfos;

  private ColumnarBatch columnarBatch;

  private Map<Integer, WritableColumnVector> idToColumnVectors;

  private WritableColumnVector[] columnVectors;

  // The capacity of vectorized batch.
  private final int capacity;

  // If true, this class returns batches instead of rows.
  private boolean returnColumnarBatch;

  // The memory mode of the columnarBatch.
  private final MemoryMode memoryMode;

  /**
   * Batch of rows that we assemble and the current index we've returned. Every time this
   * batch is used up (batchIdx == numBatched), we populated the batch.
   */
  private int batchIdx = 0;
  private int numBatched = 0;

  // The schema of vectorized batch.
  private final InternalSchema schema;

  private final Map<Integer, String> missingColumns = new HashMap<>();

  public Spark31HoodieVectorizedParquetRecordReader(
      ZoneId convertTz,
      String datetimeRebaseMode,
      String int96RebaseMode,
      boolean useOffHeap,
      int capacity,
      Map<Integer, Pair<DataType, DataType>> typeChangeInfos,
      InternalSchema schema) {
    super(convertTz, datetimeRebaseMode, int96RebaseMode, useOffHeap, capacity);
    memoryMode = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
    this.typeChangeInfos = typeChangeInfos;
    this.capacity = capacity;
    this.schema = schema;
  }

  @Override
  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
    super.initBatch(partitionColumns, partitionValues);
    if (columnVectors == null) {
      columnVectors = new WritableColumnVector[sparkSchema.length() + partitionColumns.length()];
    }
    if (idToColumnVectors == null) {
      idToColumnVectors = new HashMap<>();
      typeChangeInfos.forEach((key, value) -> {
        WritableColumnVector vector =
            memoryMode == MemoryMode.OFF_HEAP ? new OffHeapColumnVector(capacity, value.getLeft()) : new OnHeapColumnVector(capacity, value.getLeft());
        idToColumnVectors.put(key, vector);
      });
    }
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException, UnsupportedOperationException {
    super.initialize(inputSplit, taskAttemptContext);
    initializeInternal();
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
    if (idToColumnVectors != null) {
      idToColumnVectors.forEach((key, value) -> value.close());
      idToColumnVectors = null;
    }
    columnVectors = null;
  }

  @Override
  public ColumnarBatch resultBatch() {
    ColumnarBatch currentColumnBatch = super.resultBatch();
    boolean changed = false;
    for (Map.Entry<Integer, Pair<DataType, DataType>> entry : typeChangeInfos.entrySet()) {
      boolean rewrite = SparkInternalSchemaConverter
          .convertColumnVectorType((WritableColumnVector) currentColumnBatch.column(entry.getKey()),
              idToColumnVectors.get(entry.getKey()), currentColumnBatch.numRows());
      if (rewrite) {
        changed = true;
        columnVectors[entry.getKey()] = idToColumnVectors.get(entry.getKey());
      }
    }
    if (changed || !missingColumns.isEmpty()) {
      if (columnarBatch == null) {
        // fill other vector
        for (int i = 0; i < columnVectors.length; i++) {
          if (columnVectors[i] == null) {
            columnVectors[i] = (WritableColumnVector) currentColumnBatch.column(i);
          }
        }
        columnarBatch = new ColumnarBatch(columnVectors);
      }
      // Set missing column with default value.
      missingColumns.keySet().forEach(this::setColumnDefaultValue);
      columnarBatch.setNumRows(currentColumnBatch.numRows());
      return columnarBatch;
    } else {
      return currentColumnBatch;
    }
  }

  @Override
  public boolean nextBatch() throws IOException {
    boolean result = super.nextBatch();
    if (idToColumnVectors != null) {
      idToColumnVectors.forEach((key, value) -> value.reset());
    }
    numBatched = resultBatch().numRows();
    batchIdx = 0;
    return result;
  }

  @Override
  public void enableReturningBatches() {
    returnColumnarBatch = true;
    super.enableReturningBatches();
  }

  @Override
  public Object getCurrentValue() {
    if (typeChangeInfos == null || typeChangeInfos.isEmpty()) {
      return super.getCurrentValue();
    }

    if (returnColumnarBatch) {
      return columnarBatch == null ? super.getCurrentValue() : columnarBatch;
    }

    return columnarBatch == null ? super.getCurrentValue() : columnarBatch.getRow(batchIdx - 1);
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    resultBatch();

    if (returnColumnarBatch) {
      return nextBatch();
    }

    if (batchIdx >= numBatched) {
      if (!nextBatch()) {
        return false;
      }
    }
    ++batchIdx;
    return true;
  }

  private void initializeInternal() {
    if (schema != null) {
      List<String[]> columnPaths = requestedSchema.getPaths();
      for (int i = 0; i < requestedSchema.getFieldCount(); i++) {
        String[] columnPath = columnPaths.get(i);
        if (!fileSchema.containsPath(columnPath)) {
          String fieldName = requestedSchema.getFields().get(i).getName();
          Field field = schema.findField(fieldName);
          if (Objects.nonNull(field) && Objects.nonNull(field.getDefaultValue())) {
            missingColumns.put(i, fieldName);
          }
        }
      }
    }
  }

  private void setColumnDefaultValue(int columnIndex) {
    WritableColumnVector columnVector = (WritableColumnVector) columnarBatch.column(columnIndex);
    Field field = schema.findField(missingColumns.get(columnIndex));
    Object defaultValue = field.getDefaultValue();
    switch (field.type().typeId()) {
      case BOOLEAN:
        columnVector.putBooleans(0, capacity, (Boolean) defaultValue);
        break;
      case INT:
        columnVector.putInts(0, capacity, (Integer) defaultValue);
        break;
      case LONG:
        columnVector.putLongs(0, capacity, (Long) defaultValue);
        break;
      case FLOAT:
        columnVector.putFloats(0, capacity, (Float) defaultValue);
        break;
      case DOUBLE:
        columnVector.putDoubles(0, capacity, (Double) defaultValue);
        break;
      case STRING:
        IntStream.range(0, capacity).forEach(i ->
            columnVector.putByteArray(i, defaultValue.toString().getBytes(StandardCharsets.UTF_8)));
        break;
      case BINARY:
        IntStream.range(0, capacity).forEach(i -> columnVector.putByteArray(i, (byte[]) defaultValue));
        break;
      case DECIMAL:
        IntStream.range(0, capacity).forEach(i ->
            columnVector.putDecimal(i, Decimal.fromDecimal(defaultValue), ((Types.DecimalType) field.type()).precision()));
        break;
      default:
        return;
    }
    columnVector.putNotNulls(0, capacity);
    columnVector.setIsConstant();
  }
}
