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

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hudi.client.utils.SparkInternalSchemaConverter;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

public class Spark312HoodieVectorizedParquetRecordReader extends VectorizedParquetRecordReader {

  // save the col type change info.
  private Map<Integer, Pair<DataType, DataType>> typeChangeInfos;

  private ColumnarBatch columnarBatch;

  private Map<Integer, WritableColumnVector> idToColumnVectors;

  private WritableColumnVector[] columnVectors;

  // The capacity of vectorized batch.
  private int capacity;

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

  public Spark312HoodieVectorizedParquetRecordReader(
      ZoneId convertTz,
      String datetimeRebaseMode,
      String int96RebaseMode,
      boolean useOffHeap,
      int capacity,
      Map<Integer, Pair<DataType, DataType>> typeChangeInfos) {
    super(convertTz, datetimeRebaseMode, int96RebaseMode, useOffHeap, capacity);
    memoryMode = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
    this.typeChangeInfos = typeChangeInfos;
    this.capacity = capacity;
  }

  @Override
  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
    super.initBatch(partitionColumns, partitionValues);
    if (columnVectors == null) {
      columnVectors = new WritableColumnVector[sparkSchema.length() + partitionColumns.length()];
    }
    if (idToColumnVectors == null) {
      idToColumnVectors = new HashMap<>();
      typeChangeInfos.entrySet()
          .stream()
          .forEach(f -> {
            WritableColumnVector vector =
                memoryMode == MemoryMode.OFF_HEAP ? new OffHeapColumnVector(capacity, f.getValue().getLeft()) : new OnHeapColumnVector(capacity, f.getValue().getLeft());
            idToColumnVectors.put(f.getKey(), vector);
          });
    }
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException, UnsupportedOperationException {
    super.initialize(inputSplit, taskAttemptContext);
  }

  @Override
  public void close() throws IOException {
    super.close();
    for (Map.Entry<Integer, WritableColumnVector> e : idToColumnVectors.entrySet()) {
      e.getValue().close();
    }
    idToColumnVectors = null;
    columnarBatch = null;
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
    if (changed) {
      if (columnarBatch == null) {
        // fill other vector
        for (int i = 0; i < columnVectors.length; i++) {
          if (columnVectors[i] == null) {
            columnVectors[i] = (WritableColumnVector) currentColumnBatch.column(i);
          }
        }
        columnarBatch = new ColumnarBatch(columnVectors);
      }
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
      idToColumnVectors.entrySet().stream().forEach(e -> e.getValue().reset());
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

    if (returnColumnarBatch)  {
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
}
