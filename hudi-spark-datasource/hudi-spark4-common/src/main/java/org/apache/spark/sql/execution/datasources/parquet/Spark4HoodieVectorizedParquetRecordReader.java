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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;

public class Spark4HoodieVectorizedParquetRecordReader extends SparkBaseHoodieVectorizedParquetRecordReader {

  /**
   * Batch of rows that we assemble and the current index we've returned. Every time this
   * batch is used up (batchIdx == numBatched), we populated the batch.
   */
  private int batchIdx = 0;
  private int numBatched = 0;

  public Spark4HoodieVectorizedParquetRecordReader(
      ZoneId convertTz,
      String datetimeRebaseMode,
      String datetimeRebaseTz,
      String int96RebaseMode,
      String int96RebaseTz,
      boolean useOffHeap,
      int capacity,
      Map<Integer, Pair<DataType, DataType>> typeChangeInfos) {
    super(convertTz, datetimeRebaseMode, datetimeRebaseTz, int96RebaseMode, int96RebaseTz, useOffHeap, capacity, typeChangeInfos);
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException, UnsupportedOperationException {
    super.initialize(inputSplit, taskAttemptContext);
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

