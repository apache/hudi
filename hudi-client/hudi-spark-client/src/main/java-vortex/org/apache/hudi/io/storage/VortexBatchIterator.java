/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io.storage;

import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.vortex.VortexInputFile;

import dev.vortex.api.Partition;
import dev.vortex.api.Scan;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Iterator that reads a Vortex scan as Spark {@link ColumnarBatch}es, without a copy of the data.
 *
 * <p>A {@link Scan} gives a sequence of {@link Partition}s. Each partition is read as an
 * {@link ArrowReader}, whose {@link VectorSchemaRoot} is used again for each batch. Thus, a batch
 * is valid only until the next call of {@link #hasNext()}.
 *
 * <p>Each batch has the columns of the output schema, then the columns of the partition schema:
 * <ul>
 *   <li>A column of the output schema that the scan gives is a view of its Arrow vector, with the
 *   struct fields of the output type, found by name. A column of unsigned 64-bit integers is a
 *   column of row indices.</li>
 *   <li>A column of the output schema that the scan does not give is null in all rows.</li>
 *   <li>A partition column has the partition value of the file in all rows.</li>
 * </ul>
 *
 * <p>This iterator owns, and closes, the {@link BufferAllocator}, the current {@link ArrowReader},
 * the {@link Scan} and the {@link VortexInputFile}.
 */
public final class VortexBatchIterator implements ClosableIterator<ColumnarBatch> {

  // Same as the batch size of the Spark readers for Parquet and ORC.
  private static final int CONSTANT_BATCH_SIZE = 4096;

  private final BufferAllocator allocator;
  private final VortexInputFile file;
  private final Scan scan;
  private final StructField[] outputFields;
  private final StructField[] partitionFields;
  private final InternalRow partitionValues;
  private final String path;

  private ArrowReader currentReader;
  private ColumnarBatch nextBatch;
  private boolean closed = false;

  public VortexBatchIterator(BufferAllocator allocator,
                             VortexInputFile file,
                             Scan scan,
                             StructType outputSchema,
                             StructType partitionSchema,
                             InternalRow partitionValues) {
    this.allocator = allocator;
    this.file = file;
    this.scan = scan;
    this.outputFields = outputSchema.fields();
    this.partitionFields = partitionSchema.fields();
    this.partitionValues = partitionValues;
    this.path = file.path();
  }

  /**
   * Returns batches for a scan that reads no column of the file, for example {@code count(*)}.
   * The columns of the output schema are null, and the partition columns have the partition
   * values of the file. The batches have {@code numRows} rows in total.
   *
   * <p>All batches are the same object, thus a batch is valid only until the next call of
   * {@link Iterator#next()}. The batches hold no off-heap memory.
   */
  public static Iterator<ColumnarBatch> constantBatches(StructType outputSchema,
                                                        StructType partitionSchema,
                                                        InternalRow partitionValues,
                                                        long numRows) {
    int batchSize = (int) Math.min(numRows, CONSTANT_BATCH_SIZE);
    StructField[] outputFields = outputSchema.fields();
    StructField[] partitionFields = partitionSchema.fields();

    ColumnVector[] vectors = new ColumnVector[outputFields.length + partitionFields.length];
    for (int i = 0; i < outputFields.length; i++) {
      vectors[i] = VortexArrowColumnVector.nulls(outputFields[i].dataType(), batchSize);
    }
    for (int i = 0; i < partitionFields.length; i++) {
      vectors[outputFields.length + i] =
          new VortexPartitionColumnVector(partitionFields[i].dataType(), partitionValues, i);
    }
    ColumnarBatch batch = new ColumnarBatch(vectors);

    return new Iterator<ColumnarBatch>() {
      private long remaining = numRows;

      @Override
      public boolean hasNext() {
        return remaining > 0;
      }

      @Override
      public ColumnarBatch next() {
        if (remaining <= 0) {
          throw new NoSuchElementException("No more batches");
        }

        int rows = (int) Math.min(remaining, batchSize);
        remaining -= rows;
        batch.setNumRows(rows);
        return batch;
      }
    };
  }

  @Override
  public boolean hasNext() {
    if (nextBatch != null) {
      return true;
    }
    if (closed) {
      return false;
    }

    // Read the current partition, then the next partitions, until a batch has rows.
    while (true) {
      if (currentReader != null) {
        nextBatch = loadNextBatch();
        if (nextBatch != null) {
          return true;
        }
        closeCurrentReader();
      }

      if (!scan.hasNext()) {
        // Release the native resources now. A task can read many files.
        close();
        return false;
      }
      currentReader = scan.next().scanArrow(allocator);
    }
  }

  @Override
  public ColumnarBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more batches in Vortex file: " + path);
    }

    ColumnarBatch batch = nextBatch;
    nextBatch = null;
    return batch;
  }

  /**
   * Loads the next batch that has rows from the current reader. Returns null when the reader has
   * no more batches.
   */
  private ColumnarBatch loadNextBatch() {
    try {
      while (currentReader.loadNextBatch()) {
        VectorSchemaRoot root = currentReader.getVectorSchemaRoot();
        int numRows = root.getRowCount();
        if (numRows == 0) {
          continue;
        }

        // The reader loads each batch into new buffers of the same root. A view keeps the memory
        // addresses of the buffers, thus the views are made again for each batch.
        return new ColumnarBatch(buildColumns(root, numRows), numRows);
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to read the next batch of Vortex file: " + path, e);
    }
    return null;
  }

  /**
   * Returns the columns of a batch: a view of each output column that the scan gives, found by
   * name, a null column for each other output column, then the partition columns.
   */
  private ColumnVector[] buildColumns(VectorSchemaRoot root, int numRows) {
    List<FieldVector> fieldVectors = root.getFieldVectors();
    Map<String, FieldVector> byName = new HashMap<>(fieldVectors.size() * 2);
    for (FieldVector fieldVector : fieldVectors) {
      byName.put(fieldVector.getName(), fieldVector);
    }

    ColumnVector[] vectors = new ColumnVector[outputFields.length + partitionFields.length];
    for (int i = 0; i < outputFields.length; i++) {
      FieldVector fieldVector = byName.get(outputFields[i].name());
      if (fieldVector == null) {
        vectors[i] = VortexArrowColumnVector.nulls(outputFields[i].dataType(), numRows);
      } else if (fieldVector instanceof UInt8Vector) {
        vectors[i] = VortexArrowColumnVector.rowIndices((UInt8Vector) fieldVector);
      } else {
        // Spark can request a struct without the fields that the query does not read.
        vectors[i] = new VortexArrowColumnVector(fieldVector).conformTo(outputFields[i].dataType());
      }
    }

    for (int i = 0; i < partitionFields.length; i++) {
      vectors[outputFields.length + i] =
          new VortexPartitionColumnVector(partitionFields[i].dataType(), partitionValues, i);
    }
    return vectors;
  }

  private void closeCurrentReader() {
    if (currentReader == null) {
      return;
    }

    try {
      currentReader.close();
    } catch (IOException e) {
      throw new HoodieException("Failed to close the Vortex Arrow reader of file: " + path, e);
    } finally {
      currentReader = null;
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;

    ArrowReader reader = currentReader;
    currentReader = null;
    nextBatch = null;
    VortexResourceCloser.closeAll(reader, scan, file, allocator, path);
  }
}
