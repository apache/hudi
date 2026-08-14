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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;

import dev.vortex.api.DataSource;
import dev.vortex.api.Partition;
import dev.vortex.api.Scan;
import dev.vortex.api.Session;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Iterator for reading Vortex files (vortex-jni 0.76.0) and converting Arrow batches to Spark
 * {@link UnsafeRow}s.
 *
 * <p>A {@link Scan} yields a sequence of {@link Partition}s; each partition is read as an Arrow
 * {@link ArrowReader} via {@link Partition#scanArrow}, whose {@link VectorSchemaRoot} is reused
 * across {@code loadNextBatch} calls. Only the columns present in the requested Spark schema are
 * projected (by name) out of each batch, so the scan may read more columns than requested.
 *
 * <p>Lifecycle managed here: the {@link BufferAllocator} and the current {@link ArrowReader}. The
 * {@link Session}/{@link DataSource}/{@link Scan} handles are held only to keep their native
 * resources alive during iteration (they are not AutoCloseable in vortex-jni 0.76.0).
 */
public final class VortexRecordIterator implements ClosableIterator<UnsafeRow> {
  private final BufferAllocator allocator;
  private final Session session;
  private final DataSource dataSource;
  private final Scan scan;
  private final StructType sparkSchema;
  private final UnsafeProjection projection;
  private final String path;

  private ArrowReader currentReader;
  private ColumnVector[] columnVectors;
  private ColumnarBatch currentBatch;
  private Iterator<InternalRow> rowIterator;
  private boolean closed = false;

  public VortexRecordIterator(BufferAllocator allocator,
                              Session session,
                              DataSource dataSource,
                              Scan scan,
                              StructType schema,
                              String path) {
    this.allocator = allocator;
    this.session = session;
    this.dataSource = dataSource;
    this.scan = scan;
    this.sparkSchema = schema;
    this.projection = UnsafeProjection.create(schema);
    this.path = path;
  }

  @Override
  public boolean hasNext() {
    if (rowIterator != null && rowIterator.hasNext()) {
      return true;
    }

    // Advance within the current partition, then across partitions, until a non-empty batch.
    while (true) {
      if (currentReader != null) {
        if (advanceBatch()) {
          return true;
        }
        // Current partition exhausted.
        closeCurrentReader();
      }
      if (!scan.hasNext()) {
        return false;
      }
      Partition partition = scan.next();
      currentReader = partition.scanArrow(allocator);
      columnVectors = null;
    }
  }

  /**
   * Load the next batch from the current ArrowReader (whose VectorSchemaRoot is reused across
   * batches, so the wrapping column vectors are built once per partition) and set up the row
   * iterator. Returns false when the current reader is exhausted.
   */
  private boolean advanceBatch() {
    try {
      while (currentReader.loadNextBatch()) {
        VectorSchemaRoot root = currentReader.getVectorSchemaRoot();
        if (columnVectors == null) {
          columnVectors = buildColumnVectors(root);
        }
        currentBatch = new ColumnarBatch(columnVectors, root.getRowCount());
        rowIterator = currentBatch.rowIterator();
        if (rowIterator.hasNext()) {
          return true;
        }
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to read next batch from Vortex file: " + path, e);
    }
    return false;
  }

  @Override
  public UnsafeRow next() {
    if (!hasNext()) {
      throw new IllegalStateException("No more records available");
    }
    InternalRow row = rowIterator.next();
    return projection.apply(row).copy();
  }

  /**
   * Wrap the requested columns of the batch as Spark {@link ColumnVector}s, matched by name. The
   * batch may carry more columns than requested (the scan reads all columns); extras are ignored.
   */
  private ColumnVector[] buildColumnVectors(VectorSchemaRoot root) {
    List<FieldVector> fieldVectors = root.getFieldVectors();
    Map<String, FieldVector> byName = new HashMap<>(fieldVectors.size() * 2);
    for (FieldVector fv : fieldVectors) {
      byName.put(fv.getName(), fv);
    }
    StructField[] sparkFields = sparkSchema.fields();
    ColumnVector[] vectors = new ColumnVector[sparkFields.length];
    for (int i = 0; i < sparkFields.length; i++) {
      String name = sparkFields[i].name();
      FieldVector fv = byName.get(name);
      if (fv == null) {
        throw new HoodieException("Vortex batch missing expected column '" + name
            + "' for file: " + path + "; available columns: " + byName.keySet());
      }
      vectors[i] = new ArrowColumnVector(fv);
    }
    return vectors;
  }

  private void closeCurrentReader() {
    currentBatch = null;
    columnVectors = null;
    rowIterator = null;
    if (currentReader != null) {
      try {
        currentReader.close();
      } catch (IOException e) {
        throw new HoodieException("Failed to close Vortex Arrow reader for file: " + path, e);
      } finally {
        currentReader = null;
      }
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
    currentBatch = null;
    columnVectors = null;
    rowIterator = null;
    VortexResourceCloser.closeAll(reader, allocator, path);
  }
}
