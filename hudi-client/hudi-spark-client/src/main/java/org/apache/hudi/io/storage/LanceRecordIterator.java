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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.lance.file.FileReadOptions;
import org.lance.file.LanceFileReader;
import org.lance.spark.vectorized.LanceArrowColumnVector;
import org.lance.util.Range;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Iterator over a Lance file that converts Arrow batches to Spark {@link UnsafeRow}s, owning the
 * {@link BufferAllocator}, {@link LanceFileReader}, {@link ArrowReader}(s) and current
 * {@link ColumnarBatch}. Used by both Hudi's internal Lance reader and the Spark datasource. An
 * optional {@link BlobDescriptorTransform} rewrites BLOB columns for DESCRIPTOR-mode reads.
 *
 * <p>BLOB chunked reading: lance-core 4.0.0 aborts the JVM in its Arrow C-stream export whenever a
 * single {@code readAll} stream crosses Lance's internal BLOB page boundary (512 rows), and the
 * requested {@code batchSize} does not change that. So BLOB reads issue one {@code readAll} per
 * row-range chunk of {@link #BLOB_READ_CHUNK_ROWS} rows; non-BLOB reads use one streamed reader.
 */
public final class LanceRecordIterator implements ClosableIterator<UnsafeRow> {

  /**
   * Rows per {@code readAll} for BLOB reads. Must not exceed Lance's internal BLOB page size,
   * which is 512 rows in lance-core 4.0.0 but is NOT exposed through any lance API; revalidate
   * this constant against the batch-scale BLOB tests whenever {@code lance.version} is bumped.
   */
  public static final int BLOB_READ_CHUNK_ROWS = 512;

  /**
   * The sequence of {@link ArrowReader}s to drain, one after another. Single-reader
   * mode yields one reader; BLOB chunked mode yields one fresh reader per row-range chunk. Each
   * returned reader is owned (and closed) by {@link LanceRecordIterator}.
   */
  private interface ArrowReaderSequence {
    /** @return the next reader to drain, or {@code null} when the sequence is exhausted. */
    ArrowReader next() throws IOException;
  }

  private final BufferAllocator allocator;
  private final LanceFileReader lanceReader;
  private final ArrowReaderSequence readerSequence;
  private final StructType sparkSchema;
  private final UnsafeProjection projection;
  private final String path;
  private final BlobDescriptorTransform blobTransform;

  /** The reader currently being drained; replaced as chunks are exhausted. */
  private ArrowReader currentReader;
  private ColumnarBatch currentBatch;
  private Iterator<InternalRow> rowIterator;
  private ColumnVector[] columnVectors;
  /** Row index within the current batch; only used by {@link BlobDescriptorTransform}. */
  private int rowIdInBatch;
  private boolean closed = false;

  /**
   * Creates a new Lance record iterator without blob transform (CONTENT mode or non-blob reads).
   */
  public LanceRecordIterator(BufferAllocator allocator,
                             LanceFileReader lanceReader,
                             ArrowReader arrowReader,
                             StructType schema,
                             String path) {
    this(allocator, lanceReader, arrowReader, schema, path, null);
  }

  /**
   * Creates a new Lance record iterator that drains a single pre-built {@link ArrowReader}.
   * Suitable for non-BLOB reads, where Lance's multi-batch FFI export is well-behaved.
   *
   * @param allocator      Arrow buffer allocator for memory management
   * @param lanceReader    Lance file reader
   * @param arrowReader    Arrow reader for batch reading
   * @param schema         Spark schema for the records
   * @param path           File path (for error messages)
   * @param blobTransform  optional blob descriptor transform for DESCRIPTOR-mode reads; null for
   *                       CONTENT mode or non-blob reads
   */
  public LanceRecordIterator(BufferAllocator allocator,
                             LanceFileReader lanceReader,
                             ArrowReader arrowReader,
                             StructType schema,
                             String path,
                             BlobDescriptorTransform blobTransform) {
    this(allocator, lanceReader, singleReaderSequence(arrowReader), schema, path, blobTransform);
  }

  private LanceRecordIterator(BufferAllocator allocator,
                              LanceFileReader lanceReader,
                              ArrowReaderSequence readerSequence,
                              StructType schema,
                              String path,
                              BlobDescriptorTransform blobTransform) {
    this.allocator = allocator;
    this.lanceReader = lanceReader;
    this.readerSequence = readerSequence;
    this.sparkSchema = schema;
    this.projection = UnsafeProjection.create(schema);
    this.path = path;
    this.blobTransform = blobTransform;
  }

  /**
   * Creates a Lance record iterator that reads a BLOB-containing file in fixed-size row-range
   * chunks, issuing a fresh {@code readAll} per chunk to dodge the lance-core multi-page BLOB
   * FFI-export panic (see class javadoc).
   *
   * @param allocator     Arrow buffer allocator for memory management
   * @param lanceReader   open Lance file reader (the iterator takes ownership and closes it)
   * @param columnNames   columns to project, or {@code null} for all columns
   * @param readOpts      Lance read options (e.g. blob read mode)
   * @param totalRows     total rows in the file ({@code lanceReader.numRows()})
   * @param schema        Spark schema for the records
   * @param path          File path (for error messages)
   * @param blobTransform optional blob descriptor transform for DESCRIPTOR-mode reads
   */
  public static LanceRecordIterator chunkedBlobReader(BufferAllocator allocator,
                                                      LanceFileReader lanceReader,
                                                      List<String> columnNames,
                                                      FileReadOptions readOpts,
                                                      long totalRows,
                                                      StructType schema,
                                                      String path,
                                                      BlobDescriptorTransform blobTransform) {
    ArrowReaderSequence sequence = new ArrowReaderSequence() {
      private long nextStart = 0;

      @Override
      public ArrowReader next() throws IOException {
        if (nextStart >= totalRows) {
          return null;
        }
        int start = Math.toIntExact(nextStart);
        int end = Math.toIntExact(Math.min(nextStart + BLOB_READ_CHUNK_ROWS, totalRows));
        nextStart = end;
        // A single range per readAll keeps the FFI stream within one BLOB page (<= 512 rows).
        List<Range> ranges = Collections.singletonList(new Range(start, end));
        return lanceReader.readAll(columnNames, ranges, BLOB_READ_CHUNK_ROWS, readOpts);
      }
    };
    return new LanceRecordIterator(allocator, lanceReader, sequence, schema, path, blobTransform);
  }

  private static ArrowReaderSequence singleReaderSequence(ArrowReader arrowReader) {
    return new ArrowReaderSequence() {
      private ArrowReader remaining = arrowReader;

      @Override
      public ArrowReader next() {
        ArrowReader r = remaining;
        remaining = null;
        return r;
      }
    };
  }

  @Override
  public boolean hasNext() {
    if (rowIterator != null && rowIterator.hasNext()) {
      return true;
    }

    if (currentBatch != null) {
      currentBatch.close();
      currentBatch = null;
    }

    try {
      while (true) {
        if (currentReader == null) {
          currentReader = readerSequence.next();
          if (currentReader == null) {
            return false;
          }
          // Each reader (range chunk) returns a distinct VectorSchemaRoot, so the cached
          // column vectors must be rebuilt against the new reader's vectors.
          columnVectors = null;
        }

        // Try to load next batch from the current reader. Loop so zero-row batches
        // (legitimately returned e.g. after filter pushdown) don't silently terminate.
        while (currentReader.loadNextBatch()) {
          VectorSchemaRoot root = currentReader.getVectorSchemaRoot();

          // Build ColumnVector[] in Spark-schema order by looking each field up by name;
          // lance-spark 0.4.0's VectorSchemaRoot may return the file's on-disk order, which
          // would misalign the UnsafeProjection. Cached per reader and reused thereafter.
          if (columnVectors == null) {
            buildColumnVectors(root);
          }

          currentBatch = new ColumnarBatch(columnVectors, root.getRowCount());
          rowIterator = currentBatch.rowIterator();
          rowIdInBatch = 0;
          if (rowIterator.hasNext()) {
            return true;
          }
          currentBatch.close();
          currentBatch = null;
        }

        // Current reader exhausted; close it and advance to the next chunk (if any).
        currentReader.close();
        currentReader = null;
        columnVectors = null;
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to read next batch from Lance file: " + path, e);
    } catch (Exception e) {
      throw new HoodieException("Failed to advance Lance reader for file: " + path, e);
    }
  }

  @Override
  public UnsafeRow next() {
    if (!hasNext()) {
      throw new IllegalStateException("No more records available");
    }
    InternalRow row = rowIterator.next();
    if (blobTransform != null) {
      return blobTransform.transformRow(row, rowIdInBatch++, columnVectors, projection);
    }
    rowIdInBatch++;
    return projection.apply(row).copy();
  }

  private void buildColumnVectors(VectorSchemaRoot root) {
    List<FieldVector> fieldVectors = root.getFieldVectors();
    Map<String, FieldVector> byName = new HashMap<>(fieldVectors.size() * 2);
    for (FieldVector fv : fieldVectors) {
      byName.put(fv.getName(), fv);
    }
    StructField[] sparkFields = sparkSchema.fields();
    if (sparkFields.length != fieldVectors.size()) {
      throw new HoodieException("Lance batch column count " + fieldVectors.size()
          + " does not match expected Spark schema size " + sparkFields.length
          + " for file: " + path);
    }
    columnVectors = new ColumnVector[sparkFields.length];
    for (int i = 0; i < sparkFields.length; i++) {
      String name = sparkFields[i].name();
      FieldVector fv = byName.get(name);
      if (fv == null) {
        throw new HoodieException("Lance batch missing expected column '" + name
            + "' for file: " + path + "; available columns: " + byName.keySet());
      }
      columnVectors[i] = new LanceArrowColumnVector(fv);
    }
    if (blobTransform != null) {
      blobTransform.init(columnVectors, sparkSchema);
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    ColumnarBatch batch = currentBatch;
    currentBatch = null;
    ArrowReader reader = currentReader;
    currentReader = null;
    LanceResourceCloser.closeAll(batch, reader, lanceReader, allocator);
  }
}
