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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.ClosableIterator;

import java.io.IOException;
import java.util.List;

/**
 * Engine hooks for the vectorized merge-on-read read of {@link HoodieFileGroupReader#getClosableBatchIterator()}:
 * reading a base file as columnar batches and building the batches the read returns.
 *
 * <p>A base batch holds the columns of the read schema in order. The engine may append columns after them, such as
 * partition values; the read keeps them in its output after the columns of the requested schema.
 *
 * @param <T> the engine row type
 * @param <B> the engine batch type
 */
public interface BatchSupport<T, B> {

  /**
   * Reads a base file as batches with the columns of {@code readSchema}. The row index column
   * ({@code PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME}), when in the schema, holds each row's
   * position in the file.
   *
   * @param applyFilters whether the read may skip rows by the scan's filters
   */
  ClosableIterator<B> readBaseFile(HoodieBaseFile baseFile, HoodieSchema readSchema, boolean applyFilters) throws IOException;

  /**
   * Whether {@link #readBaseFile} may run on a thread other than the one consuming the batches.
   */
  default boolean supportsConcurrentOpen() {
    return false;
  }

  int numRows(B batch);

  boolean isNullAt(B batch, int column, int row);

  long getLong(B batch, int column, int row);

  /**
   * The record key at a row in the engine's form, equal to {@link #toEngineKey} of the same key.
   */
  Object getEngineKey(B batch, int column, int row);

  Object toEngineKey(String recordKey);

  /**
   * A row of a batch in the read schema layout. It is only valid until the batch advances.
   */
  T getRow(B batch, int row);

  /**
   * The first {@code numOutputColumns} columns of the read schema of a batch with {@code numReadColumns} columns,
   * followed by the columns the engine appended, at the given rows, or at all rows when {@code rowIds} is null. The
   * result may share memory with the batch and is only valid until the batch advances.
   */
  B select(B batch, int numOutputColumns, int numReadColumns, int[] rowIds, int numRows);

  /**
   * A batch of rows laid out in the output schema, followed by the columns the engine appends.
   */
  B toBatch(List<T> rows);
}
