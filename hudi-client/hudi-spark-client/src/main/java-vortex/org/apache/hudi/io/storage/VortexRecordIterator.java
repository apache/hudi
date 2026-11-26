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
import org.apache.hudi.io.vortex.VortexInputFile;

import dev.vortex.api.Scan;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator that reads a Vortex scan as Spark {@link UnsafeRow}s.
 *
 * <p>It reads the batches of a {@link VortexBatchIterator} and copies each row. A requested column
 * that the scan does not give is null. A column of unsigned 64-bit integers is a column of row
 * indices.
 *
 * <p>This iterator owns, and closes, the {@link BufferAllocator}, the {@link Scan} and the
 * {@link VortexInputFile}.
 */
public final class VortexRecordIterator implements ClosableIterator<UnsafeRow> {

  private final VortexBatchIterator batches;
  private final UnsafeProjection projection;

  private Iterator<InternalRow> rowIterator;

  public VortexRecordIterator(BufferAllocator allocator,
                              VortexInputFile file,
                              Scan scan,
                              StructType schema) {
    this.batches = new VortexBatchIterator(allocator, file, scan, schema, new StructType(), InternalRow.empty());
    this.projection = UnsafeProjection.create(schema);
  }

  @Override
  public boolean hasNext() {
    while (rowIterator == null || !rowIterator.hasNext()) {
      if (!batches.hasNext()) {
        return false;
      }
      rowIterator = batches.next().rowIterator();
    }
    return true;
  }

  @Override
  public UnsafeRow next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more records available");
    }
    return projection.apply(rowIterator.next()).copy();
  }

  @Override
  public void close() {
    rowIterator = null;
    batches.close();
  }
}
