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

package org.apache.hudi.table.format;

import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.format.cow.vector.reader.ParquetColumnarRowSplitReader;

import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * Hoodie wrapper for flink parquet reader.
 */
public final class ParquetSplitRecordIterator implements ClosableIterator<RowData> {
  private final ParquetColumnarRowSplitReader reader;

  public ParquetSplitRecordIterator(ParquetColumnarRowSplitReader reader) {
    this.reader = reader;
  }

  @Override
  public boolean hasNext() {
    try {
      return !reader.reachedEnd();
    } catch (IOException e) {
      throw new HoodieIOException("Decides whether the parquet columnar row split reader reached end exception", e);
    }
  }

  @Override
  public RowData next() {
    return reader.nextRecord();
  }

  @Override
  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      throw new HoodieIOException("Close the parquet columnar row split reader exception", e);
    }
  }
}
