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

package org.apache.hudi.func;

import org.apache.hudi.common.util.queue.BoundedInMemoryQueue;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.Iterator;

/**
 * This class wraps a parquet reader and provides an iterator based api to read from a parquet file. This is used in
 * {@link BoundedInMemoryQueue}
 */
public class ParquetReaderIterator<T> implements Iterator<T> {

  // Parquet reader for an existing parquet file
  private final ParquetReader<T> parquetReader;
  // Holds the next entry returned by the parquet reader
  private T next;

  public ParquetReaderIterator(ParquetReader<T> parquetReader) {
    this.parquetReader = parquetReader;
  }

  @Override
  public boolean hasNext() {
    try {
      // To handle when hasNext() is called multiple times for idempotency and/or the first time
      if (this.next == null) {
        this.next = parquetReader.read();
      }
      return this.next != null;
    } catch (IOException io) {
      throw new HoodieIOException("unable to read next record from parquet file ", io);
    }
  }

  @Override
  public T next() {
    try {
      // To handle case when next() is called before hasNext()
      if (this.next == null) {
        if (!hasNext()) {
          throw new HoodieIOException("No more records left to read from parquet file");
        }
      }
      T retVal = this.next;
      this.next = parquetReader.read();
      return retVal;
    } catch (IOException io) {
      throw new HoodieIOException("unable to read next record from parquet file ", io);
    }
  }

  public void close() throws IOException {
    parquetReader.close();
  }
}
