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

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.lance.file.LanceFileReader;

import java.io.IOException;

/**
 * Shared close logic for Lance-backed iterators. Closes the in-flight batch, Arrow reader,
 * Lance reader, and allocator in that order — collecting exceptions and rethrowing after the
 * allocator is closed so a failed reader close never leaks the allocator.
 */
final class LanceResourceCloser {

  private LanceResourceCloser() {
  }

  static void closeAll(ColumnarBatch currentBatch,
                       ArrowReader arrowReader,
                       LanceFileReader lanceReader,
                       BufferAllocator allocator) {
    IOException arrowException = null;
    Exception lanceException = null;

    if (currentBatch != null) {
      currentBatch.close();
    }
    if (arrowReader != null) {
      try {
        arrowReader.close();
      } catch (IOException e) {
        arrowException = e;
      }
    }
    if (lanceReader != null) {
      try {
        lanceReader.close();
      } catch (Exception e) {
        lanceException = e;
      }
    }
    if (allocator != null) {
      allocator.close();
    }

    if (arrowException != null) {
      HoodieIOException ex = new HoodieIOException("Failed to close Arrow reader", arrowException);
      if (lanceException != null) {
        ex.addSuppressed(lanceException);
      }
      throw ex;
    }
    if (lanceException != null) {
      throw new HoodieException("Failed to close Lance reader", lanceException);
    }
  }
}
