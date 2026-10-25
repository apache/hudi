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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * Shared close logic for the Vortex reader iterator (vortex-jni 0.76.0). Closes the current Arrow
 * reader (which owns the exported VectorSchemaRoot / column vectors) and then the allocator,
 * rethrowing after the allocator is closed so a failed reader close never leaks the allocator.
 *
 * <p>The Vortex Session/DataSource/Scan handles are not AutoCloseable in 0.76.0 (native cleanup is
 * implicit), so they are not closed here.
 */
final class VortexResourceCloser {

  private VortexResourceCloser() {
  }

  static void closeAll(ArrowReader currentReader, BufferAllocator allocator, String path) {
    Exception readerException = null;

    if (currentReader != null) {
      try {
        currentReader.close();
      } catch (Exception e) {
        readerException = e;
      }
    }
    if (allocator != null) {
      allocator.close();
    }

    if (readerException != null) {
      throw new HoodieException("Failed to close Vortex Arrow reader for file: " + path, readerException);
    }
  }
}
