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
import org.apache.hudi.io.vortex.VortexInputFile;

import dev.vortex.api.Scan;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * Shared close logic for the Vortex reader iterator. Closes the current Arrow reader (which owns
 * the exported VectorSchemaRoot / column vectors), the Vortex scan, the file, and then the
 * allocator. The file is closed after the scan, because the native code reads through it until
 * the scan and its readers are closed. A failed close is thrown again after the allocator is
 * closed, so it never leaks the allocator.
 */
final class VortexResourceCloser {

  private VortexResourceCloser() {
  }

  static void closeAll(ArrowReader currentReader,
                       Scan scan,
                       VortexInputFile file,
                       BufferAllocator allocator,
                       String path) {
    Exception readerException = null;

    if (currentReader != null) {
      try {
        currentReader.close();
      } catch (Exception e) {
        readerException = e;
      }
    }

    if (scan != null) {
      scan.close();
    }

    if (file != null) {
      try {
        file.close();
      } catch (Exception e) {
        readerException = addSuppressed(readerException, e);
      }
    }

    if (allocator != null) {
      allocator.close();
    }

    if (readerException != null) {
      throw new HoodieException("Failed to close Vortex reader for file: " + path, readerException);
    }
  }

  private static Exception addSuppressed(Exception primary, Exception e) {
    if (primary == null) {
      return e;
    }
    primary.addSuppressed(e);
    return primary;
  }
}
