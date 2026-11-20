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

package org.apache.hudi.utilities.unstructured.embedding.source;

import java.io.Closeable;
import java.io.IOException;

/**
 * Random access to the bytes of one source row's blob: positions are relative to the blob's first byte and reads stop at
 * {@link #length()}. An out-of-line blob is read from storage on demand, an inline one from the row's own bytes; neither copies
 * the file into memory.
 */
public interface SeekableBlob extends Closeable {

  /** The blob's size in bytes. */
  long length();

  /** The position of the next read, in [0, length]. */
  long getPos() throws IOException;

  /**
   * @param pos in [0, length]
   * @throws java.io.EOFException when {@code pos} is outside the blob
   */
  void seek(long pos) throws IOException;

  /** Reads up to {@code len} bytes; -1 at the end of the blob. */
  int read(byte[] buffer, int offset, int len) throws IOException;
}
