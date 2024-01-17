/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io.compress;

import java.io.IOException;
import java.io.InputStream;

/**
 * A compression input stream.
 *
 * <p>Implementations are assumed to be buffered.  This permits clients to
 * reposition the underlying input stream then call {@link #resetState()},
 * without having to also synchronize client buffers.
 * <p>
 * This class is copied from
 * {@code org.apache.hadoop.io.compress.CompressionInputStream}
 */
public abstract class CompressionInputStream extends InputStream {
  /**
   * The input stream to be compressed.
   */
  protected final InputStream in;
  protected long maxAvailableData = 0L;

  private Decompressor trackedDecompressor;

  /**
   * Create a compression input stream that reads
   * the decompressed bytes from the given stream.
   *
   * @param in The input stream to be compressed.
   * @throws IOException
   */
  protected CompressionInputStream(InputStream in) throws IOException {
    this.maxAvailableData = in.available();
    this.in = in;
  }

  @Override
  public void close() throws IOException {
    try {
      in.close();
    } finally {
      if (trackedDecompressor != null) {
        CodecPool.returnDecompressor(trackedDecompressor);
        trackedDecompressor = null;
      }
    }
  }

  /**
   * Read bytes from the stream.
   * Made abstract to prevent leakage to underlying stream.
   */
  @Override
  public abstract int read(byte[] b, int off, int len) throws IOException;

  /**
   * Reset the decompressor to its initial state and discard any buffered data,
   * as the underlying stream may have been repositioned.
   */
  public abstract void resetState() throws IOException;

  void setTrackedDecompressor(Decompressor decompressor) {
    trackedDecompressor = decompressor;
  }
}

