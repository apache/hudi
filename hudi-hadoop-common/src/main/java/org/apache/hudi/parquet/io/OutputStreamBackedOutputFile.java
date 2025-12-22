/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.parquet.io;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * Implementation of the {@link OutputFile} backed by {@link java.io.OutputStream}
 */
public class OutputStreamBackedOutputFile implements OutputFile {

  private static final long DEFAULT_BLOCK_SIZE = 1024L * 1024L;

  private final FSDataOutputStream outputStream;

  public OutputStreamBackedOutputFile(FSDataOutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) {
    return new PositionOutputStreamAdapter(outputStream);
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) {
    return create(blockSizeHint);
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return DEFAULT_BLOCK_SIZE;
  }

  private static class PositionOutputStreamAdapter extends PositionOutputStream {
    private final FSDataOutputStream delegate;

    PositionOutputStreamAdapter(FSDataOutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void write(int b) throws IOException {
      delegate.write(b);
    }

    @Override
    public void write(@Nonnull byte[] buffer, int off, int len) throws IOException {
      delegate.write(buffer, off, len);
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() {
      // We're deliberately not closing the delegate stream here to allow caller
      // to explicitly manage its lifecycle
    }
  }
}
