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

package org.apache.hudi.common.fs.inline;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.io.ByteBufferPool;

import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

/**
 * Inline {@link FSDataInputStream}. A startOffset that is passed in is assumed to be the start of the InputStream.
 * All operations are handled having the {@code startOffset} as starting point.
 */
public class InLineFsDataInputStream extends FSDataInputStream {

  private final int startOffset;
  private final FSDataInputStream outerStream;
  private final int length;

  public InLineFsDataInputStream(int startOffset, FSDataInputStream outerStream, int length) throws IOException {
    super(outerStream.getWrappedStream());
    this.startOffset = startOffset;
    this.outerStream = outerStream;
    this.length = length;
    outerStream.seek(startOffset);
  }

  @Override
  public void seek(long desired) throws IOException {
    if (desired > length) {
      throw new IOException("Attempting to seek past inline content");
    }
    outerStream.seek(startOffset + desired);
  }

  @Override
  public long getPos() throws IOException {
    return outerStream.getPos() - startOffset;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    if ((length + offset) > this.length) {
      throw new IOException("Attempting to read past inline content");
    }
    return outerStream.read(startOffset + position, buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    if ((length + offset) > this.length) {
      throw new IOException("Attempting to read past inline content");
    }
    outerStream.readFully(startOffset + position, buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer)
      throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    if (targetPos > this.length) {
      throw new IOException("Attempting to seek past inline content");
    }
    return outerStream.seekToNewSource(startOffset + targetPos);
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    return outerStream.read(buf);
  }

  @Override
  public FileDescriptor getFileDescriptor() throws IOException {
    return outerStream.getFileDescriptor();
  }

  @Override
  public void setReadahead(Long readahead) throws IOException, UnsupportedOperationException {
    if (readahead > this.length) {
      throw new IOException("Attempting to set read ahead past inline content");
    }
    outerStream.setReadahead(readahead);
  }

  @Override
  public void setDropBehind(Boolean dropBehind) throws IOException, UnsupportedOperationException {
    outerStream.setDropBehind(dropBehind);
  }

  @Override
  public ByteBuffer read(ByteBufferPool bufferPool, int maxLength, EnumSet<ReadOption> opts)
      throws IOException, UnsupportedOperationException {
    if (maxLength > this.length) {
      throw new IOException("Attempting to read max length beyond inline content");
    }
    return outerStream.read(bufferPool, maxLength, opts);
  }

  @Override
  public void releaseBuffer(ByteBuffer buffer) {
    outerStream.releaseBuffer(buffer);
  }

  @Override
  public void unbuffer() {
    outerStream.unbuffer();
  }
}
