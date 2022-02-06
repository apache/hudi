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

import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * Implementation of {@link InputFile} backed by {@code byte[]} buffer
 */
public class ByteBufferBackedInputFile implements InputFile {
  private final byte[] buffer;
  private final int offset;
  private final int length;

  public ByteBufferBackedInputFile(byte[] buffer, int offset, int length) {
    this.buffer = buffer;
    this.offset = offset;
    this.length = length;
  }

  public ByteBufferBackedInputFile(byte[] buffer) {
    this(buffer, 0, buffer.length);
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new DelegatingSeekableInputStream(new ByteBufferBackedInputStream(buffer, offset, length)) {
      @Override
      public long getPos() {
        return ((ByteBufferBackedInputStream) getStream()).getPosition();
      }

      @Override
      public void seek(long newPos) {
        ((ByteBufferBackedInputStream) getStream()).seek(newPos);
      }
    };
  }
}
