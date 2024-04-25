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

package org.apache.hudi.io;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.io.ByteBufferPool;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileDescriptor;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;

public class InlineSeekableDataInputStream implements DataInput, SeekableDataInputStream  {
  private final long startOffset;
  private final SeekableDataInputStream outerStream;
  private final long length;
  /**
   * Creates a DataInputStream that uses the specified
   * underlying InputStream.
   *
   * @param in the specified input stream
   */
  public InlineSeekableDataInputStream(long startOffset, SeekableDataInputStream outerStream, long length) {
    this.startOffset = startOffset;
    this.outerStream = outerStream;
    this.length = length;
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
  public void close() throws IOException {

  }

  @Override
  public void readFully(byte[] b) throws IOException {

  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    if ((length + offset) > this.length) {
      throw new IOException("Attempting to read past inline content");
    }
    outerStream.readFully(b, startOffset + off, len);

  }

  @Override
  public int skipBytes(int n) throws IOException {
    return 0;
  }

  @Override
  public boolean readBoolean() throws IOException {
    return false;
  }

  @Override
  public byte readByte() throws IOException {
    return 0;
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return 0;
  }

  @Override
  public short readShort() throws IOException {
    return 0;
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return 0;
  }

  @Override
  public char readChar() throws IOException {
    return 0;
  }

  @Override
  public int readInt() throws IOException {
    return 0;
  }

  @Override
  public long readLong() throws IOException {
    return 0;
  }

  @Override
  public float readFloat() throws IOException {
    return 0;
  }

  @Override
  public double readDouble() throws IOException {
    return 0;
  }

  @Override
  public String readLine() throws IOException {
    return null;
  }

  @Override
  public String readUTF() throws IOException {
    return null;
  }
}
