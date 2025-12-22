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

package org.apache.hudi.common.fs;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper for DataOutputStream to keep track of number of bytes written.
 */
public class SizeAwareDataOutputStream {

  // Actual outputStream
  private DataOutputStream outputStream;
  // Counter to keep track of number of bytes written
  private AtomicLong size;

  public SizeAwareDataOutputStream(FileOutputStream fileOutputStream, int cacheSize) {
    this.outputStream = new DataOutputStream(new BufferedOutputStream(fileOutputStream, cacheSize));
    this.size = new AtomicLong(0L);
  }

  public void writeLong(long v) throws IOException {
    size.addAndGet(Long.BYTES);
    outputStream.writeLong(v);
  }

  public void writeInt(int v) throws IOException {
    size.addAndGet(Integer.BYTES);
    outputStream.writeInt(v);
  }

  public void write(byte[] v) throws IOException {
    size.addAndGet(v.length);
    outputStream.write(v);
  }

  public void write(byte[] v, int offset, int len) throws IOException {
    size.addAndGet((long) len + offset);
    outputStream.write(v, offset, len);
  }

  public void flush() throws IOException {
    outputStream.flush();
  }

  public void close() throws IOException {
    outputStream.close();
  }

  public long getSize() {
    return size.get();
  }
}
