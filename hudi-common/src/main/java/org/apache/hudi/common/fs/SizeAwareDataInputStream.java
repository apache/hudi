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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps track of how many bytes were read from a DataInputStream.
 */
public class SizeAwareDataInputStream {

  private final DataInputStream dis;
  private final AtomicInteger numberOfBytesRead;

  public SizeAwareDataInputStream(DataInputStream dis) {
    this.dis = dis;
    this.numberOfBytesRead = new AtomicInteger(0);
  }

  public int readInt() throws IOException {
    int value = dis.readInt();
    numberOfBytesRead.addAndGet(Integer.BYTES);
    return value;
  }

  public void readFully(byte[] b, int off, int len) throws IOException {
    dis.readFully(b, off, len);
    numberOfBytesRead.addAndGet(len);
  }

  public void readFully(byte[] b) throws IOException {
    dis.readFully(b);
    numberOfBytesRead.addAndGet(b.length);
  }

  public int skipBytes(int n) throws IOException {
    int numOfBytes = dis.skipBytes(n);
    numberOfBytesRead.addAndGet(numOfBytes);
    return numOfBytes;
  }

  public void close() throws IOException {
    dis.close();
  }

  public int getNumberOfBytesRead() {
    return numberOfBytesRead.get();
  }
}
