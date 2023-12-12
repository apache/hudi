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

package org.apache.hudi.io.hfile;

import org.apache.hudi.io.util.IOUtils;

import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT16;
import static org.apache.hudi.io.util.IOUtils.readShort;

/**
 * Represents the key part only.
 */
public class Key implements Comparable<Key> {
  private static final int CONTENT_LENGTH_SIZE = SIZEOF_INT16;
  private final byte[] bytes;
  private final int offset;
  private final int length;

  public Key(byte[] bytes) {
    this(bytes, 0, bytes.length);
  }

  public Key(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public int getOffset() {
    return this.offset;
  }

  public int getLength() {
    return length;
  }

  public int getContentOffset() {
    return getOffset() + CONTENT_LENGTH_SIZE;
  }

  public int getContentLength() {
    return readShort(bytes, getOffset());
  }

  @Override
  public int compareTo(Key o) {
    return IOUtils.compareTo(
        getBytes(), getContentOffset(), getContentLength(),
        o.getBytes(), o.getContentOffset(), o.getContentLength());
  }
}
