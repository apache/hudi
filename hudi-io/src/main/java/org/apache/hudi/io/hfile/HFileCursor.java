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

import org.apache.hudi.common.util.Option;

/**
 * Stores the current position and {@link KeyValue} at the position in the HFile.
 * The same instance is used as a position cursor during HFile reading.
 * The {@link KeyValue} can be lazily read and cached.
 */
public class HFileCursor {
  private static final int INVALID_POSITION = -1;

  private int offset;
  private Option<KeyValue> keyValue;
  private boolean eof;

  public HFileCursor() {
    this.offset = INVALID_POSITION;
    this.keyValue = Option.empty();
    this.eof = false;
  }

  public boolean isSeeked() {
    return offset != INVALID_POSITION || eof;
  }

  public boolean isValid() {
    return !(offset == INVALID_POSITION || eof);
  }

  public int getOffset() {
    return offset;
  }

  public Option<KeyValue> getKeyValue() {
    return keyValue;
  }

  public void set(int offset, KeyValue keyValue) {
    this.offset = offset;
    this.keyValue = Option.of(keyValue);
  }

  public void setOffset(int offset) {
    this.offset = offset;
    this.keyValue = Option.empty();
  }

  public void setKeyValue(KeyValue keyValue) {
    this.keyValue = Option.of(keyValue);
  }

  public void setEof() {
    this.eof = true;
    this.keyValue = Option.empty();
  }

  public void unsetEof() {
    this.eof = false;
  }

  public void increment(long incr) {
    this.offset += incr;
    this.keyValue = Option.empty();
  }

  @Override
  public String toString() {
    return "HFilePosition{offset="
        + offset
        + ", keyValue="
        + keyValue.toString()
        + "}";
  }
}
