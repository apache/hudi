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

import static org.apache.hudi.io.hfile.DataSize.SIZEOF_SHORT;

/**
 * Represents the key part only.
 */
public class KeyOnlyKeyValue extends KeyValue {
  public KeyOnlyKeyValue(byte[] bytes) {
    this(bytes, 0, bytes.length);
  }

  public KeyOnlyKeyValue(byte[] bytes, int offset, int length) {
    super(bytes, offset, length);
  }

  @Override
  public int getKeyOffset() {
    return this.offset;
  }

  @Override
  public int getRowOffset() {
    return getKeyOffset() + SIZEOF_SHORT;
  }

  @Override
  public int getKeyLength() {
    return length;
  }

  @Override
  public int getValueOffset() {
    throw new IllegalArgumentException("KeyOnlyKeyValue does not work with values.");
  }

  @Override
  public int getValueLength() {
    throw new IllegalArgumentException("KeyOnlyKeyValue does not work with values.");
  }
}
