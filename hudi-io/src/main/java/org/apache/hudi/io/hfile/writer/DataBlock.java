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

package org.apache.hudi.io.hfile.writer;

import org.apache.hudi.io.hfile.HFileBlockType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT16;

public class DataBlock extends Block {
  protected final List<KeyValueEntry> entries = new ArrayList<>();

  public DataBlock(int blockSize) {
    super(HFileBlockType.DATA.getMagic(), blockSize);
  }

  public DataBlock(byte[] magic, int blockSize) {
    super(magic, blockSize);
  }

  public boolean isEmpty() {
    return entries.isEmpty();
  }

  public void add(byte[] key, byte[] value) {
    KeyValueEntry kv = new KeyValueEntry(key, value);
    add(kv, true);
  }

  public int getNumOfEntries() {
    return entries.size();
  }

  protected void add(KeyValueEntry kv, boolean sorted) {
    entries.add(kv);
    if (sorted) {
      entries.sort(KeyValueEntry::compareTo);
    }
  }

  public byte[] getFirstKey() {
    return entries.get(0).key;
  }

  public byte[] getLastKey() {
    if (entries.isEmpty()) {
      return new byte[0];
    }
    return entries.get(entries.size() - 1).key;
  }

  @Override
  public ByteBuffer getPayload() {
    ByteBuffer dataBuf = ByteBuffer.allocate(blockSize);
    for (KeyValueEntry kv : entries) {
      // Length of key + length of a short variable indicating length of key;
      dataBuf.putInt(kv.key.length + SIZEOF_INT16);
      // Length of value;
      dataBuf.putInt(kv.value.length);
      // Key content length;
      dataBuf.putShort((short)kv.key.length);
      // Key.
      dataBuf.put(kv.key);
      // Value.
      dataBuf.put(kv.value);
      // MVCC.
      dataBuf.put((byte)0);
    }
    dataBuf.flip();
    return dataBuf;
  }
}
