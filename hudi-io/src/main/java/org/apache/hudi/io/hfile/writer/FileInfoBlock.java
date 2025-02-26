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
import org.apache.hudi.io.hfile.protobuf.generated.HFileProtos;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.io.hfile.HFileFileInfoBlock.PB_MAGIC;

public class FileInfoBlock extends Block {
  private final Map<String, byte[]> entries = new HashMap<>();

  public FileInfoBlock(int blockSize) {
    super(HFileBlockType.FILE_INFO.getMagic(), blockSize);
  }

  public void add(String name, byte[] value) {
    entries.put(name, value);
  }

  @Override
  public ByteBuffer getPayload() {
    ByteBuffer buff = ByteBuffer.allocate(blockSize * 2);
    HFileProtos.InfoProto.Builder builder =
        HFileProtos.InfoProto.newBuilder();
    for (Map.Entry<String, byte[]> e : entries.entrySet()) {
      HFileProtos.BytesBytesPair bbp = HFileProtos.BytesBytesPair
          .newBuilder()
          .setFirst(ByteString.copyFrom(getUTF8Bytes(e.getKey())))
          .setSecond(ByteString.copyFrom(e.getValue()))
          .build();
      builder.addMapEntry(bbp);
    }
    buff.put(PB_MAGIC);
    byte[] payload = builder.build().toByteArray();
    try {
      buff.put(getVariableLengthEncodes(payload.length));
    } catch (IOException e) {
      throw new RuntimeException("Failed to calculate File Info variable length");
    }
    buff.put(payload);
    buff.flip();
    return buff;
  }
}