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

import org.apache.hudi.io.hfile.protobuf.generated.HFileProtos;
import org.apache.hudi.io.util.IOUtils;

import com.google.protobuf.ByteString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Represents a {@link HFileBlockType#FILE_INFO} block.
 */
public class HFileFileInfoBlock extends HFileBlock {
  // Magic we put ahead of a serialized protobuf message
  public static final byte[] PB_MAGIC = new byte[] {'P', 'B', 'U', 'F'};
  // Write properties
  private final Map<String, byte[]> fileInfoToWrite = new HashMap<>();

  public HFileFileInfoBlock(HFileContext context,
                            byte[] byteBuff,
                            int startOffsetInBuff) {
    super(context, HFileBlockType.FILE_INFO, byteBuff, startOffsetInBuff);
  }

  private HFileFileInfoBlock(HFileContext context) {
    super(context, HFileBlockType.FILE_INFO, -1L);
  }

  public static HFileFileInfoBlock createFileInfoBlockToWrite(HFileContext context) {
    return new HFileFileInfoBlock(context);
  }

  public HFileInfo readFileInfo() throws IOException {
    int pbMagicLength = PB_MAGIC.length;
    if (IOUtils.compareTo(PB_MAGIC, 0, pbMagicLength,
        byteBuff, startOffsetInBuff + HFILEBLOCK_HEADER_SIZE, pbMagicLength) != 0) {
      throw new IOException(
          "Unexpected Protobuf magic at the beginning of the HFileFileInfoBlock: "
              + fromUTF8Bytes(byteBuff, startOffsetInBuff + HFILEBLOCK_HEADER_SIZE, pbMagicLength));
    }
    ByteArrayInputStream inputStream = new ByteArrayInputStream(
        byteBuff,
        startOffsetInBuff + HFILEBLOCK_HEADER_SIZE + pbMagicLength, uncompressedSizeWithoutHeader);
    Map<UTF8StringKey, byte[]> fileInfoMap = new HashMap<>();
    HFileProtos.InfoProto infoProto = HFileProtos.InfoProto.parseDelimitedFrom(inputStream);
    for (HFileProtos.BytesBytesPair pair : infoProto.getMapEntryList()) {
      fileInfoMap.put(
          new UTF8StringKey(pair.getFirst().toByteArray()), pair.getSecond().toByteArray());
    }
    return new HFileInfo(fileInfoMap);
  }

  // ================ Below are for Write ================

  public void add(String name, byte[] value) {
    fileInfoToWrite.put(name, value);
  }

  @Override
  public ByteBuffer getUncompressedBlockDataToWrite() {
    ByteBuffer buff = ByteBuffer.allocate(context.getBlockSize() * 2);
    HFileProtos.InfoProto.Builder builder =
        HFileProtos.InfoProto.newBuilder();
    for (Map.Entry<String, byte[]> e : fileInfoToWrite.entrySet()) {
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
      buff.put(getVariableLengthEncodedBytes(payload.length));
    } catch (IOException e) {
      throw new RuntimeException("Failed to calculate File Info variable length");
    }
    buff.put(payload);
    buff.flip();
    return buff;
  }
}
