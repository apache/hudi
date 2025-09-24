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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class HFileMetaIndexBlock extends HFileIndexBlock {

  private HFileMetaIndexBlock(HFileContext context) {
    super(context, HFileBlockType.ROOT_INDEX);
  }

  public static HFileMetaIndexBlock createMetaIndexBlockToWrite(HFileContext context) {
    return new HFileMetaIndexBlock(context);
  }

  @Override
  public ByteBuffer getUncompressedBlockDataToWrite() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(context.getBlockSize());
    try (DataOutputStream outputStream = new DataOutputStream(baos)) {
      for (BlockIndexEntry entry : entries) {
        outputStream.writeLong(entry.getOffset());
        outputStream.writeInt(entry.getSize());
        // Key length.
        try {
          byte[] keyLength = getVariableLengthEncodedBytes(entry.getFirstKey().getLength());
          outputStream.write(keyLength);
        } catch (IOException e) {
          throw new RuntimeException(
              "Failed to serialize number: " + entry.getFirstKey().getLength());
        }
        // Note that: NO two-bytes for encoding key length.
        // Key.
        outputStream.write(entry.getFirstKey().getBytes());
      }
    }

    // Set metrics.
    byte[] allData = baos.toByteArray();
    blockDataSize = allData.length;
    return ByteBuffer.wrap(allData);
  }
}
