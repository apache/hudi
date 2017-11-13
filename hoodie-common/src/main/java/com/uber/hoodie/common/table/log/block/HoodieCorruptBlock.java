/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.table.log.block;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Corrupt block is emitted whenever the scanner finds the length of the block written at the
 * beginning does not match (did not find a EOF or a sync marker after the length)
 */
public class HoodieCorruptBlock extends HoodieLogBlock {

  private final byte[] corruptedBytes;

  private HoodieCorruptBlock(byte[] corruptedBytes, Map<LogMetadataType, String> metadata) {
    super(metadata);
    this.corruptedBytes = corruptedBytes;
  }

  private HoodieCorruptBlock(byte[] corruptedBytes) {
    this(corruptedBytes, null);
  }

  @Override
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);
    if (super.getLogMetadata() != null) {
      output.write(HoodieLogBlock.getLogMetadataBytes(super.getLogMetadata()));
    }
    output.write(corruptedBytes);
    return baos.toByteArray();
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.CORRUPT_BLOCK;
  }

  public byte[] getCorruptedBytes() {
    return corruptedBytes;
  }

  public static HoodieLogBlock fromBytes(byte[] content, int blockSize, boolean readMetadata)
      throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(content));
    Map<LogMetadataType, String> metadata = null;
    int bytesRemaining = blockSize;
    if (readMetadata) {
      try { //attempt to read metadata
        metadata = HoodieLogBlock.getLogMetadata(dis);
        bytesRemaining = blockSize - HoodieLogBlock.getLogMetadataBytes(metadata).length;
      } catch (IOException e) {
        // unable to read metadata, possibly corrupted
        metadata = null;
      }
    }
    byte[] corruptedBytes = new byte[bytesRemaining];
    dis.readFully(corruptedBytes);
    return new HoodieCorruptBlock(corruptedBytes, metadata);
  }
}
