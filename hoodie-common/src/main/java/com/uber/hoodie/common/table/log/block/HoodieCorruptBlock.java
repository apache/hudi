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

import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.exception.HoodieException;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Corrupt block is emitted whenever the scanner finds the length of the block written at the
 * beginning does not match (did not find a EOF or a sync marker after the length)
 */
public class HoodieCorruptBlock extends HoodieLogBlock {

  private byte[] corruptedBytes;

  private HoodieCorruptBlock(Optional<HoodieLogBlockContentLocation> blockLocation,
                             Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    super(header, footer, blockLocation);
  }

  private HoodieCorruptBlock(byte [] corruptedBytes,
                             Map<HeaderMetadataType, String> header, Map<HeaderMetadataType, String> footer) {
    super(header, footer, Optional.ofNullable(null));
    this.corruptedBytes = corruptedBytes;
  }

  @Override
  public byte[] getContentBytes() throws IOException {

    if(corruptedBytes == null) {
      throw new HoodieException("corrupted bytes is empty, use HoodieLazyBlockReader to read contents lazily");
    }
    return corruptedBytes;
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.CORRUPT_BLOCK;
  }

  public static HoodieLogBlock getBlock(byte [] corruptedBytes,
                                        Map<HeaderMetadataType, String> header,
                                        Map<HeaderMetadataType, String> footer) throws IOException {

    return new HoodieCorruptBlock(corruptedBytes, header, footer);
  }

  public static HoodieLogBlock getBlock(HoodieLogFile logFile,
                                        long position,
                                        long blockSize,
                                        Map<HeaderMetadataType, String> header,
                                        Map<HeaderMetadataType, String> footer) throws IOException {

    return new HoodieCorruptBlock(Optional.of(new HoodieLogBlockContentLocation(logFile, position, blockSize)), header, footer);
  }
}
