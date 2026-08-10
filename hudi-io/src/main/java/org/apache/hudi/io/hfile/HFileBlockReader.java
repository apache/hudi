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

import org.apache.hudi.io.SeekableDataInputStream;

import java.io.EOFException;
import java.io.IOException;

/**
 * A reader to read one or more HFile blocks based on the start and end offsets.
 */
public class HFileBlockReader {
  private final HFileContext context;
  private final long streamStartOffset;
  private final SeekableDataInputStream stream;
  private final byte[] byteBuff;
  private int offset;
  private boolean isReadFully = false;

  /**
   * Instantiates the {@link HFileBlockReader}.
   *
   * @param context     HFile context.
   * @param stream      input data.
   * @param startOffset start offset to read from.
   * @param endOffset   end offset to stop at.
   */
  public HFileBlockReader(HFileContext context,
                          SeekableDataInputStream stream,
                          long startOffset,
                          long endOffset) {
    this.context = context;
    this.stream = stream;
    this.streamStartOffset = startOffset;
    this.offset = 0;
    long length = endOffset - startOffset;
    if (length >= 0 && length <= Integer.MAX_VALUE) {
      this.byteBuff = new byte[(int) length];
    } else {
      throw new IllegalArgumentException(
          "The range of bytes is too large or invalid: ["
              + startOffset + ", " + endOffset + "], length=" + length);
    }
  }

  /**
   * Reads the next block based on the expected block type.
   *
   * @param expectedBlockType expected block type.
   * @return {@link HFileBlock} instance matching the expected block type.
   * @throws IOException if the type of next block does not match the expected type.
   */
  public HFileBlock nextBlock(HFileBlockType expectedBlockType) throws IOException {
    if (offset >= byteBuff.length) {
      throw new EOFException("No more data to read");
    }

    if (!isReadFully) {
      // Full range of bytes are read fully into a byte array
      stream.seek(streamStartOffset);
      stream.readFully(byteBuff);
      isReadFully = true;
    }

    HFileBlock block = HFileBlock.parse(context, byteBuff, offset);
    block.unpack();

    if (block.getBlockType() != expectedBlockType) {
      throw new IOException("Unexpected block type: " + block.getBlockType()
          + "; expecting " + expectedBlockType);
    }

    offset += block.getOnDiskSizeWithHeader();
    return block;
  }
}
