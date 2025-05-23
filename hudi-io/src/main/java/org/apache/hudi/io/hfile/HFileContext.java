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

import org.apache.hudi.io.compress.CompressionCodec;
import org.apache.hudi.io.compress.HoodieDecompressor;
import org.apache.hudi.io.compress.HoodieDecompressorFactory;

/**
 * The context of HFile that contains information of the blocks.
 */
public class HFileContext {
  private final CompressionCodec compressionCodec;
  private final HoodieDecompressor decompressor;
  private final int blockSize;

  private HFileContext(CompressionCodec compressionCodec, int blockSize) {
    this.compressionCodec = compressionCodec;
    this.decompressor = HoodieDecompressorFactory.getDecompressor(compressionCodec);
    this.blockSize = blockSize;
  }

  public CompressionCodec getCompressionCodec() {
    return compressionCodec;
  }

  public HoodieDecompressor getDecompressor() {
    return decompressor;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private CompressionCodec compressionCodec = CompressionCodec.NONE;
    private int blockSize = 1024 * 1024;

    public Builder() {
    }

    public Builder blockSize(int blockSize) {
      this.blockSize = blockSize;
      return this;
    }

    public Builder compressionCodec(CompressionCodec compressionCodec) {
      this.compressionCodec = compressionCodec;
      return this;
    }

    public HFileContext build() {
      return new HFileContext(compressionCodec, blockSize);
    }
  }
}
