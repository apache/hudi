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

  private HFileContext(CompressionCodec compressionCodec) {
    this.compressionCodec = compressionCodec;
    this.decompressor = HoodieDecompressorFactory.getDecompressor(compressionCodec);
  }

  CompressionCodec getCompressionCodec() {
    return compressionCodec;
  }

  HoodieDecompressor getDecompressor() {
    return decompressor;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private CompressionCodec compressionCodec = CompressionCodec.NONE;

    public Builder() {
    }

    public Builder compressionCodec(CompressionCodec compressionCodec) {
      this.compressionCodec = compressionCodec;
      return this;
    }

    public HFileContext build() {
      return new HFileContext(compressionCodec);
    }
  }
}
