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

package org.apache.hudi.io.compress;

import org.apache.hudi.io.compress.airlift.HoodieAirliftGzipDecompressor;
import org.apache.hudi.io.compress.builtin.HoodieNoneDecompressor;

/**
 * Factory for {@link HoodieDecompressor}.
 */
public class HoodieDecompressorFactory {
  public static HoodieDecompressor getDecompressor(CompressionCodec compressionCodec) {
    switch (compressionCodec) {
      case NONE:
        return new HoodieNoneDecompressor();
      case GZIP:
        return new HoodieAirliftGzipDecompressor();
      default:
        throw new IllegalArgumentException(
            "The decompression is not supported for compression codec: " + compressionCodec);
    }
  }
}
