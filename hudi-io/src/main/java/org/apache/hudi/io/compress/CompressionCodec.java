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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Available compression codecs.
 * There should not be any assumption on the ordering or ordinal of the defined enums.
 */
public enum CompressionCodec {
  NONE("none"),
  BZIP2("bz2"),
  GZIP("gz"),
  LZ4("lz4"),
  LZO("lzo"),
  SNAPPY("snappy"),
  ZSTD("zstd");

  private static final Map<String, CompressionCodec>
      HFILE_NAME_TO_COMPRESSION_CODEC_MAP = createNameToCompressionCodecMap();

  private final String name;

  CompressionCodec(final String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static CompressionCodec findCodecByName(String name) {
    CompressionCodec codec =
        HFILE_NAME_TO_COMPRESSION_CODEC_MAP.getOrDefault(name.toLowerCase(), null);
    if (codec != null) {
      return codec;
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot find compression codec: %s", name));
    }
  }

  /**
   * Create a mapping from its name to the compression codec.
   */
  private static Map<String, CompressionCodec> createNameToCompressionCodecMap() {
    Map<String, CompressionCodec> result = new HashMap<>();
    result.put(LZO.getName(), LZO);
    result.put(GZIP.getName(), GZIP);
    result.put(NONE.getName(), NONE);
    result.put(SNAPPY.getName(), SNAPPY);
    result.put(LZ4.getName(), LZ4);
    result.put(BZIP2.getName(), BZIP2);
    result.put(ZSTD.getName(), ZSTD);
    return Collections.unmodifiableMap(result);
  }
}
