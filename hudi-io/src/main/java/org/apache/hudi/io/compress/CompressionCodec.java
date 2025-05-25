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

import org.apache.hudi.common.util.ValidationUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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
      NAME_TO_COMPRESSION_CODEC_MAP = createNameToCompressionCodecMap();

  private final String name;

  CompressionCodec(final String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static CompressionCodec findCodecByName(String name) {
    CompressionCodec codec =
        NAME_TO_COMPRESSION_CODEC_MAP.get(name.toLowerCase());
    ValidationUtils.checkArgument(
        codec != null, String.format("Cannot find compression codec: %s", name));
    return codec;
  }

  /**
   * Create a mapping from its name to the compression codec.
   */
  private static Map<String, CompressionCodec> createNameToCompressionCodecMap() {
    return Collections.unmodifiableMap(
        Arrays.stream(CompressionCodec.values())
            .collect(Collectors.toMap(CompressionCodec::getName, Function.identity()))
    );
  }
}
