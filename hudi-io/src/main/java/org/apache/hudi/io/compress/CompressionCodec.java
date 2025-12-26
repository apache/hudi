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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Available compression codecs.
 * There should not be any assumption on the ordering or ordinal of the defined enums.
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@Getter
public enum CompressionCodec {
  NONE("none", 2),
  BZIP2("bz2", 5),
  GZIP("gz", 1),
  LZ4("lz4", 4),
  LZO("lzo", 0),
  SNAPPY("snappy", 3),
  ZSTD("zstd", 6);

  private static final Map<String, CompressionCodec>
      NAME_TO_COMPRESSION_CODEC_MAP = createNameToCompressionCodecMap();
  private static final Map<Integer, CompressionCodec>
      ID_TO_COMPRESSION_CODEC_MAP = createIdToCompressionCodecMap();

  private final String name;
  // CompressionCodec ID to be stored in HFile on storage
  // The ID of each codec cannot change or else that breaks all existing HFiles out there
  // even the ones that are not compressed! (They use the NONE algorithm)
  private final int id;

  public static CompressionCodec findCodecByName(String name) {
    CompressionCodec codec =
        NAME_TO_COMPRESSION_CODEC_MAP.get(name.toLowerCase());
    ValidationUtils.checkArgument(
        codec != null, String.format("Cannot find compression codec: %s", name));
    return codec;
  }

  /**
   * Gets the compression codec based on the ID.  This ID is written to the HFile on storage.
   *
   * @param id ID indicating the compression codec
   * @return compression codec based on the ID
   */
  public static CompressionCodec decodeCompressionCodec(int id) {
    CompressionCodec codec = ID_TO_COMPRESSION_CODEC_MAP.get(id);
    ValidationUtils.checkArgument(
        codec != null, "Compression code not found for ID: " + id);
    return codec;
  }

  /**
   * @return the mapping of name to compression codec.
   */
  private static Map<String, CompressionCodec> createNameToCompressionCodecMap() {
    return Collections.unmodifiableMap(
        Arrays.stream(CompressionCodec.values())
            .collect(Collectors.toMap(CompressionCodec::getName, Function.identity()))
    );
  }

  /**
   * @return the mapping of ID to compression codec.
   */
  private static Map<Integer, CompressionCodec> createIdToCompressionCodecMap() {
    return Collections.unmodifiableMap(
        Arrays.stream(CompressionCodec.values())
            .collect(Collectors.toMap(CompressionCodec::getId, Function.identity()))
    );
  }
}
