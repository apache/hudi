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

package org.apache.hudi.common.util;

import org.apache.hudi.io.compress.CompressionCodec;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.table.log.block.HoodieHFileDataBlock.HFILE_COMPRESSION_ALGO_PARAM_KEY;
import static org.apache.hudi.common.util.HFileUtils.getHFileCompressionAlgorithm;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HFileUtils}
 */
public class TestHFileUtils {
  @ParameterizedTest
  @EnumSource(CompressionCodec.class)
  public void testGetHFileCompressionAlgorithm(CompressionCodec codec) {
    Map<CompressionCodec, Compression.Algorithm> expectedAlgoMap = new HashMap<>();
    expectedAlgoMap.put(CompressionCodec.NONE, Compression.Algorithm.NONE);
    expectedAlgoMap.put(CompressionCodec.BZIP2, Compression.Algorithm.BZIP2);
    expectedAlgoMap.put(CompressionCodec.GZIP, Compression.Algorithm.GZ);
    expectedAlgoMap.put(CompressionCodec.LZ4, Compression.Algorithm.LZ4);
    expectedAlgoMap.put(CompressionCodec.LZO, Compression.Algorithm.LZO);
    expectedAlgoMap.put(CompressionCodec.SNAPPY, Compression.Algorithm.SNAPPY);
    expectedAlgoMap.put(CompressionCodec.ZSTD, Compression.Algorithm.ZSTD);

    for (boolean upperCase : new boolean[] {true, false}) {
      Map<String, String> paramsMap = Collections.singletonMap(
          HFILE_COMPRESSION_ALGO_PARAM_KEY,
          upperCase ? codec.getName().toUpperCase() : codec.getName().toLowerCase());
      assertEquals(expectedAlgoMap.get(codec), getHFileCompressionAlgorithm(paramsMap));
    }
  }

  @Test
  public void testGetDefaultHFileCompressionAlgorithm() {
    assertEquals(Compression.Algorithm.GZ, getHFileCompressionAlgorithm(Collections.emptyMap()));
  }
}
