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

import org.apache.hadoop.hbase.io.compress.Compression;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME;
import static org.apache.hudi.common.util.HFileUtils.getHFileCompressionAlgorithm;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HFileUtils}
 */
public class TestHFileUtils {
  @ParameterizedTest
  @EnumSource(Compression.Algorithm.class)
  public void testGetHFileCompressionAlgorithm(Compression.Algorithm algo) {
    for (boolean upperCase : new boolean[] {true, false}) {
      Map<String, String> paramsMap = Collections.singletonMap(
          HFILE_COMPRESSION_ALGORITHM_NAME.key(),
          upperCase ? algo.getName().toUpperCase() : algo.getName().toLowerCase());
      assertEquals(algo, getHFileCompressionAlgorithm(paramsMap));
    }
  }

  @Test
  public void testGetHFileCompressionAlgorithmWithEmptyString() {
    assertEquals(Compression.Algorithm.GZ, getHFileCompressionAlgorithm(
        Collections.singletonMap(HFILE_COMPRESSION_ALGORITHM_NAME.key(), "")));
  }

  @Test
  public void testGetDefaultHFileCompressionAlgorithm() {
    assertEquals(Compression.Algorithm.GZ, getHFileCompressionAlgorithm(Collections.emptyMap()));
  }
}
