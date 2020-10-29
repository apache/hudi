/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

public class TestIndexCompatibility extends HoodieClientTestBase {

  private static Iterable<Object[]> indexTypeCompatibleParameter() {
    return Arrays.asList(new Object[][] { { "GLOBAL_BLOOM", "GLOBAL_BLOOM" }, { "GLOBAL_BLOOM", "BLOOM" },
        { "GLOBAL_BLOOM", "SIMPLE" }, { "GLOBAL_BLOOM", "GLOBAL_SIMPLE" }, { "GLOBAL_SIMPLE", "GLOBAL_SIMPLE" },
        { "GLOBAL_SIMPLE", "GLOBAL_BLOOM" }, { "SIMPLE", "SIMPLE" }, { "BLOOM", "BLOOM" }, { "HBASE", "HBASE" },
        { "CUSTOM", "CUSTOM" } });
  }

  private static Iterable<Object[]> indexTypeNotCompatibleParameter() {
    return Arrays.asList(new Object[][] { { "SIMPLE", "BLOOM"}, { "SIMPLE", "GLOBAL_BLOOM"},
        { "BLOOM", "GLOBAL_BLOOM"}, { "CUSTOM", "BLOOM"}, { "CUSTOM", "GLOBAL_BLOOM"}, { "CUSTOM", "HBASE"}});
  }

  @ParameterizedTest
  @MethodSource("indexTypeCompatibleParameter")
  public void testTableIndexTypeCompatible(String persistIndexType, String writeIndexType) {
    assertDoesNotThrow(() -> {
      HoodieIndexUtils.checkIndexTypeCompatible(IndexType.valueOf(writeIndexType), IndexType.valueOf(persistIndexType));
    }, "");
  }

  @ParameterizedTest
  @MethodSource("indexTypeNotCompatibleParameter")
  public void testTableIndexTypeNotCompatible(String persistIndexType, String writeIndexType) {
    assertThrows(HoodieException.class, () -> {
      HoodieIndexUtils.checkIndexTypeCompatible(IndexType.valueOf(writeIndexType), IndexType.valueOf(persistIndexType));
    }, "");
  }
}
