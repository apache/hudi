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

package org.apache.hudi.common.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestMapUtils {

  private static Stream<Arguments> containsAllArgs() {
    Map<String, String> m0 = new HashMap<>();
    m0.put("k0", "v0");
    m0.put("k1", "v1");
    m0.put("k2", "v2");
    Map<String, String> m1 = new HashMap<>();
    m1.put("k1", "v1");
    Map<String, String> m2 = new HashMap<>();
    m2.put("k2", "v2");
    m2.put("k", "v");
    Map<String, String> m3 = Collections.emptyMap();
    Map<String, String> m4 = new HashMap<>();
    m4.put("k0", null);
    Map<String, Integer> m5 = new HashMap<>();
    m5.put("k0", 0);

    List<Arguments> argsList = new ArrayList<>();

    argsList.add(Arguments.of(m0, m1, true));
    argsList.add(Arguments.of(m0, m3, true));
    argsList.add(Arguments.of(m5, m3, true));
    argsList.add(Arguments.of(m0, m4, false));
    argsList.add(Arguments.of(m0, m2, false));
    argsList.add(Arguments.of(m0, m5, false));

    return argsList.stream();
  }

  @ParameterizedTest
  @MethodSource("containsAllArgs")
  void containsAll(Map<?, ?> m1, Map<?, ?> m2, boolean expectedResult) {
    assertEquals(expectedResult, MapUtils.containsAll(m1, m2));
  }
}
