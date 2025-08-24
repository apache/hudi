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

package org.apache.hudi.avro;

import org.apache.avro.util.Utf8;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestAvroRecordContext {

  private static Stream<Arguments> testConvertValueToEngineType() {
    return Stream.of(
        Arguments.of(1L, 1L),
        Arguments.of("test", "test"),
        Arguments.of(new Utf8("utf8_string"), "utf8_string"),
        Arguments.of(1.23, 1.23));
  }

  @ParameterizedTest
  @MethodSource
  void testConvertValueToEngineType(Comparable input, Comparable expected) {
    Comparable actual = AvroRecordContext.getFieldAccessorInstance().convertValueToEngineType(input);
    assertEquals(expected, actual);
  }
}
