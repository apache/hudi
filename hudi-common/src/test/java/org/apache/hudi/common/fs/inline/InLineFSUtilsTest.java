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

package org.apache.hudi.common.fs.inline;

import org.apache.hudi.common.testutils.FileSystemTestUtils;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link InLineFileSystem}.
 */
public class InLineFSUtilsTest {

  private static Stream<Arguments> configParams() {
    Long[] data = new Long[] {
        0L,
        1000L,
        (long) Integer.MAX_VALUE + 1,
        Long.MAX_VALUE
    };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("configParams")
  void startOffset(long startOffset) {
    Path inlinePath =  FileSystemTestUtils.getPhantomFile(FileSystemTestUtils.getRandomOuterFSPath(), startOffset, 0L);
    assertEquals(startOffset, InLineFSUtils.startOffset(inlinePath));
  }

  @ParameterizedTest
  @MethodSource("configParams")
  void length(long inlineLength) {
    Path inlinePath =  FileSystemTestUtils.getPhantomFile(FileSystemTestUtils.getRandomOuterFSPath(), 0L, inlineLength);
    assertEquals(inlineLength, InLineFSUtils.length(inlinePath));
  }
}
