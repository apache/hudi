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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test cases for {@link LSMTimeline}.
 */
public class TestLSMTimeline {
  @Test
  void testParseMinInstantTime() {
    String fileName = "001_002_0.parquet";
    String minInstantTime = LSMTimeline.getMinInstantTime(fileName);
    assertThat(minInstantTime, is("001"));
    assertThrows(HoodieException.class, () -> LSMTimeline.getMinInstantTime("invalid_file_name.parquet"));
  }

  @Test
  void testParseMaxInstantTime() {
    String fileName = "001_002_0.parquet";
    String maxInstantTime = LSMTimeline.getMaxInstantTime(fileName);
    assertThat(maxInstantTime, is("002"));
    assertThrows(HoodieException.class, () -> LSMTimeline.getMaxInstantTime("invalid_file_name.parquet"));
  }

  @Test
  void testParseFileLayer() {
    String fileName = "001_002_0.parquet";
    int layer = LSMTimeline.getFileLayer(fileName);
    assertThat(layer, is(0));
    assertThat("for invalid file name, returns 0", LSMTimeline.getFileLayer("invalid_file_name.parquet"), is(0));
  }
}
