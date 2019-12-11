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

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests numeric utils.
 */
public class TestNumericUtils {

  @Test
  public void testHumanReadableByteCount() {
    assertTrue(NumericUtils.humanReadableByteCount(0).equals("0.0 B"));
    assertTrue(NumericUtils.humanReadableByteCount(27).equals("27.0 B"));
    assertTrue(NumericUtils.humanReadableByteCount(1023).equals("1023.0 B"));
    assertTrue(NumericUtils.humanReadableByteCount(1024).equals("1.0 KB"));
    assertTrue(NumericUtils.humanReadableByteCount(110592).equals("108.0 KB"));
    assertTrue(NumericUtils.humanReadableByteCount(28991029248L).equals("27.0 GB"));
    assertTrue(NumericUtils.humanReadableByteCount(1855425871872L).equals("1.7 TB"));
    assertTrue(NumericUtils.humanReadableByteCount(9223372036854775807L).equals("8.0 EB"));

  }
}
