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

package org.apache.hudi.io.util;

import org.apache.hudi.io.hfile.UTF8StringKey;

import org.junit.jupiter.api.Test;

import static org.apache.hudi.io.hfile.HFileUtils.isPrefixOfKey;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TestHFileUtils}.
 */
public class TestHFileUtils {
  @Test
  public void testIsPrefixOfKey() {
    assertTrue(isPrefixOfKey(new UTF8StringKey(""), new UTF8StringKey("")));
    assertTrue(isPrefixOfKey(new UTF8StringKey(""), new UTF8StringKey("abcdefg")));
    assertTrue(isPrefixOfKey(new UTF8StringKey("abc"), new UTF8StringKey("abcdefg")));
    assertTrue(isPrefixOfKey(new UTF8StringKey("abcdefg"), new UTF8StringKey("abcdefg")));
    assertFalse(isPrefixOfKey(new UTF8StringKey("abd"), new UTF8StringKey("abcdefg")));
    assertFalse(isPrefixOfKey(new UTF8StringKey("b"), new UTF8StringKey("abcdefg")));
    assertFalse(isPrefixOfKey(new UTF8StringKey("abcdefgh"), new UTF8StringKey("abcdefg")));
  }
}
