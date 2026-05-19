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

package org.apache.hudi

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class TestHoodieCLIUtils {

  @Test
  def testExtractOptionsBasic(): Unit = {
    val parsed = HoodieCLIUtils.extractOptions("k1=v1,k2=v2")
    assertEquals(2, parsed.size)
    assertEquals("v1", parsed("k1"))
    assertEquals("v2", parsed("k2"))
  }

  @Test
  def testExtractOptionsTrimsWhitespace(): Unit = {
    val parsed = HoodieCLIUtils.extractOptions(" k1 = v1 ,  k2= v 2 ")
    assertEquals("v1", parsed("k1"))
    // internal whitespace inside value is preserved, only edges are trimmed
    assertEquals("v 2", parsed("k2"))
  }

  @Test
  def testExtractOptionsIgnoresEmptyTokens(): Unit = {
    // trailing comma, consecutive commas, leading comma — all silently ignored
    val parsed = HoodieCLIUtils.extractOptions(",k1=v1,, ,k2=v2,")
    assertEquals(2, parsed.size)
    assertEquals("v1", parsed("k1"))
    assertEquals("v2", parsed("k2"))
  }

  @Test
  def testExtractOptionsValueContainsEquals(): Unit = {
    // only the first `=` should be treated as a delimiter
    val parsed = HoodieCLIUtils.extractOptions("k=a=b=c")
    assertEquals(1, parsed.size)
    assertEquals("a=b=c", parsed("k"))
  }

  @Test
  def testExtractOptionsAllowsEmptyValue(): Unit = {
    val parsed = HoodieCLIUtils.extractOptions("k=")
    assertEquals(1, parsed.size)
    assertEquals("", parsed("k"))
  }

  @Test
  def testExtractOptionsDuplicateKeyLastWins(): Unit = {
    val parsed = HoodieCLIUtils.extractOptions("k=v1,k=v2,k=v3")
    assertEquals(1, parsed.size)
    assertEquals("v3", parsed("k"))
  }

  @Test
  def testExtractOptionsNullAndEmpty(): Unit = {
    assertTrue(HoodieCLIUtils.extractOptions(null).isEmpty)
    assertTrue(HoodieCLIUtils.extractOptions("").isEmpty)
    assertTrue(HoodieCLIUtils.extractOptions("   ").isEmpty)
    assertTrue(HoodieCLIUtils.extractOptions(",,, ").isEmpty)
  }

  @Test
  def testExtractOptionsThrowsOnMissingDelimiter(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () => HoodieCLIUtils.extractOptions("k1=v1,invalid"))
    assertTrue(ex.getMessage.contains("invalid"))
  }

  @Test
  def testExtractOptionsThrowsOnEmptyKey(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () => HoodieCLIUtils.extractOptions("=v"))
    assertTrue(ex.getMessage.contains("key=value") || ex.getMessage.contains("Option key"))
  }

  @Test
  def testExtractOptionsThrowsOnWhitespaceKey(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => HoodieCLIUtils.extractOptions("   =v"))
  }
}