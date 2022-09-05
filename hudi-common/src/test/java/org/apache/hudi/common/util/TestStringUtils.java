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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStringUtils {

  private static final String[] STRINGS = {"This", "is", "a", "test"};

  @Test
  public void testStringJoinWithDelim() {
    String joinedString = StringUtils.joinUsingDelim("-", STRINGS);
    assertEquals(STRINGS.length, joinedString.split("-").length);
  }

  @Test
  public void testStringJoin() {
    assertNotEquals(null, StringUtils.join(""));
    assertNotEquals(null, StringUtils.join(STRINGS));
  }

  @Test
  public void testStringJoinWithJavaImpl() {
    assertNull(StringUtils.join(",", null));
    assertEquals("", String.join(",", Collections.singletonList("")));
    assertEquals(",", String.join(",", Arrays.asList("", "")));
    assertEquals("a,", String.join(",", Arrays.asList("a", "")));
  }

  @Test
  public void testStringNullToEmpty() {
    String str = "This is a test";
    assertEquals(str, StringUtils.nullToEmpty(str));
    assertEquals("", StringUtils.nullToEmpty(null));
  }

  @Test
  public void testStringObjToString() {
    assertNull(StringUtils.objToString(null));
    assertEquals("Test String", StringUtils.objToString("Test String"));

    // assert byte buffer
    ByteBuffer byteBuffer1 = ByteBuffer.wrap("1234".getBytes());
    ByteBuffer byteBuffer2 = ByteBuffer.wrap("5678".getBytes());
    // assert equal because ByteBuffer has overwritten the toString to return a summary string
    assertEquals(byteBuffer1.toString(), byteBuffer2.toString());
    // assert not equal
    assertNotEquals(StringUtils.objToString(byteBuffer1), StringUtils.objToString(byteBuffer2));
  }

  @Test
  public void testStringEmptyToNull() {
    assertNull(StringUtils.emptyToNull(""));
    assertEquals("Test String", StringUtils.emptyToNull("Test String"));
  }

  @Test
  public void testStringNullOrEmpty() {
    assertTrue(StringUtils.isNullOrEmpty(null));
    assertTrue(StringUtils.isNullOrEmpty(""));
    assertNotEquals(null, StringUtils.isNullOrEmpty("this is not empty"));
    assertTrue(StringUtils.isNullOrEmpty(""));
  }

  @Test
  public void testSplit() {
    assertEquals(new ArrayList<>(), StringUtils.split(null, ","));
    assertEquals(new ArrayList<>(), StringUtils.split("", ","));
    assertEquals(Arrays.asList("a", "b", "c"), StringUtils.split("a,b, c", ","));
    assertEquals(Arrays.asList("a", "b", "c"), StringUtils.split("a,b,, c ", ","));
  }
}
