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

package org.apache.hudi.utils;

import org.apache.hudi.client.utils.ConcatenatingIterator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

public class TestConcatenatingIterator {

  // Simple test for iterator concatenation
  @Test
  public void testConcatBasic() {
    Iterator<Integer> i1 = Arrays.asList(5, 3, 2, 1).iterator();
    Iterator<Integer> i2 = Collections.emptyIterator(); // empty iterator
    Iterator<Integer> i3 = Collections.singletonList(3).iterator();

    ConcatenatingIterator<Integer> ci = new ConcatenatingIterator<>(Arrays.asList(i1, i2, i3));
    List<Integer> allElements = new ArrayList<>();
    while (ci.hasNext()) {
      allElements.add(ci.next());
    }

    assertEquals(5, allElements.size());
    assertEquals(Arrays.asList(5, 3, 2, 1, 3), allElements);
  }

  @Test
  public void testConcatError() {
    Iterator<Integer> i1 = Collections.emptyIterator(); // empty iterator

    ConcatenatingIterator<Integer> ci = new ConcatenatingIterator<>(Collections.singletonList(i1));
    assertFalse(ci.hasNext());
    try {
      ci.next();
      fail("expected error for empty iterator");
    } catch (IllegalArgumentException e) {
      //
    }
  }
}