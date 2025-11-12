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

package org.apache.hudi.common.util.collection;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

public class TestFilterIterator {

  @Test
  public void testFilter() {
    // Create a list of integers
    List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
    // Create a filter that filters out even numbers
    Predicate<Integer> filter = i -> i % 2 != 0;
    // Create a filter iterator
    FilterIterator<Integer> filterIterator = new FilterIterator<>(list.iterator(), filter);
    // Create a list to store the filtered elements
    List<Integer> filteredList = new ArrayList<>();
    // Iterate over the filter iterator
    while (filterIterator.hasNext()) {
      filteredList.add(filterIterator.next());
    }
    // Assert that the filtered list contains only odd numbers
    assertEquals(Arrays.asList(1, 3, 5), filteredList);
  }

  @Test
  public void testFilterFailed() {
    Iterator<Integer> i1 = Collections.emptyIterator(); // empty iterator

    FilterIterator<Integer> ci = new FilterIterator<>(i1, i -> true);
    assertFalse(ci.hasNext());
    try {
      ci.next();
      fail("expected error for empty iterator");
    } catch (IllegalArgumentException e) {
      // no-op
    }
  }

}
