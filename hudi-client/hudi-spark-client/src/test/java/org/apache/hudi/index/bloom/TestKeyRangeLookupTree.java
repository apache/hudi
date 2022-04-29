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

package org.apache.hudi.index.bloom;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link KeyRangeLookupTree}.
 */
public class TestKeyRangeLookupTree {

  private static final Random RANDOM = new Random();
  private KeyRangeLookupTree keyRangeLookupTree;
  private Map<String, HashSet<String>> expectedMatches;

  public TestKeyRangeLookupTree() {
    keyRangeLookupTree = new KeyRangeLookupTree();
    expectedMatches = new HashMap<>();
  }

  /**
   * Tests for single node in the tree for different inputs.
   */
  @Test
  public void testFileGroupLookUpOneEntry() {
    KeyRangeNode toInsert = new KeyRangeNode(Long.toString(300), Long.toString(450), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    testRangeOfInputs(290, 305);
    testRangeOfInputs(390, 400);
    testRangeOfInputs(445, 455);
    testRangeOfInputs(600, 605);
  }

  /**
   * Tests for many entries in the tree with same start value and different end values.
   */
  @Test
  public void testFileGroupLookUpManyEntriesWithSameStartValue() {
    String startKey = Long.toString(120);
    long endKey = 250;
    KeyRangeNode toInsert = new KeyRangeNode(startKey, Long.toString(endKey), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    for (int i = 0; i < 10; i++) {
      endKey += 1 + RANDOM.nextInt(100);
      toInsert = new KeyRangeNode(startKey, Long.toString(endKey), UUID.randomUUID().toString());
      updateExpectedMatchesToTest(toInsert);
      keyRangeLookupTree.insert(toInsert);
    }
    testRangeOfInputs(110, endKey + 5);
  }

  /**
   * Tests for many duplicate entries in the tree.
   */
  @Test
  public void testFileGroupLookUpManyDuplicateEntries() {
    KeyRangeNode toInsert = new KeyRangeNode(Long.toString(1200), Long.toString(2000), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    for (int i = 0; i < 10; i++) {
      toInsert = new KeyRangeNode(Long.toString(1200), Long.toString(2000), UUID.randomUUID().toString());
      updateExpectedMatchesToTest(toInsert);
      keyRangeLookupTree.insert(toInsert);
    }
    testRangeOfInputs(1050, 1100);
    testRangeOfInputs(1500, 1600);
    testRangeOfInputs(1990, 2100);
  }

  // Tests helpers

  /**
   * Tests for curated entries in look up tree.
   */
  @Test
  public void testFileGroupLookUp() {

    // testing with hand curated inputs
    KeyRangeNode toInsert = new KeyRangeNode(Long.toString(500), Long.toString(600), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    toInsert = new KeyRangeNode(Long.toString(750), Long.toString(950), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    toInsert = new KeyRangeNode(Long.toString(120), Long.toString(620), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    toInsert = new KeyRangeNode(Long.toString(550), Long.toString(775), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    toInsert = new KeyRangeNode(Long.toString(725), Long.toString(850), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    toInsert = new KeyRangeNode(Long.toString(750), Long.toString(825), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    toInsert = new KeyRangeNode(Long.toString(750), Long.toString(990), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    toInsert = new KeyRangeNode(Long.toString(800), Long.toString(820), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    toInsert = new KeyRangeNode(Long.toString(200), Long.toString(550), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    toInsert = new KeyRangeNode(Long.toString(520), Long.toString(600), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    toInsert = new KeyRangeNode(Long.toString(120), Long.toString(620), UUID.randomUUID().toString());
    updateExpectedMatchesToTest(toInsert);
    keyRangeLookupTree.insert(toInsert);
    testRangeOfInputs(110, 999);
  }

  /**
   * Method to test the look up tree for different range of input keys.
   *
   * @param start starting value of the look up key
   * @param end end value of the look up tree
   */
  private void testRangeOfInputs(long start, long end) {
    for (long i = start; i <= end; i++) {
      String iStr = Long.toString(i);
      if (!expectedMatches.containsKey(iStr)) {
        assertEquals(Collections.EMPTY_SET, keyRangeLookupTree.getMatchingIndexFiles(iStr));
      } else {
        assertEquals(expectedMatches.get(iStr), keyRangeLookupTree.getMatchingIndexFiles(iStr));
      }
    }
  }

  /**
   * Updates the expected matches for a given {@link KeyRangeNode}.
   *
   * @param toInsert the {@link KeyRangeNode} to be inserted
   */
  private void updateExpectedMatchesToTest(KeyRangeNode toInsert) {
    long startKey = Long.parseLong(toInsert.getMinRecordKey());
    long endKey = Long.parseLong(toInsert.getMaxRecordKey());
    for (long i = startKey; i <= endKey; i++) {
      String iStr = Long.toString(i);
      if (!expectedMatches.containsKey(iStr)) {
        expectedMatches.put(iStr, new HashSet<>());
      }
      expectedMatches.get(iStr).add(toInsert.getFileNameList().get(0));
    }
  }

}
