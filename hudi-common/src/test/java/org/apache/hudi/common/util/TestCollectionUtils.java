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

package org.apache.hudi.common.util;

import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.util.CollectionUtils.batches;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestCollectionUtils {

  @Test
  void getBatchesFromList() {
    assertThrows(IllegalArgumentException.class, () -> {
      batches(Collections.emptyList(), -1);
    });

    assertThrows(IllegalArgumentException.class, () -> {
      batches(Collections.emptyList(), 0);
    });

    assertEquals(Collections.emptyList(), batches(Collections.emptyList(), 1));

    List<List<Integer>> intsBatches1 = batches(Arrays.asList(1, 2, 3, 4, 5, 6), 3);
    assertEquals(2, intsBatches1.size());
    assertEquals(Arrays.asList(1, 2, 3), intsBatches1.get(0));
    assertEquals(Arrays.asList(4, 5, 6), intsBatches1.get(1));

    List<List<Integer>> intsBatches2 = batches(Arrays.asList(1, 2, 3, 4, 5, 6), 5);
    assertEquals(2, intsBatches2.size());
    assertEquals(Arrays.asList(1, 2, 3, 4, 5), intsBatches2.get(0));
    assertEquals(Collections.singletonList(6), intsBatches2.get(1));
  }

  @Test
  void testSuccessfulMultithreadedFor() {
    List<Boolean> flags = Arrays.asList(false, false, false);
    CollectionUtils.forEachParallel(Arrays.asList(0, 1, 2), (Integer i) -> flags.set(i, true));
    flags.forEach(f -> assertEquals(true, f));
  }

  @Test
  void testMultithreadedForCompletionOnError() {
    Boolean[] flags = new Boolean[10];
    //make shuffled list containing numbers 0 to 99
    List<Integer> range = IntStream.range(0, 100)
        .boxed().collect(Collectors.toList());
    Collections.shuffle(range, new Random(432543542));

    //test forEachParallel
    HoodieException hoodieException = assertThrows(HoodieException.class, () -> CollectionUtils.forEachParallel(range, (Integer i) -> {
      if (i % 10 == 0) {
        //set flags[0 to 9] to true
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        flags[i / 10] = Boolean.TRUE;
      } else {
        //throw exceptions for all i that don't end in a 0
        throw new RuntimeException("FAILURE<" + i + ">");
      }
    }));
    //Ensure that all the expected failures are recorded and that expected successes don't throw exceptions
    Arrays.asList(flags).forEach(f -> assertEquals(true, f));
    range.forEach(i -> assertEquals(i % 10 != 0, hoodieException.getMessage().contains("FAILURE<" + i + ">")));
  }

}
