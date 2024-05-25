/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format.cow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.Arrays;
import java.util.Comparator;
import org.apache.hadoop.fs.BlockLocation;
import org.junit.jupiter.api.Test;

public class TestBlockLocationSort {

  private static BlockLocation createBlockLocation(int offset, int length) {
    return new BlockLocation(new String[0], new String[0], offset, length);
  }

  @Test
  void testBlockLocationSort() {
    BlockLocation o1 = createBlockLocation(0, 5);
    BlockLocation o2 = createBlockLocation(6, 4);
    BlockLocation o3 = createBlockLocation(5, 5);

    BlockLocation[] blocks = {o1, o2, o3};
    BlockLocation[] sortedBlocks = {o1, o3, o2};

    Arrays.sort(blocks, Comparator.comparingLong(BlockLocation::getOffset));
    assertThat(blocks, equalTo(sortedBlocks));

    // Sort again to ensure idempotency
    Arrays.sort(blocks, Comparator.comparingLong(BlockLocation::getOffset));
    assertThat(blocks, equalTo(sortedBlocks));
  }

}
