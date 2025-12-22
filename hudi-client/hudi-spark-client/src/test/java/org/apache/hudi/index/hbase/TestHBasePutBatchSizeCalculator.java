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

package org.apache.hudi.index.hbase;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHBasePutBatchSizeCalculator {

  @Test
  public void testPutBatchSizeCalculation() {
    SparkHoodieHBaseIndex.HBasePutBatchSizeCalculator batchSizeCalculator = new SparkHoodieHBaseIndex
                                                                                    .HBasePutBatchSizeCalculator();
    // All asserts cases below are derived out of the first
    // example below, with change in one parameter at a time.
    int putBatchSize = batchSizeCalculator.getBatchSize(10, 16667, 1200, 200, 0.1f);
    // Total puts that can be sent  in 1 second = (10 * 16667 * 0.1) = 16,667
    // Total puts per batch will be (16,667 / parallelism) = 83.335, where 200 is the maxExecutors
    assertEquals(putBatchSize, 84);

    // Number of Region Servers are halved, total requests sent in a second are also halved, so batchSize is also halved
    int putBatchSize2 = batchSizeCalculator.getBatchSize(5, 16667, 1200, 200, 0.1f);
    assertEquals(putBatchSize2, 42);

    // If the parallelism is halved, batchSize has to double
    int putBatchSize3 = batchSizeCalculator.getBatchSize(10, 16667, 1200, 100, 0.1f);
    assertEquals(putBatchSize3, 167);

    // If the parallelism is halved, batchSize has to double.
    // This time parallelism is driven by numTasks rather than numExecutors
    int putBatchSize4 = batchSizeCalculator.getBatchSize(10, 16667, 100, 200, 0.1f);
    assertEquals(putBatchSize4, 167);

    // If sleepTimeMs is halved, batchSize has to halve
    int putBatchSize5 = batchSizeCalculator.getBatchSize(10, 16667, 1200, 200, 0.05f);
    assertEquals(putBatchSize5, 42);

    // If maxQPSPerRegionServer is doubled, batchSize also doubles
    int putBatchSize6 = batchSizeCalculator.getBatchSize(10, 33334, 1200, 200, 0.1f);
    assertEquals(putBatchSize6, 167);
  }

}
