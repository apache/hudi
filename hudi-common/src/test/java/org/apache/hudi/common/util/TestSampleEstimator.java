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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSampleEstimator {

  @Test
  public void testSampleEstimator() {
    SampleEstimator estimator = new SampleEstimator(100, 0.1);
    long estimatedSize = 0;
    for (int i = 0; i < 1000; i++) {
      estimatedSize = estimator.estimate(() -> 100L);
      assertEquals(100, estimatedSize);
      assertEquals(i / 100, estimator.getSampleCount());
    }

    for (int i = 1000; i < 2000; i++) {
      long previousEstimatedSize = estimatedSize;
      estimatedSize = estimator.estimate(() -> 200L);
      assertTrue(estimatedSize > 100 && estimatedSize < 200);
      if (i % 100 == 0) {
        assertEquals(i / 100, estimator.getSampleCount());
        assertTrue(estimatedSize > previousEstimatedSize);
      }
      assertEquals(i / 100, estimator.getSampleCount());
    }
  }

}
