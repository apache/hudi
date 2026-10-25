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

package org.apache.hudi.sink.bootstrap.aggregate;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class TestBootstrapAggregation {

  @Test
  void testAccumulatorCountsDistinctTasksAndMerges() {
    BootstrapAccumulator first = new BootstrapAccumulator();
    first.update(0);
    first.update(0);
    first.update(1);
    assertEquals(2, first.readyTaskNum());
    assertSame(first, first.merge(null));

    BootstrapAccumulator second = new BootstrapAccumulator();
    second.update(1);
    second.update(2);
    assertSame(first, first.merge(second));
    assertEquals(3, first.readyTaskNum());
  }

  @Test
  void testAggregateFunctionLifecycle() {
    BootstrapAggFunction function = new BootstrapAggFunction();
    BootstrapAccumulator first = function.createAccumulator();
    BootstrapAccumulator second = function.createAccumulator();

    assertSame(first, function.add(3, first));
    function.add(3, first);
    function.add(4, second);
    assertEquals(1, function.getResult(first));
    assertSame(first, function.merge(first, second));
    assertEquals(2, function.getResult(first));
    assertEquals("BootstrapAggFunction", BootstrapAggFunction.NAME);
  }
}
