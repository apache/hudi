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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMathUtils {

  @Test
  public void testPowerOf2() {
    Assertions.assertTrue(MathUtils.isPowerOf2(1));
    Assertions.assertTrue(MathUtils.isPowerOf2(2));
    Assertions.assertTrue(MathUtils.isPowerOf2(4));
    Assertions.assertTrue(MathUtils.isPowerOf2(16));
    Assertions.assertTrue(MathUtils.isPowerOf2(1024));

    Assertions.assertFalse(MathUtils.isPowerOf2(-1));
    Assertions.assertFalse(MathUtils.isPowerOf2(0));
    Assertions.assertFalse(MathUtils.isPowerOf2(3));
    Assertions.assertFalse(MathUtils.isPowerOf2(15));
    Assertions.assertFalse(MathUtils.isPowerOf2(1023));

  }

}
