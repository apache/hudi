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

package org.apache.hudi.optimize;

import org.davidmoten.hilbert.HilbertCurve;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHilbertCurveUtils {

  private static final HilbertCurve INSTANCE = HilbertCurve.bits(5).dimensions(2);

  @Test
  public void testIndex() {
    long[] t = {1, 2};
    assertEquals(13, INSTANCE.index(t).intValue());
    long[] t1 = {0, 16};
    assertEquals(256, INSTANCE.index(t1).intValue());
  }
}
