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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Arrays;

public class TestHilbertCurve {

  private static final HilbertCurve INSTANCE = HilbertCurve.bits(5).dimensions(2);

  @Test
  public void testIndex() {
    long[] t = {1, 2};
    assertEquals(13, INSTANCE.index(t).intValue());
    long[] t1 = {0, 16};
    assertEquals(256, INSTANCE.index(t1).intValue());
  }

  @Test
  public void testTranspose() {
    long[] ti = INSTANCE.transpose(BigInteger.valueOf(256));
    assertEquals(2, ti.length);
    assertEquals(0, ti[0]);
    assertEquals(16, ti[1]);
  }

  @Test
  public void toIndexByte() {
    HilbertCurve c = HilbertCurve.bits(4).dimensions(2);
    long[][] points = new long[][]{{1,2}, {3,2}, {5,6}, {7,8} };
    // bits interleave
    byte[][] resCheck = new byte[][] {{0b00, 0b00, 0b00, 0b00, 0b00, 0b00, 0b00, 0b0110},
        {0b00, 0b00, 0b00, 0b00, 0b00, 0b00, 0b00, 0b1110},
        {0b00, 0b00, 0b00, 0b00, 0b00, 0b00, 0b00, 0b00110110},
        {0b00, 0b00, 0b00, 0b00, 0b00, 0b00, 0b00, 0b01101010}};
    for (int i = 0; i < points.length; i++) {
      byte[] res = c.toIndexBytes(Arrays.stream(points[i]).toArray());
      for (int j = 0; j < 8; j++) {
        assertEquals(res[j], resCheck[i][j]);
      }
    }
  }
}
