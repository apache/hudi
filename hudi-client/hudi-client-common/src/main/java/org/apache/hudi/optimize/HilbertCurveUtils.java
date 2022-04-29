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

import java.math.BigInteger;

/**
 * Utils for Hilbert Curve.
 */
public class HilbertCurveUtils {
  public static byte[] indexBytes(HilbertCurve hilbertCurve, long[] points, int paddingNum) {
    BigInteger index = hilbertCurve.index(points);
    return paddingToNByte(index.toByteArray(), paddingNum);
  }

  public static byte[] paddingToNByte(byte[] a, int paddingNum) {
    if (a.length == paddingNum) {
      return a;
    }
    if (a.length > paddingNum) {
      byte[] result = new byte[paddingNum];
      System.arraycopy(a, 0, result, 0, paddingNum);
      return result;
    }
    int paddingSize = paddingNum - a.length;
    byte[] result = new byte[paddingNum];
    for (int i = 0; i < paddingSize; i++) {
      result[i] = 0;
    }
    System.arraycopy(a, 0, result, paddingSize, a.length);
    return result;
  }
}
