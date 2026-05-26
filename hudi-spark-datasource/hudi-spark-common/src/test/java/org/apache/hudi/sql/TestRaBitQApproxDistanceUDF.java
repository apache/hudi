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

package org.apache.hudi.sql;

import org.apache.hudi.common.index.vector.RaBitQEncoder;
import org.apache.hudi.common.index.vector.VectorQuantizer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestRaBitQApproxDistanceUDF {

  @Test
  void testApproxDistanceMatchesEncoderEstimate() throws Exception {
    float[] query = new float[] {1.0f, -1.0f, 2.0f, -2.0f, 3.0f, -3.0f, 4.0f, -4.0f};
    float[] candidate = new float[] {0.5f, -0.75f, 1.5f, -1.0f, 2.0f, -2.5f, 3.5f, -3.0f};
    RaBitQEncoder encoder = new RaBitQEncoder(8, 42L, false);
    VectorQuantizer.QuantizedVector encoded = encoder.encode(candidate);

    float expected = encoder.estimateDistance(encoder.encodeQuery(query), encoded);
    Float actual = new RaBitQApproxDistanceUDF().call(
        encoded.code,
        encoded.scalar,
        "[1.0, -1.0, 2.0, -2.0, 3.0, -3.0, 4.0, -4.0]",
        8,
        42L,
        false);

    assertEquals(expected, actual, 1e-6f);
  }

  @Test
  void testNullScalarDefaultsToUnitNormForNormalizedInputs() throws Exception {
    float[] query = new float[] {1.0f, 0.0f, -1.0f, 0.5f};
    float[] candidate = new float[] {0.5f, 0.5f, -0.5f, 0.5f};
    RaBitQEncoder encoder = new RaBitQEncoder(4, 7L, true);
    VectorQuantizer.QuantizedVector encoded = encoder.encode(candidate);

    float expected = encoder.estimateDistance(
        encoder.encodeQuery(query),
        new VectorQuantizer.QuantizedVector(encoded.code, 1.0f));
    Float actual = new RaBitQApproxDistanceUDF().call(
        encoded.code,
        null,
        "[1.0, 0.0, -1.0, 0.5]",
        4,
        7L,
        true);

    assertEquals(expected, actual, 1e-6f);
  }
}
