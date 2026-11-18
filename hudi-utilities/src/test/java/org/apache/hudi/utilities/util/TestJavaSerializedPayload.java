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

package org.apache.hudi.utilities.util;

import org.apache.spark.util.SizeEstimator;
import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A broadcast is sized on the driver by Spark's {@link SizeEstimator}, which walks transient fields.
 * The payload must present only its bytes to it, never the value it was built from.
 */
class TestJavaSerializedPayload {

  private static final int UNSERIALIZED_LONGS = 1 << 20;

  /** Serializes to a few bytes, while its transient field holds 8 MiB the estimator would count. */
  private static final class Value implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String name;
    private final transient long[] notShipped;

    Value(String name, boolean large) {
      this.name = name;
      this.notShipped = large ? new long[UNSERIALIZED_LONGS] : null;
    }
  }

  @Test
  void theDriverSizesOnlyTheBytes() {
    JavaSerializedPayload<Value> payload = new JavaSerializedPayload<>(new Value("v", true));
    long estimate = SizeEstimator.estimate(payload);
    assertTrue(estimate < 64L * 1024, "the estimator reached the value: " + estimate + " bytes");
  }

  @Test
  void getReturnsTheDeserializedValue() {
    JavaSerializedPayload<Value> payload = new JavaSerializedPayload<>(new Value("v", false));
    Value value = payload.get();
    assertEquals("v", value.name);
    assertNull(value.notShipped, "a transient field does not travel");
    assertSame(value, payload.get(), "get() caches");
  }
}
