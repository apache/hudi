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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link Source#releaseResources()}.
 *
 * <p>releaseResources() is invoked from StreamSync.syncOnce()'s finally block, after the
 * write/commit has already completed. A transient Spark failure while unpersisting the
 * cached source RDD (e.g. a NullPointerException from BlockManagerMaster.removeRdd when
 * the SparkContext is mid-teardown/stopping) must not fail an otherwise-successful
 * ingestion round.
 */
public class TestSource {

  /**
   * Minimal concrete {@link Source} whose unpersist step always fails, so the
   * swallow-on-failure behaviour of releaseResources() can be exercised without a
   * live SparkContext.
   */
  private static class MockSourceWithUnpersistenceFailure extends Source<String> {
    private int unpersistCalls = 0;

    MockSourceWithUnpersistenceFailure(TypedProperties props) {
      super(props, null, null, null);
    }

    @Override
    protected InputBatch<String> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
      // Not exercised by these tests; releaseResources() is tested directly.
      return null;
    }

    @Override
    protected void unpersistCachedSourceRdd() {
      unpersistCalls++;
      throw new NullPointerException("simulated BlockManagerMaster.removeRdd NPE");
    }
  }

  @Test
  public void releaseResourcesSwallowsTransientUnpersistFailure() {
    MockSourceWithUnpersistenceFailure source =
        new MockSourceWithUnpersistenceFailure(new TypedProperties());
    assertDoesNotThrow(source::releaseResources,
        "A transient unpersist failure in cleanup must not propagate out of releaseResources()");
    assertEquals(1, source.unpersistCalls, "unpersist should have been attempted exactly once");
  }
}
