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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;

import org.apache.spark.TaskContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A test client whose behaviour the test scripts through static state (local Spark runs every task in the test's JVM): the
 * dummy client's vectors, except
 * <ul>
 *   <li>an input whose text contains {@value #PERMANENT} fails for good;</li>
 *   <li>while {@link #OUTAGE} is above zero, every call reports the provider unavailable and counts it down;</li>
 *   <li>while {@link #RETRYABLE} is above zero, every input of a call is retryable and the count goes down by one per call;</li>
 *   <li>with {@link #FAIL_FIRST_ATTEMPT_OF_PARTITION} set to k, the first attempt of task k throws a platform failure, which
 *   fails the task (Spark then retries it); {@link #TASK_FAILURES} counts those throws.</li>
 * </ul>
 * {@link #CALLS} counts every call. Reset with {@link #reset()}.
 */
public class ScriptedEmbeddingClient implements EmbeddingClient {

  public static final String PERMANENT = "PERMANENT";
  public static final AtomicInteger OUTAGE = new AtomicInteger();
  public static final AtomicInteger RETRYABLE = new AtomicInteger();
  public static final AtomicInteger FAIL_FIRST_ATTEMPT_OF_PARTITION = new AtomicInteger(-1);
  public static final AtomicInteger TASK_FAILURES = new AtomicInteger();
  public static final AtomicInteger CALLS = new AtomicInteger();
  /** When set, every call is refused for good with this message ({@link EmbeddingClientConfigException}). */
  public static final AtomicReference<String> REFUSAL = new AtomicReference<>();
  /** Calls made by a re-attempted Spark task (attempt number above 0). */
  public static final AtomicInteger REATTEMPT_CALLS = new AtomicInteger();

  private EmbeddingModelSpec spec;

  public static void reset() {
    OUTAGE.set(0);
    RETRYABLE.set(0);
    FAIL_FIRST_ATTEMPT_OF_PARTITION.set(-1);
    TASK_FAILURES.set(0);
    CALLS.set(0);
    REFUSAL.set(null);
    REATTEMPT_CALLS.set(0);
  }

  @Override
  public void init(EmbeddingModelSpec spec, TypedProperties props) {
    this.spec = spec;
  }

  @Override
  public EmbeddingModelSpec spec() {
    return spec;
  }

  @Override
  public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
    CALLS.incrementAndGet();
    TaskContext task = TaskContext.get();
    if (task != null && task.attemptNumber() > 0) {
      REATTEMPT_CALLS.incrementAndGet();
    }
    if (REFUSAL.get() != null) {
      throw new EmbeddingClientConfigException(REFUSAL.get());
    }
    if (task != null && task.attemptNumber() == 0 && task.partitionId() == FAIL_FIRST_ATTEMPT_OF_PARTITION.get()) {
      TASK_FAILURES.incrementAndGet();
      throw new EmbeddingComputePlatformException("injected failure of the first attempt of compute task " + task.partitionId());
    }
    if (OUTAGE.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
      throw new EmbeddingProviderUnavailableException("injected provider outage");
    }
    boolean retryable = RETRYABLE.getAndUpdate(n -> Math.max(0, n - 1)) > 0;
    List<EmbeddingResult> results = new ArrayList<>(inputs.size());
    for (EmbeddingInput input : inputs) {
      if (retryable) {
        results.add(EmbeddingResult.retryable("injected retryable answer"));
      } else if (input.getText() != null && input.getText().contains(PERMANENT)) {
        results.add(EmbeddingResult.permanent("injected permanent failure"));
      } else {
        results.add(EmbeddingResult.ok(DummyEmbeddingClient.vectorFor(input.getSourcePathSha(), input.getChunkId(), spec.getDimension())));
      }
    }
    return results;
  }
}
