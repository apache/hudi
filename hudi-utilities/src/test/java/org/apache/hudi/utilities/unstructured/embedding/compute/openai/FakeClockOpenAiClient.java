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

package org.apache.hudi.utilities.unstructured.embedding.compute.openai;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The OpenAI-compatible client on a JVM-wide fake clock, for sync-level tests in local Spark (compute tasks run in the test JVM):
 * set as a model's {@code .client.class}, its sleeps (backoff and rate-limit waits) only advance {@link #CLOCK}, so a paced
 * compute step finishes at once and its waits are exact.
 */
public class FakeClockOpenAiClient extends OpenAiCompatibleEmbeddingClient {

  /** The fake clock, in ms; tests may read it in a stub responder to record arrivals. */
  public static final AtomicLong CLOCK = new AtomicLong(1_000_000L);

  public FakeClockOpenAiClient() {
    super(new JdkHttpTransport(), CLOCK::get, CLOCK::addAndGet, new Random(11), System::getenv);
  }
}
