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

package org.apache.hudi.utilities.unstructured.embedding.audio.whisper;

import org.apache.hudi.common.config.TypedProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Test only: opens {@code contexts} whisper contexts of the configured model ONE AT A TIME
 * in this JVM's {@link WhisperContextPool} and leaves them idle there, with the model registered for the life of the JVM, so the
 * prep tasks that follow borrow existing contexts and no two contexts are ever opened at once. whisper-jni 1.7.1 keeps its
 * contexts in an unsynchronised native map, so contexts opened concurrently by two tasks can end the JVM; this keeps the
 * native smoke a measurement of memory. The transcriptions themselves still run in the prep tasks.
 */
public final class WhisperPoolPrewarm {

  private WhisperPoolPrewarm() {
  }

  /** Opens {@code contexts} contexts sequentially and returns them to the pool idle; returns the pool's open count. */
  public static int prewarm(TypedProperties props, int contexts) {
    WhisperConfig config = WhisperConfig.from(props);
    WhisperContextPool pool = WhisperContextPool.shared();
    pool.register(config);
    List<WhisperEngine> engines = new ArrayList<>();
    for (int i = 0; i < contexts; i++) {
      engines.add(pool.acquire(config));
    }
    engines.forEach(engine -> pool.release(config, engine));
    return pool.open();
  }
}
