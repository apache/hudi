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

import org.apache.hudi.common.util.ValidationUtils;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * The executor JVM's whisper contexts, keyed by model file. A transcription borrows one context for the call and gives it back,
 * so the number of contexts alive is the largest number of transcriptions that ever ran at once in this JVM, at most its Spark
 * task slots: one context per task slot, however many transcriber instances the caller creates and whether or not it closes
 * them. A context is opened lazily on the first borrow that finds none idle; the idle contexts of a model are freed when the last
 * open transcriber of that model unregisters. A context's memory is native (model weights, KV caches, compute buffers), not on
 * the Java heap.
 */
final class WhisperContextPool {

  private static final WhisperContextPool SHARED = new WhisperContextPool(NativeWhisperEngine::open);

  private final WhisperEngine.Factory factory;
  private final Map<Path, Deque<WhisperEngine>> idle = new HashMap<>();
  private final Map<Path, Integer> registered = new HashMap<>();
  private int open;

  WhisperContextPool(WhisperEngine.Factory factory) {
    this.factory = factory;
  }

  /** The pool of this JVM, over the native engine. */
  static WhisperContextPool shared() {
    return SHARED;
  }

  /** Records one open transcriber of {@code config}'s model. */
  synchronized void register(WhisperConfig config) {
    registered.merge(config.modelPath(), 1, Integer::sum);
  }

  /** Records that a transcriber of {@code config}'s model closed; the last one frees that model's idle contexts. */
  void unregister(WhisperConfig config) {
    Deque<WhisperEngine> toFree = null;
    synchronized (this) {
      Path model = config.modelPath();
      Integer count = registered.get(model);
      ValidationUtils.checkState(count != null && count > 0, () -> "no open transcriber of " + model);
      if (count == 1) {
        registered.remove(model);
        toFree = idle.remove(model);
        if (toFree != null) {
          open -= toFree.size();
        }
      } else {
        registered.put(model, count - 1);
      }
    }
    if (toFree != null) {
      closeAll(toFree);
    }
  }

  /** Closes every engine, then rethrows the first failure with the later ones suppressed. Called outside the lock. */
  private static void closeAll(Deque<WhisperEngine> toFree) {
    Throwable firstFailure = null;
    for (WhisperEngine engine : toFree) {
      try {
        engine.close();
        // by design: closes every engine even after an Error; the first failure is rethrown, the rest suppressed
      } catch (RuntimeException | Error failure) {
        if (firstFailure == null) {
          firstFailure = failure;
        } else if (firstFailure != failure) {
          firstFailure.addSuppressed(failure);
        }
      }
    }
    if (firstFailure instanceof RuntimeException) {
      throw (RuntimeException) firstFailure;
    }
    if (firstFailure != null) {
      throw (Error) firstFailure;
    }
  }

  /**
   * Lends an idle context of {@code config}'s model, opening one when none is idle.
   *
   * @throws IllegalArgumentException when the context cannot be opened (see {@link WhisperEngine.Factory#open})
   */
  WhisperEngine acquire(WhisperConfig config) {
    synchronized (this) {
      Deque<WhisperEngine> models = idle.get(config.modelPath());
      if (models != null && !models.isEmpty()) {
        return models.pop();
      }
    }
    WhisperEngine engine = factory.open(config);
    synchronized (this) {
      open++;
    }
    return engine;
  }

  /** Gives back a context lent by {@link #acquire}; freed at once when no transcriber of its model is open any more. */
  void release(WhisperConfig config, WhisperEngine engine) {
    synchronized (this) {
      Path model = config.modelPath();
      if (registered.containsKey(model)) {
        idle.computeIfAbsent(model, m -> new ArrayDeque<>()).push(engine);
        return;
      }
      open--;
    }
    engine.close();
  }

  /** Contexts opened by this pool and not freed yet (lent or idle). */
  synchronized int open() {
    return open;
  }

  /** Idle contexts of {@code config}'s model. */
  synchronized int idle(WhisperConfig config) {
    Deque<WhisperEngine> models = idle.get(config.modelPath());
    return models == null ? 0 : models.size();
  }
}
