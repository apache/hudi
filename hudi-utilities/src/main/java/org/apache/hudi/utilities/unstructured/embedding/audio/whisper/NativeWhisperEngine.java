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

import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;

import io.github.givimad.whisperjni.WhisperContext;
import io.github.givimad.whisperjni.WhisperFullParams;
import io.github.givimad.whisperjni.WhisperJNI;
import io.github.givimad.whisperjni.WhisperSamplingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * One whisper.cpp context through whisper-jni 1.7.1. The binding's jar carries the native libraries for linux-arm64
 * ({@code debian-arm64/}) and linux-x86_64 ({@code debian-amd64/}), built against glibc 2.31; {@link WhisperJNI#loadLibrary()}
 * extracts and loads the one for this platform once per JVM. The context's memory (model weights, KV caches, compute buffers) is
 * native, outside the Java heap.
 *
 * <p>whisper-jni 1.7.1 keeps every context of the JVM in one native map with no lock of its own: a context created or freed
 * while another task's call reads that map can corrupt it, and the binding's {@code map::at} then aborts the JVM. So creating and
 * freeing a context take this class's write lock and every call on a context takes its read lock: transcriptions on different
 * contexts still run in parallel, and no create or free overlaps one of them.
 */
final class NativeWhisperEngine implements WhisperEngine {

  /**
   * The logic of transcribing through whisper-jni (the parameters this engine hands whisper.cpp and how it reads the result back),
   * pinned in the audio prep logic version ({@link WhisperSegmentTranscriber#ENGINE_LOGIC_VERSION}) instead of the whisper-jni
   * jar version. Bump only when a dependency upgrade or code change alters prep output; the golden test below proves it:
   * {@code TestWhisperTranscriberModel} transcribes a pinned clip with the pinned models (gated on the model cache).
   */
  static final String LOGIC_VERSION = "whisper-jni-transcription-v1";

  private static final Logger LOG = LoggerFactory.getLogger(NativeWhisperEngine.class);
  private static final AtomicInteger LIVE = new AtomicInteger();
  private static final Object LOAD_LOCK = new Object();
  /** The loaded binding, set once per JVM under {@link #LOAD_LOCK}. */
  private static final AtomicReference<WhisperJNI> WHISPER = new AtomicReference<>();
  /** Guards whisper-jni's JVM-wide context map: write for a context's create and free, read for every call on a context. */
  private static final ReentrantReadWriteLock CONTEXT_MAP_LOCK = new ReentrantReadWriteLock();

  private final WhisperContext context;
  private boolean closed;

  private NativeWhisperEngine(WhisperContext context) {
    this.context = context;
  }

  /**
   * Loads the native library (once per JVM) and the model of {@code config}.
   *
   * @throws IllegalArgumentException when the native library cannot be loaded on this platform, or the model file is missing,
   *                                  unreadable or not a ggml whisper model: configuration errors, never retried
   */
  static NativeWhisperEngine open(WhisperConfig config) {
    WhisperJNI jni = library();
    Path model = config.modelPath();
    if (!Files.isRegularFile(model) || !Files.isReadable(model)) {
      throw new IllegalArgumentException(WhisperConfig.MODEL.key() + " names no readable model file on this executor: " + model);
    }
    WhisperContext context;
    CONTEXT_MAP_LOCK.writeLock().lock();
    try {
      context = jni.init(model);
    } catch (IOException e) {
      throw new IllegalArgumentException(WhisperConfig.MODEL.key() + " could not be loaded from " + model, e);
    } finally {
      CONTEXT_MAP_LOCK.writeLock().unlock();
    }
    if (context == null) {
      throw new IllegalArgumentException(WhisperConfig.MODEL.key() + " is not a ggml whisper model: " + model);
    }
    int live = LIVE.incrementAndGet();
    LOG.info("Opened whisper context for {} ({} live in this JVM)", model, live);
    return new NativeWhisperEngine(context);
  }

  /**
   * Fails the call when whisper's {@code full()} returned {@code rc} other than 0: a {@link PrepPlatformException} (class P),
   * because whisper.cpp reports an allocation or compute failure inside ggml the same way as audio it cannot take, and the
   * segment it was handed already passed our own checks ({@link PcmPayload}).
   */
  static void checkFullResult(int rc) {
    if (rc != 0) {
      throw new PrepPlatformException("whisper failed on the audio segment: full() returned " + rc, null);
    }
  }

  /** Whisper contexts open in this JVM. */
  static int liveContexts() {
    return LIVE.get();
  }

  /**
   * The loaded binding.
   *
   * @throws IllegalArgumentException when the native library is not available for this platform
   */
  static WhisperJNI library() {
    WhisperJNI loaded = WHISPER.get();
    if (loaded != null) {
      return loaded;
    }
    synchronized (LOAD_LOCK) {
      if (WHISPER.get() == null) {
        try {
          WhisperJNI.loadLibrary();
        } catch (IOException | LinkageError e) {
          throw new IllegalArgumentException("the whisper native library cannot be loaded on " + System.getProperty("os.name")
              + "/" + System.getProperty("os.arch") + " (whisper-jni ships linux-arm64 and linux-x86_64); check "
              + AudioConfigKeys.TRANSCRIBER_CLASS.key(), e);
        }
        WHISPER.set(new WhisperJNI());
      }
      return WHISPER.get();
    }
  }

  @Override
  public List<RawUtterance> transcribe(WhisperConfig config, float[] pcm, int samples) {
    WhisperJNI jni = library();
    WhisperFullParams params = new WhisperFullParams(WhisperSamplingStrategy.GREEDY);
    params.nThreads = config.threadCount();
    params.language = config.languageCode();
    params.translate = config.translate();
    params.printProgress = false;
    params.printRealtime = false;
    params.printTimestamps = false;
    params.printSpecial = false;
    CONTEXT_MAP_LOCK.readLock().lock();
    try {
      // File names do not establish capability, and a pooled context may serve a later request.
      if (!jni.isMultilingual(context)) {
        if (!WhisperConfig.DEFAULT_LANGUAGE.equals(config.languageCode())) {
          throw new IllegalArgumentException(WhisperConfig.LANGUAGE.key() + " must be en for the loaded English-only model");
        }
        if (config.translate()) {
          throw new IllegalArgumentException(WhisperConfig.TASK.key() + " must be transcribe for the loaded English-only model");
        }
      }
      checkFullResult(jni.full(context, params, pcm, samples));
      int count = jni.fullNSegments(context);
      List<RawUtterance> utterances = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        utterances.add(new RawUtterance(jni.fullGetSegmentTimestamp0(context, i), jni.fullGetSegmentTimestamp1(context, i),
            jni.fullGetSegmentText(context, i)));
      }
      return utterances;
    } finally {
      CONTEXT_MAP_LOCK.readLock().unlock();
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      CONTEXT_MAP_LOCK.writeLock().lock();
      try {
        context.close();
      } finally {
        CONTEXT_MAP_LOCK.writeLock().unlock();
      }
      LIVE.decrementAndGet();
    }
  }
}
