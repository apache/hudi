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

package org.apache.hudi.utilities.unstructured.embedding.image;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test decoder that runs the SAME logic through a different code path: on a thread of its own, handing the result back.
 * It stands for a decoder moved out of the task's thread or process: same logic version, so the same bytes.
 */
public class ThreadHandoffImageDecoder implements ImageDecoder {

  /** Name of the thread that ran the last decode, so a test can show the code path differed. */
  public static final AtomicReference<String> LAST_THREAD = new AtomicReference<>();

  @Override
  public String logicVersion() {
    return ImageResizeLogicV1.LOGIC_VERSION;
  }

  @Override
  public PreparedImage decode(ImageSource source, RasterPlan plan, ImagePreprocessingSpec spec) throws IOException {
    AtomicReference<PreparedImage> result = new AtomicReference<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread worker = new Thread(() -> {
      LAST_THREAD.set(Thread.currentThread().getName());
      try {
        result.set(ImageResizeLogicV1.run(source, plan, spec));
      } catch (Throwable t) {
        failure.set(t);
      }
    }, "image-decoder-handoff");
    worker.start();
    try {
      worker.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while waiting for the decoder thread", e);
    }
    if (failure.get() instanceof IOException) {
      throw (IOException) failure.get();
    }
    if (failure.get() != null) {
      throw new IOException("decoder thread failed", failure.get());
    }
    return result.get();
  }
}
