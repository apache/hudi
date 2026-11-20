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

/**
 * Decodes and resizes one image under a {@link RasterPlan} that {@link ImagePreparer} computed from the header before
 * calling it. This is the seam that lets the decode move out of process: an implementation may run the decode in this JVM
 * ({@link InProcessImageDecoder}) or elsewhere (another thread, another process), as long as it runs the logic named by
 * {@link #logicVersion()} and returns the same bytes ({@link ImagePreprocessingSpec#newDecoder()} refuses another logic).
 *
 * <p>Implementations need a public no-arg constructor and must be safe to call sequentially for many images.
 */
public interface ImageDecoder {

  /** The decoding-logic version this decoder runs, e.g. {@link ImageResizeLogicV1#LOGIC_VERSION}. */
  String logicVersion();

  /**
   * @param source a fresh stream per call over the image's bytes
   * @param plan   the subsampling and the raster bytes it was asserted to allocate
   * @param spec   the resize target and budget
   * @throws IOException when the pixels cannot be decoded
   */
  PreparedImage decode(ImageSource source, RasterPlan plan, ImagePreprocessingSpec spec) throws IOException;
}
