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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.utilities.unstructured.embedding.video.JcodecKeyframeVideoFrameSource;

/**
 * Data prep of a video file with JCodec, {@value #NAME}: the frame rows of {@link VideoFramePreprocessor} (same shape, same aux
 * keys, same storage) made by {@link JcodecKeyframeVideoFrameSource} (pure Java, H.264 in MP4 only), so a job prepares video
 * without FFmpeg's jars. Select it for every video extension with
 * {@code hoodie.unstructured.embedding.prep.video.preprocessor.class=}{@code org.apache.hudi.utilities.unstructured.embedding.prep.JcodecVideoFramePreprocessor};
 * the default stays FFmpeg. A video of another type is its file's quarantine entry naming the FFmpeg preprocessor. Its frames' bytes
 * differ from FFmpeg's, so its rows carry its own name as {@code input_logic}; both preprocessors' rows may share one table.
 */
public class JcodecVideoFramePreprocessor extends VideoFramePreprocessor {

  public static final String NAME = "video-frames-h264mp4-v1";

  public JcodecVideoFramePreprocessor() {
    super(new JcodecKeyframeVideoFrameSource());
  }

  @Override
  public String name() {
    return NAME;
  }

  /** Its frames are JCodec's: the stored input's logic is this preprocessor's name. */
  @Override
  protected String inputLogic() {
    return NAME;
  }
}
