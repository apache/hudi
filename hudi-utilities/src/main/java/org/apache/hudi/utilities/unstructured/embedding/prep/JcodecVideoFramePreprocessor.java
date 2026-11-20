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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.video.JcodecKeyframeVideoFrameSource;

/**
 * Data prep of a video file with JCodec, {@value #PREPROCESSOR_NAME}: the frame rows of {@link VideoFramePreprocessor} (same shape, same aux
 * keys, same storage) made by {@link JcodecKeyframeVideoFrameSource} (pure Java, H.264 in MP4 only), so a job prepares video
 * without FFmpeg's jars. Select it for every video extension with
 * {@code hoodie.unstructured.embedding.prep.video.preprocessor.class=}{@code org.apache.hudi.utilities.unstructured.embedding.prep.JcodecVideoFramePreprocessor};
 * the default stays FFmpeg. A video of another type is its file's quarantine entry naming the FFmpeg preprocessor. Its frames' bytes
 * differ from FFmpeg's, so its rows carry its own name as {@code input_logic}; both preprocessors' rows may share one table.
 *
 * <p>No audio: this preprocessor prepares frames only, so a job that enables audio ({@link AudioConfigKeys#ENABLED}) with
 * this preprocessor is refused on the driver, naming the key.
 */
public class JcodecVideoFramePreprocessor extends VideoFramePreprocessor {

  public static final String PREPROCESSOR_NAME = "video-frames-h264mp4-v1";

  public JcodecVideoFramePreprocessor() {
    super(new JcodecKeyframeVideoFrameSource());
  }

  @Override
  public String name() {
    return PREPROCESSOR_NAME;
  }

  /** Its frames are JCodec's: the stored input's logic is this preprocessor's name. */
  @Override
  protected String inputLogic() {
    return PREPROCESSOR_NAME;
  }

  /**
   * Refuses audio, then checks the frame keys as the default video preprocessor does.
   *
   * @throws IllegalArgumentException naming {@link AudioConfigKeys#ENABLED} when audio is on
   */
  @Override
  public void validateConfig(TypedProperties props, Tokenizer tokenizer) {
    if (AudioConfig.from(props).enabled()) {
      throw new IllegalArgumentException("Config " + AudioConfigKeys.ENABLED.key() + "=true, but video preprocessor " + PREPROCESSOR_NAME
          + " (" + EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key() + ") prepares frames only: audio is not supported by "
          + "this preprocessor. Set " + AudioConfigKeys.ENABLED.key() + "=false, or prepare video with "
          + VideoFramePreprocessor.class.getName());
    }
    super.validateConfig(props, tokenizer);
  }
}
