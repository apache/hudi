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
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The per-modality video preprocessor key ({@link EmbeddingTableConfig#PREP_VIDEO_PREPROCESSOR_CLASS}): one setting picks the
 * preprocessor of every video extension, static for the job; it defaults to the FFmpeg {@link VideoFramePreprocessor}; a
 * per-extension class key still wins for its own extension; the key must name a {@link SourceKind#VIDEO} preprocessor.
 */
public class TestVideoPreprocessorKey {

  /** Another video preprocessor: a {@link VideoFramePreprocessor} subclass with its own name. */
  public static class OtherVideoPreprocessor extends VideoFramePreprocessor {
    static final String NAME = "video-frames-other-v1";

    @Override
    public String name() {
      return NAME;
    }
  }

  @Test
  public void testDefaultIsTheFfmpegPreprocessorForEveryVideoExtension() {
    assertEquals(VideoFramePreprocessor.class.getName(), EmbeddingTableConfig.DEFAULT_PREP_VIDEO_PREPROCESSOR_CLASS);
    PreprocessorRegistry registry = new PreprocessorRegistry(new TypedProperties());
    for (String extension : PreprocessorRegistry.VIDEO_EXTENSIONS) {
      assertEquals(VideoFramePreprocessor.class, registry.forExtension(extension).get().getClass(), extension);
    }
    assertEquals(VideoFramePreprocessor.NAME, registry.forExtension("mp4").get().name());
  }

  @Test
  public void testTheKeySetsEveryVideoExtensionAndNothingElse() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), OtherVideoPreprocessor.class.getName());
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    for (String extension : PreprocessorRegistry.VIDEO_EXTENSIONS) {
      assertEquals(OtherVideoPreprocessor.class, registry.forExtension(extension).get().getClass(), extension);
    }
    assertEquals(ImageInputPreprocessor.class, registry.forExtension("png").get().getClass());
    assertEquals(PreprocessorRegistry.PDF_PREPROCESSOR_CLASS, registry.forExtension("pdf").get().getClass().getName());
    assertEquals(OtherVideoPreprocessor.NAME, registry.forExtension("webm").get().name());
  }

  @Test
  public void testAPerExtensionKeyStillWinsForItsExtension() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), OtherVideoPreprocessor.class.getName());
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "mov" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        VideoFramePreprocessor.class.getName());
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    assertEquals(VideoFramePreprocessor.class, registry.forExtension("mov").get().getClass());
    assertEquals(OtherVideoPreprocessor.class, registry.forExtension("mp4").get().getClass());
  }

  @Test
  public void testTheKeyMustNameAVideoPreprocessor() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), ImageInputPreprocessor.class.getName());
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> new PreprocessorRegistry(props).validateConfig(props, TokenizerFactory.create(props)));
    assertTrue(refused.getMessage().contains(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key())
        && refused.getMessage().contains(ImageInputPreprocessor.class.getName()), refused.getMessage());
  }
}
