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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubAudioSegmentSource;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageContentTypes;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;
import org.apache.hudi.utilities.unstructured.embedding.video.JcodecKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;

import org.junit.jupiter.api.Test;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The JCodec video preprocessor ({@link JcodecVideoFramePreprocessor}): picked by the video key for every video extension while
 * the default stays FFmpeg; it refuses audio on the driver, naming the key; its rows have the FFmpeg rows' shape with its own names.
 */
class TestJcodecVideoFramePreprocessor {

  private static final String C01 = "/video/codec-matrix/c01-h264-aac.mp4";

  private static TypedProperties jcodecProps() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), JcodecVideoFramePreprocessor.class.getName());
    return props;
  }

  @Test
  void testTheVideoKeySelectsJcodecForEveryVideoExtension() {
    PreprocessorRegistry registry = new PreprocessorRegistry(jcodecProps());
    for (String extension : PreprocessorRegistry.VIDEO_EXTENSIONS) {
      FilePreprocessor preprocessor = registry.forExtension(extension).get();
      assertEquals(JcodecVideoFramePreprocessor.class, preprocessor.getClass(), extension);
      assertEquals(JcodecVideoFramePreprocessor.PREPROCESSOR_NAME, preprocessor.name(), extension);
    }
    FilePreprocessor jcodec = registry.forExtension("mp4").get();
    assertEquals("video-frames-h264mp4-v1", jcodec.name());
    assertEquals(SourceKind.VIDEO, jcodec.sourceKind());
    assertTrue(jcodec.storesInputs());
    assertEquals(new HashSet<>(Arrays.asList(EmbeddingTableSchema.MODALITY_IMAGE)), jcodec.modalities(jcodecProps()));
    registry.validateConfig(jcodecProps(), TokenizerFactory.create(jcodecProps()));
  }

  @Test
  void testTheDefaultStaysFfmpeg() {
    PreprocessorRegistry registry = new PreprocessorRegistry(new TypedProperties());
    for (String extension : PreprocessorRegistry.VIDEO_EXTENSIONS) {
      assertEquals(VideoFramePreprocessor.class, registry.forExtension(extension).get().getClass(), extension);
    }
  }

  @Test
  void testAudioIsRefusedOnTheDriverNamingTheKey() {
    TypedProperties props = jcodecProps();
    props.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    props.setProperty(AudioConfigKeys.SEGMENT_SOURCE_CLASS.key(), StubAudioSegmentSource.class.getName());
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    Tokenizer tokenizer = TokenizerFactory.create(props);
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> registry.validateConfig(props, tokenizer));
    String message = refused.getMessage();
    assertTrue(message.contains(AudioConfigKeys.ENABLED.key()), message);
    assertTrue(message.contains(JcodecVideoFramePreprocessor.PREPROCESSOR_NAME), message);
    assertTrue(message.contains(VideoFramePreprocessor.class.getName()), message);
    assertTrue(message.contains("frames only"), message);
    assertFalse(message.contains("FFmpeg"), message);
    // audio off (explicitly) is accepted
    props.setProperty(AudioConfigKeys.ENABLED.key(), "false");
    new PreprocessorRegistry(props).validateConfig(props, TokenizerFactory.create(props));
  }

  /** A JCodec frame row: the FFmpeg row's shape (modality image, the frame index, every aux key) under JCodec's own names. */
  @Test
  void testFrameRowsHaveTheFfmpegShapeWithJcodecNames() throws Exception {
    Path c01 = Paths.get(TestJcodecVideoFramePreprocessor.class.getResource(C01).toURI());
    TypedProperties props = jcodecProps();
    FileMeta file = new FileMeta("sha-c01", "mp4", Files.size(c01));
    List<Chunk> chunks = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    JcodecVideoFramePreprocessor preprocessor = new JcodecVideoFramePreprocessor();
    try (ClosableIterator<Chunk> iterator = preprocessor.open(VideoClips.supplier(c01), file, TokenizerFactory.create(props), props)) {
      while (iterator.hasNext()) {
        Chunk chunk = iterator.next();
        chunks.add(chunk);
        Map<String, String> aux = chunk.getAux();
        assertEquals(new HashSet<>(Arrays.asList(EmbeddingTableSchema.AUX_FRAME_INDEX, VideoFramePreprocessor.AUX_TIMESTAMP_MS,
            VideoFramePreprocessor.AUX_DEDUP_KEY, VideoFramePreprocessor.AUX_VIDEO_LOGIC, VideoFramePreprocessor.AUX_WIDTH,
            VideoFramePreprocessor.AUX_HEIGHT, EmbeddingTableSchema.AUX_INPUT_WIDTH, EmbeddingTableSchema.AUX_INPUT_HEIGHT,
            EmbeddingTableSchema.AUX_INPUT_LOGIC, VideoFramePreprocessor.AUX_SIZE_BYTES, VideoFramePreprocessor.AUX_EXTENSION)),
            aux.keySet());
        long index = chunks.size() - 1;
        assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, chunk.getModality());
        assertEquals(Long.valueOf(index), chunk.getFrameIndex());
        assertEquals(String.valueOf(index), aux.get(EmbeddingTableSchema.AUX_FRAME_INDEX));
        assertEquals(JcodecKeyframeVideoFrameSource.LOGIC_VERSION, aux.get(VideoFramePreprocessor.AUX_VIDEO_LOGIC));
        assertEquals(JcodecVideoFramePreprocessor.PREPROCESSOR_NAME, aux.get(EmbeddingTableSchema.AUX_INPUT_LOGIC));
        assertEquals("160", aux.get(VideoFramePreprocessor.AUX_WIDTH));
        assertEquals("120", aux.get(EmbeddingTableSchema.AUX_INPUT_HEIGHT));
        assertEquals("mp4", aux.get(VideoFramePreprocessor.AUX_EXTENSION));
        assertEquals(String.valueOf(Files.size(c01)), aux.get(VideoFramePreprocessor.AUX_SIZE_BYTES));
        assertEquals(ImageContentTypes.JPEG, chunk.getPayloadContentType());
        assertFalse(chunk.isPayloadSource(), "a frame is a derived payload");
        try (InputStream in = chunk.getPayload().open()) {
          BufferedImage image = ImageIO.read(in);
          assertNotNull(image, "a readable JPEG");
          assertEquals(160, image.getWidth());
        }
        times.add(Long.parseLong(aux.get(VideoFramePreprocessor.AUX_TIMESTAMP_MS)));
      }
    }
    assertTrue(!chunks.isEmpty() && Arrays.asList(0L, 1000L, 2000L, 3000L, 4000L, 5000L).containsAll(times), times.toString());
  }
}
