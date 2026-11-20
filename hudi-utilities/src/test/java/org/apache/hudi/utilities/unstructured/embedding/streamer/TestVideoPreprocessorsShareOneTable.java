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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyMultimodalEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreparer;
import org.apache.hudi.utilities.unstructured.embedding.prep.JcodecVideoFramePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.FrameExtractionStrategy;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.video.FfmpegKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.embedding.video.JcodecKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Frames prepared by FFmpeg and by JCodec share one embedding table: a round with the default (FFmpeg) video preprocessor, then a
 * round with the video key set to {@link JcodecVideoFramePreprocessor} over another video, on the same table. Both videos' frames
 * are {@code image} rows of the same shape, each carrying the table-pinned {@code prep_logic_version}; compute embeds both; the second round leaves the first round's rows as they were.
 */
@Tag("functional")
public class TestVideoPreprocessorsShareOneTable extends EmbeddingRoundTestBase {

  /** Spark 3.5's Parquet caps inline values at 80 KiB: the 29 KB clip and its small frames sit under it. */
  private static final long VIDEO_INLINE_MAX_BYTES = 80L * 1024;
  private static final Path CLIP = Paths.get("src/test/resources/video/codec-matrix/c01-h264-aac.mp4");

  private TypedProperties computeProps() {
    String model = EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL;
    TypedProperties props = props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, VIDEO_INLINE_MAX_BYTES,
        model + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, DummyMultimodalEmbeddingClient.PROVIDER,
        model + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX, "text,image");
    // the provider decides the client: the base's scripted client class is removed
    props.remove(model + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX);
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "mp4" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        JcodecVideoFramePreprocessor.class.getName());
    return props;
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(HoodieTableType.class)
  public void ffmpegAndJcodecFramesShareOneTable(HoodieTableType tableType) throws Exception {
    byte[] clip = Files.readAllBytes(CLIP);
    FixtureFile byFfmpeg = object("videos/by-ffmpeg.mov", clip);
    FixtureFile byJcodec = object("videos/by-jcodec.mp4", clip);

    TypedProperties ffmpegProps = computeProps();
    announce(byFfmpeg);
    Round first = EmbeddingStreamers.round(spark(), ffmpegProps, cfg -> cfg.tableType = tableType.name());
    assertEquals(1, first.getCompleted().size(), first.toString());
    Map<String, String> ffmpegRows = describe(rows(table()), byFfmpeg);

    TypedProperties jcodecProps = computeProps();
    announce(byJcodec);
    Round second = EmbeddingStreamers.round(spark(), jcodecProps, cfg -> cfg.tableType = tableType.name());
    assertEquals(1, second.getCompleted().size(), second.toString());
    assertTrue(second.getErrors().isEmpty(), second.toString());
    assertEquals(2, EmbeddingStreamers.commits(table(), storage()).size());

    Map<String, Row> rows = rows(table());
    List<byte[]> ffmpegImages = new ArrayList<>();
    List<byte[]> jcodecImages = new ArrayList<>();
    expectedFrames(new FfmpegKeyframeVideoFrameSource(), byFfmpeg, ffmpegProps, ffmpegImages);
    expectedFrames(new JcodecKeyframeVideoFrameSource(), byJcodec, jcodecProps, jcodecImages);
    Set<String> expectedKeys = new TreeSet<>();
    for (int k = 0; k < ffmpegImages.size(); k++) {
      expectedKeys.add(key(byFfmpeg, EmbeddingTableSchema.MODALITY_IMAGE, k));
    }
    for (int k = 0; k < jcodecImages.size(); k++) {
      expectedKeys.add(key(byJcodec, EmbeddingTableSchema.MODALITY_IMAGE, k));
    }
    assertEquals(expectedKeys, rows.keySet(), "both videos' frames, and nothing else, are the table's rows");

    ImagePreparer preparer = new ImagePreparer(EmbeddingTableConfig.from(jcodecProps).imagePreprocessing());
    assertFrames(rows, byFfmpeg, ffmpegImages, preparer);
    assertFrames(rows, byJcodec, jcodecImages, preparer);
    assertEquals(ffmpegRows, describe(rows, byFfmpeg), "the JCodec round leaves the FFmpeg rows as they were");
  }

  /** The frames {@code strategy} keeps for the clip under {@code props}, read straight from the file (the expected rows). */
  private static void expectedFrames(FrameExtractionStrategy strategy, FixtureFile video, TypedProperties props, List<byte[]> images)
      throws IOException {
    try (ClosableIterator<ExtractedFrame> it = strategy.frames(VideoClips.supplier(CLIP),
        new FileMeta(video.sha, video.extension, video.bytes.length), props)) {
      while (it.hasNext()) {
        try (InputStream in = it.next().getImage().open()) {
          images.add(in.readAllBytes());
        }
      }
    }
    assertTrue(images.size() > 1, strategy.name() + " keeps several frames: " + images.size());
  }

  private void assertFrames(Map<String, Row> rows, FixtureFile video, List<byte[]> images, ImagePreparer preparer) throws IOException {
    for (int k = 0; k < images.size(); k++) {
      Row row = rows.get(key(video, EmbeddingTableSchema.MODALITY_IMAGE, k));
      assertEquals(video.sourcePath, row.getAs(EmbeddingTableSchema.SOURCE_FILE_PATH));
      scala.collection.Map<String, String> aux = row.getAs(EmbeddingTableSchema.AUX);
      assertEquals(String.valueOf(k), aux.apply(EmbeddingTableSchema.AUX_FRAME_INDEX));
      assertEquals(prepLogicVersion("video"), aux.apply(EmbeddingTableSchema.AUX_PREP_LOGIC_VERSION), video.sourcePath);
      Row prepared = row.getAs(EmbeddingTableSchema.PREPARED_IMAGE);
      assertEquals(HoodieSchema.Blob.INLINE, prepared.getAs(HoodieSchema.Blob.TYPE), "a frame is stored inline");
      assertArrayEquals(images.get(k), (byte[]) prepared.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD), video.sourcePath + " frame " + k);
      float[] expected = DummyMultimodalEmbeddingClient.vectorForImage(
          preparer.prepare(ImageFixtures.source(images.get(k))).getEncoded(), EmbeddingStreamers.MODEL_DIMENSIONS);
      assertArrayEquals(expected, vector(row), video.sourcePath + " frame " + k);
    }
  }

  /** The rows of {@code video} as text, with each inline frame's bytes by content (a byte array prints by identity). */
  private static Map<String, String> describe(Map<String, Row> rows, FixtureFile video) {
    Map<String, String> out = new TreeMap<>();
    for (Map.Entry<String, Row> entry : rows.entrySet()) {
      Row row = entry.getValue();
      if (video.sha.equals(row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA))) {
        Row prepared = row.getAs(EmbeddingTableSchema.PREPARED_IMAGE);
        byte[] frame = prepared.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD);
        out.put(entry.getKey(), row.toString().replaceAll("\\[B@[0-9a-f]+", "") + " frame=" + Arrays.toString(frame));
      }
    }
    return out;
  }
}
