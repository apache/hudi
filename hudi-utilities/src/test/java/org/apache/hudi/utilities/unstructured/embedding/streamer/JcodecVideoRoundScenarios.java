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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyMultimodalEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreparer;
import org.apache.hudi.utilities.unstructured.embedding.prep.JcodecVideoFramePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.video.JcodecKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.storage.ParquetInlineCap;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Scenarios of a JVM WITHOUT FFmpeg (org.bytedeco) on its class path, run by {@link TestJcodecVideoRoundWithoutFfmpeg} in a child
 * JVM ({@link ScenarioJvm}); every scenario first checks that FFmpeg's classes cannot be loaded. The class's name is not a
 * surefire test name, but a run that names every test class (as CI's utilities shard does) selects it in a JVM that has FFmpeg:
 * there the scenarios are skipped, never run. In the child JVM a skipped scenario is ABORTED, which the parent test fails.
 *
 * <p>{@link #JCODEC_ROUND}: an H.264 MP4 and an HEVC MP4 through one embedding round of the single writer with every video
 * extension prepared by the JCodec preprocessor, on both table types and in both modes. The H.264 video is frame rows (an inline
 * JPEG each, chunk_id = frame index, times from 0) in the round's one commit, embedded by the dummy multimodal client under
 * {@code prep_and_compute}; the HEVC video is no row and one prep error naming its codec and the FFmpeg preprocessor.
 *
 * <p>{@link #SANITY}: a text and an image file through a round, the harness's proof that needs no video class.
 */
public class JcodecVideoRoundScenarios extends EmbeddingRoundTestBase {

  /** The scenario method of the JCodec video round. */
  public static final String JCODEC_ROUND = "jcodecVideoBecomesFrameRowsInOneCommit";
  /** The scenario method of the round without video. */
  public static final String SANITY = "textAndImageRoundRunsWithoutFfmpeg";
  /** The JCodec video preprocessor, as the video preprocessor key names it. */
  static final String JCODEC_PREPROCESSOR = JcodecVideoFramePreprocessor.class.getName();
  /** Classes of the FFmpeg jars (javacv, ffmpeg) that a JVM without them cannot load. */
  static final List<String> FFMPEG_CLASSES = Arrays.asList("org.bytedeco.javacv.FFmpegFrameGrabber", "org.bytedeco.ffmpeg.global.avcodec");
  /** The largest inline threshold of a Spark 3.5 Parquet: the frames are derived inputs, stored inline up to it. */
  private static final long FRAME_INLINE_MAX_BYTES = ParquetInlineCap.MAX_BYTES;
  private static final String MATRIX = "/video/codec-matrix/";

  private final FixtureFile h264 = object("video/c01-h264-aac.mp4", resource(MATRIX + "c01-h264-aac.mp4"));
  private final FixtureFile hevc = object("video/c03-hevc-aac.mp4", resource(MATRIX + "c03-hevc-aac.mp4"));

  @BeforeAll
  public static void ffmpegIsNotOnTheClassPath() {
    for (String ffmpegClass : FFMPEG_CLASSES) {
      assumeFalse(loadable(ffmpegClass), ffmpegClass + " is loadable: the scenarios run only in a JVM without the FFmpeg jars");
    }
  }

  private static boolean loadable(String className) {
    try {
      Class.forName(className);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  @ParameterizedTest(name = "{0}, {1}")
  @CsvSource({"COPY_ON_WRITE, prep_only", "COPY_ON_WRITE, prep_and_compute",
      "MERGE_ON_READ, prep_only", "MERGE_ON_READ, prep_and_compute"})
  public void jcodecVideoBecomesFrameRowsInOneCommit(HoodieTableType tableType, String mode) throws Exception {
    assertTrue(h264.bytes.length <= FRAME_INLINE_MAX_BYTES, "the H.264 video is inline in its source row: " + h264.bytes.length);
    announce(h264, hevc);
    TypedProperties props = jobProps(mode, EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), JCODEC_PREPROCESSOR);

    Round round = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());

    assertEquals(1, round.getCompleted().size(), round.toString());
    assertEquals(1, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(tableType, EmbeddingStreamers.metaClient(table(), storage()).getTableType());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), round.getCommit().get()));
    // the HEVC video: no row, one prep error naming its codec and the preprocessor that decodes it
    assertEquals(1, round.getErrors().size(), round.toString());
    Map<String, Object> error = round.getErrors().get(0);
    assertEquals(hevc.sha, error.get("source_path_sha"));
    assertEquals("prep", error.get("stage"));
    assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, error.get("modality"));
    String message = String.valueOf(error.get("error"));
    assertTrue(message.contains("HEVC (hvc1)") && message.contains(VideoFramePreprocessor.class.getName()), message);

    Map<String, Row> rows = rows(table());
    assertTrue(rows.values().stream().noneMatch(row -> hevc.sha.equals(row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA))),
        "the HEVC video has no row");
    List<Row> frames = frameRows(rows, h264);
    assertFalse(frames.isEmpty(), "the H.264 video has frame rows");
    assertEquals(frames.size(), rows.size(), "the frames are the table's rows");
    assertEquals(frames.size(), round.getRowsWritten());
    long previousTimeMs = -1;
    for (int i = 0; i < frames.size(); i++) {
      Row frame = frames.get(i);
      assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, frame.getAs(EmbeddingTableSchema.MODALITY));
      assertEquals(i, (long) frame.getAs(EmbeddingTableSchema.CHUNK_ID), "chunk ids are dense from 0");
      assertEquals(h264.sourcePath, frame.getAs(EmbeddingTableSchema.SOURCE_FILE_PATH));
      scala.collection.Map<String, String> aux = frame.getAs(EmbeddingTableSchema.AUX);
      assertEquals(String.valueOf(i), aux.apply(EmbeddingTableSchema.AUX_FRAME_INDEX));
      long timeMs = Long.parseLong(aux.apply(VideoFramePreprocessor.AUX_TIMESTAMP_MS));
      assertTrue(i == 0 ? timeMs == 0 : timeMs > previousTimeMs, "frame times rise strictly from 0: " + previousTimeMs + ", " + timeMs);
      previousTimeMs = timeMs;
      assertEquals(JcodecVideoFramePreprocessor.NAME, aux.apply(EmbeddingTableSchema.AUX_INPUT_LOGIC));
      assertEquals(JcodecKeyframeVideoFrameSource.LOGIC_VERSION, aux.apply(VideoFramePreprocessor.AUX_VIDEO_LOGIC));
      assertEquals("mp4", aux.apply(VideoFramePreprocessor.AUX_EXTENSION));
      assertEquals(String.valueOf(h264.bytes.length), aux.apply(VideoFramePreprocessor.AUX_SIZE_BYTES));
      byte[] jpeg = inlineImage(frame);
      BufferedImage image = ImageIO.read(new ByteArrayInputStream(jpeg));
      assertNotNull(image, "frame " + i + " is a readable JPEG");
      assertEquals(aux.apply(VideoFramePreprocessor.AUX_WIDTH), String.valueOf(image.getWidth()));
      assertEquals(aux.apply(VideoFramePreprocessor.AUX_HEIGHT), String.valueOf(image.getHeight()));
    }

    if (EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
      assertNoModelColumn(rows);
    } else {
      ImagePreparer preparer = new ImagePreparer(EmbeddingTableConfig.from(props).imagePreprocessing());
      for (Row frame : frames) {
        float[] expected = DummyMultimodalEmbeddingClient.vectorForImage(
            preparer.prepare(ImageFixtures.source(inlineImage(frame))).getEncoded(), EmbeddingStreamers.MODEL_DIMENSIONS);
        assertArrayEquals(expected, vector(frame), "frame " + frame.getAs(EmbeddingTableSchema.CHUNK_ID));
      }
    }

    // the same versions announced again: the same keys and values, no new rows
    List<String> before = describeFrames(rows);
    Version h264Version = versionOf(h264);
    Version hevcVersion = versionOf(hevc);
    announce(h264, hevc);
    touch(h264, Instant.ofEpochMilli(h264Version.modificationTime));
    touch(hevc, Instant.ofEpochMilli(hevcVersion.modificationTime));
    Round rerun = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());
    assertEquals(1, rerun.getCompleted().size(), rerun.toString());
    assertEquals(before, describeFrames(rows(table())));
  }

  @Test
  public void textAndImageRoundRunsWithoutFfmpeg() throws Exception {
    FixtureFile text = text("notes", "alpha", "beta");
    FixtureFile image = object("images/small.png", ImageFixtures.image("png", 8, 6));
    announce(text, image);
    TypedProperties props = jobProps(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);

    Round round = round(props);

    assertEquals(1, round.getCompleted().size(), round.toString());
    assertEquals(1, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(0, round.getErrors().size(), round.toString());
    Map<String, Row> rows = rows(table());
    assertEquals(3, rows.size(), rows.keySet().toString());
    for (long chunk = 0; chunk < 2; chunk++) {
      assertArrayEquals(DummyEmbeddingClient.vectorFor(text.sha, chunk, EmbeddingStreamers.MODEL_DIMENSIONS),
          vector(rows.get(key(text, "text", chunk))));
    }
    ImagePreparer preparer = new ImagePreparer(EmbeddingTableConfig.from(props).imagePreprocessing());
    float[] expected = DummyMultimodalEmbeddingClient.vectorForImage(
        preparer.prepare(ImageFixtures.source(image.bytes)).getEncoded(), EmbeddingStreamers.MODEL_DIMENSIONS);
    assertArrayEquals(expected, vector(rows.get(key(image, "image", 0))));
  }

  /** The job's properties, frames inline up to {@link #FRAME_INLINE_MAX_BYTES}, the dummy multimodal client under compute. */
  private TypedProperties jobProps(String mode, String... overrides) {
    if (EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
      return props(table(), mode, FRAME_INLINE_MAX_BYTES, overrides);
    }
    String model = EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL;
    List<String> all = new ArrayList<>(Arrays.asList(overrides));
    all.add(model + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX);
    all.add(DummyMultimodalEmbeddingClient.PROVIDER);
    all.add(model + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX);
    all.add("text,image");
    TypedProperties props = props(table(), mode, FRAME_INLINE_MAX_BYTES, all.toArray(new String[0]));
    // the provider decides the client: the base's scripted client class is removed
    props.remove(model + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX);
    return props;
  }

  /** The frame rows of {@code video}, by chunk id. */
  private static List<Row> frameRows(Map<String, Row> rows, FixtureFile video) {
    return rows.values().stream().filter(row -> video.sha.equals(row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA)))
        .sorted(Comparator.comparingLong(row -> (long) row.getAs(EmbeddingTableSchema.CHUNK_ID))).collect(Collectors.toList());
  }

  /** The inline bytes of a frame row's {@code prepared_image}: a frame is stored inline only. */
  private static byte[] inlineImage(Row frame) {
    Row prepared = frame.getAs(EmbeddingTableSchema.PREPARED_IMAGE);
    assertEquals(HoodieSchema.Blob.INLINE, prepared.getAs(HoodieSchema.Blob.TYPE));
    byte[] bytes = prepared.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD);
    assertNotNull(bytes);
    return bytes;
  }

  private static void assertNoModelColumn(Map<String, Row> rows) {
    for (Row row : rows.values()) {
      assertFalse(Arrays.asList(row.schema().fieldNames()).contains(EmbeddingStreamers.MODEL_COLUMN),
          "prep_only writes no model column: " + row.schema().simpleString());
    }
  }

  private static byte[] resource(String name) {
    try (InputStream in = JcodecVideoRoundScenarios.class.getResourceAsStream(name)) {
      assertNotNull(in, "missing test resource " + name);
      return in.readAllBytes();
    } catch (IOException e) {
      throw new IllegalStateException("cannot read test resource " + name, e);
    }
  }

  /** Each row as text, with the inline bytes of its prepared image and its vector by value. */
  private static List<String> describeFrames(Map<String, Row> rows) {
    List<String> out = new ArrayList<>();
    rows.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
      Row row = entry.getValue();
      Row image = row.getAs(EmbeddingTableSchema.PREPARED_IMAGE);
      byte[] inline = image == null ? null : image.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD);
      float[] vector = Arrays.asList(row.schema().fieldNames()).contains(EmbeddingStreamers.MODEL_COLUMN) ? vector(row) : null;
      out.add(entry.getKey() + " aux=" + row.getAs(EmbeddingTableSchema.AUX) + " inline=" + Arrays.toString(inline)
          + " vector=" + Arrays.toString(vector));
    });
    return out;
  }
}
