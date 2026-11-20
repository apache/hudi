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
import org.apache.hudi.utilities.unstructured.embedding.image.ImageContentTypes;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreparer;
import org.apache.hudi.utilities.unstructured.embedding.input.EmbeddingInputConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.video.FfmpegKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An mp4 through one real embedding round of the single writer with FFmpeg on the class path, on both table types and in both
 * modes: the default video preprocessor (FFmpeg's keyframe frames) makes one {@code image} row per kept frame in the round's one
 * commit, at chunk_id = the frame index, each storing its JPEG frame inline in {@code prepared_image}; under
 * {@code prep_and_compute} a multimodal client embeds every frame in the same commit, under {@code prep_only} the table has no
 * vector column. The video is the job's only file. A rerun over the same version of the object changes nothing.
 */
@Tag("functional")
public class TestVideoEmbeddingRound extends EmbeddingRoundTestBase {

  /**
   * Spark 3.5's Parquet caps inline values at 80 KiB: the 29 KB clip and its small frames sit under it, the clip under the blob
   * threshold and its frames under the input store's (frames above it: TestVideoInputStoreRound).
   */
  private static final long VIDEO_INLINE_MAX_BYTES = 80L * 1024;
  private static final Path CLIP = Paths.get("src/test/resources/video/codec-matrix/c01-h264-aac.mp4");

  private TypedProperties videoProps(String mode) {
    String inputInline = String.valueOf(VIDEO_INLINE_MAX_BYTES);
    if (EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
      return props(table(), mode, VIDEO_INLINE_MAX_BYTES, EmbeddingInputConfig.INLINE_MAX_BYTES.key(), inputInline);
    }
    String model = EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL;
    TypedProperties props = props(table(), mode, VIDEO_INLINE_MAX_BYTES, EmbeddingInputConfig.INLINE_MAX_BYTES.key(), inputInline,
        model + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, DummyMultimodalEmbeddingClient.PROVIDER,
        model + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX, "text,image");
    // the provider decides the client: the base's scripted client class is removed
    props.remove(model + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX);
    return props;
  }

  /** The frames the FFmpeg source keeps for the clip under {@code props}, read straight from the file (the expected rows). */
  private static List<ExtractedFrame> expectedFrames(FixtureFile video, TypedProperties props, List<byte[]> images)
      throws IOException {
    List<ExtractedFrame> frames = new ArrayList<>();
    try (ClosableIterator<ExtractedFrame> it = new FfmpegKeyframeVideoFrameSource().frames(VideoClips.supplier(CLIP),
        new FileMeta(video.sha, video.extension, video.bytes.length), props)) {
      while (it.hasNext()) {
        ExtractedFrame frame = it.next();
        frames.add(frame);
        try (InputStream in = frame.getImage().open()) {
          images.add(in.readAllBytes());
        }
      }
    }
    return frames;
  }

  @ParameterizedTest(name = "{0}, {1}")
  @CsvSource({"COPY_ON_WRITE, prep_only", "COPY_ON_WRITE, prep_and_compute",
      "MERGE_ON_READ, prep_only", "MERGE_ON_READ, prep_and_compute"})
  public void videoFramesBecomeImageRowsInOneCommit(HoodieTableType tableType, String mode) throws Exception {
    FixtureFile video = object("videos/clip.mp4", Files.readAllBytes(CLIP));
    assertTrue(video.bytes.length <= VIDEO_INLINE_MAX_BYTES, "the clip is inline: " + video.bytes.length);
    announce(video);
    TypedProperties props = videoProps(mode);
    List<byte[]> images = new ArrayList<>();
    List<ExtractedFrame> frames = expectedFrames(video, props, images);
    assertTrue(frames.size() > 1, "the clip keeps several frames: " + frames.size());

    Round round = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());

    assertEquals(1, round.getCompleted().size(), round.toString());
    assertEquals(1, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(tableType, EmbeddingStreamers.metaClient(table(), storage()).getTableType());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), round.getCommit().get()));
    assertTrue(round.getErrors().isEmpty(), round.toString());
    assertEquals(frames.size(), round.getRowsWritten());

    Map<String, Row> rows = rows(table());
    Set<String> expectedKeys = new TreeSet<>();
    for (int k = 0; k < frames.size(); k++) {
      expectedKeys.add(key(video, EmbeddingTableSchema.MODALITY_IMAGE, k));
    }
    assertEquals(expectedKeys, rows.keySet());
    ImagePreparer preparer = new ImagePreparer(EmbeddingTableConfig.from(props).imagePreprocessing());
    for (int k = 0; k < frames.size(); k++) {
      ExtractedFrame frame = frames.get(k);
      Row row = rows.get(key(video, EmbeddingTableSchema.MODALITY_IMAGE, k));
      assertEquals(video.sha, row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA));
      assertEquals(video.sourcePath, row.getAs(EmbeddingTableSchema.SOURCE_FILE_PATH));
      assertEquals(video.extension, row.getAs(EmbeddingTableSchema.SOURCE_FILE_EXTENSION));
      assertEquals((long) video.bytes.length, (long) row.getAs(EmbeddingTableSchema.SOURCE_FILE_SIZE));
      assertNull(row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
      scala.collection.Map<String, String> aux = row.getAs(EmbeddingTableSchema.AUX);
      assertEquals(String.valueOf(k), aux.apply(EmbeddingTableSchema.AUX_FRAME_INDEX));
      assertEquals(String.valueOf(frame.getTimeMs()), aux.apply(VideoFramePreprocessor.AUX_TIMESTAMP_MS));
      assertEquals(frame.getDedupKey(), aux.apply(VideoFramePreprocessor.AUX_DEDUP_KEY));
      assertEquals(String.valueOf(frame.getWidth()), aux.apply(EmbeddingTableSchema.AUX_INPUT_WIDTH));
      assertEquals(String.valueOf(frame.getHeight()), aux.apply(EmbeddingTableSchema.AUX_INPUT_HEIGHT));
      assertEquals(prepLogicVersion("video"), aux.apply(EmbeddingTableSchema.AUX_PREP_LOGIC_VERSION));
      assertEquals(ImageContentTypes.JPEG, frame.getMimeType());

      Row prepared = row.getAs(EmbeddingTableSchema.PREPARED_IMAGE);
      assertEquals(HoodieSchema.Blob.INLINE, prepared.getAs(HoodieSchema.Blob.TYPE), "a frame is stored inline");
      assertArrayEquals(images.get(k), (byte[]) prepared.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD), "frame " + k);
      assertArrayEquals(images.get(k), read(prepared), "frame " + k + " reads back");

      if (EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
        assertFalse(Arrays.asList(row.schema().fieldNames()).contains(EmbeddingStreamers.MODEL_COLUMN),
            "prep_only writes no model column: " + row.schema().simpleString());
      } else {
        float[] expected = DummyMultimodalEmbeddingClient.vectorForImage(
            preparer.prepare(ImageFixtures.source(images.get(k))).getEncoded(), EmbeddingStreamers.MODEL_DIMENSIONS);
        assertArrayEquals(expected, vector(row), "frame " + k);
      }
    }

    // the same version announced again (same modification time and size): the same keys and values, no new rows
    List<String> before = describeWithFrames(rows);
    Version version = versionOf(video);
    announce(video);
    touch(video, Instant.ofEpochMilli(version.modificationTime));
    Round rerun = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());
    assertEquals(1, rerun.getCompleted().size(), rerun.toString());
    assertEquals(before, describeWithFrames(rows(table())));
  }

  /** {@link #describe} with each inline frame's bytes by content (a byte array prints by identity). */
  private static List<String> describeWithFrames(Map<String, Row> rows) {
    List<String> out = new ArrayList<>();
    for (Map.Entry<String, Row> entry : new TreeMap<>(rows).entrySet()) {
      Row prepared = entry.getValue().getAs(EmbeddingTableSchema.PREPARED_IMAGE);
      byte[] frame = prepared.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD);
      out.add(entry.getValue().toString().replaceAll("\\[B@[0-9a-f]+", "") + " frame=" + Arrays.toString(frame));
    }
    return out;
  }

  private byte[] read(Row preparedImage) throws IOException {
    try (InputStream in = new BlobBytesSource(storage()).open(preparedImage)) {
      return in.readAllBytes();
    }
  }
}
