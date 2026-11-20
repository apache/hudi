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
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioRows;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubSegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyMultimodalEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.prep.Chunk;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessingException;
import org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoDecodeException;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
 * An mp4 with speech through one real embedding round of the single writer, with FFmpeg on the class path and audio on, on both
 * table types and in both modes: the default video preprocessor makes one {@code image} row per kept frame and, after them, one
 * {@code text} row per transcript chunk (the default FFmpeg audio segment source cuts the speech, the stub transcriber says one
 * fixed line per segment, the default transcript packer packs two lines per chunk at {@code chunk.max.tokens} = 16), all in the
 * round's one commit. Under
 * {@code prep_and_compute} the multimodal client embeds the frames and the speech in the same commit; under {@code prep_only} the
 * table has no vector column. The expected rows are what the preprocessor makes when run on the file alone. A rerun over the same
 * version of the object changes nothing.
 */
@Tag("functional")
public class TestSpeechEmbeddingRound extends EmbeddingRoundTestBase {

  /** Spark 3.5's Parquet caps inline values at 80 KiB: the clip is stored out of line, its frames inline. */
  private static final long VIDEO_INLINE_MAX_BYTES = 80L * 1024;
  private static final Path CLIP = Paths.get("src/test/resources/video/h264-aac-speech-12s.mp4");
  /** An mp4 with an audio track and no video stream. */
  private static final Path AUDIO_ONLY = Paths.get("src/test/resources/video/aac-speech-audio-only.mp4");

  private TypedProperties speechProps(String mode) {
    String[] audio = {AudioConfigKeys.ENABLED.key(), "true",
        AudioConfigKeys.TRANSCRIBER_CLASS.key(), StubSegmentTranscriber.class.getName(),
        // the table's floor: the packer fits two of the stub's fixed lines in a chunk, so the speech makes several chunks
        EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "16"};
    if (EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
      return props(table(), mode, VIDEO_INLINE_MAX_BYTES, audio);
    }
    String model = EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL;
    List<String> overrides = new ArrayList<>(Arrays.asList(audio));
    overrides.add(model + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX);
    overrides.add(DummyMultimodalEmbeddingClient.PROVIDER);
    overrides.add(model + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX);
    overrides.add("text,image");
    TypedProperties props = props(table(), mode, VIDEO_INLINE_MAX_BYTES, overrides.toArray(new String[0]));
    // the provider decides the client: the base's scripted client class is removed
    props.remove(model + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX);
    return props;
  }

  /** The chunks the preprocessor makes for the clip under {@code props}, run on the file alone (the expected rows). */
  private static List<Chunk> expectedChunks(FixtureFile video, TypedProperties props) throws Exception {
    return expectedChunks(CLIP, video, props);
  }

  /** The chunks the preprocessor makes for {@code clip} before it ends or fails, run on the file alone. */
  private static List<Chunk> expectedChunks(Path clip, FixtureFile video, TypedProperties props) throws Exception {
    List<Chunk> chunks = new ArrayList<>();
    VideoFramePreprocessor preprocessor = new VideoFramePreprocessor();
    try (ClosableIterator<Chunk> it = preprocessor.open(VideoClips.supplier(clip),
        new FileMeta(video.sha, video.extension, video.bytes.length), TokenizerFactory.create(props), props)) {
      while (it.hasNext()) {
        chunks.add(it.next());
      }
    } catch (PreprocessingException | VideoDecodeException e) {
      // the file's content failure, thrown after its speech: the chunks before it stay
    } finally {
      preprocessor.close();
    }
    return chunks;
  }

  @ParameterizedTest(name = "{0}, {1}")
  @CsvSource({"COPY_ON_WRITE, prep_only", "COPY_ON_WRITE, prep_and_compute",
      "MERGE_ON_READ, prep_only", "MERGE_ON_READ, prep_and_compute"})
  public void videoFramesAndSpeechBecomeRowsInOneCommit(HoodieTableType tableType, String mode) throws Exception {
    FixtureFile video = object("videos/speech.mp4", Files.readAllBytes(CLIP));
    announce(video);
    TypedProperties props = speechProps(mode);
    List<Chunk> expected = expectedChunks(video, props);
    List<Chunk> frames = new ArrayList<>();
    List<Chunk> speech = new ArrayList<>();
    for (Chunk chunk : expected) {
      (EmbeddingTableSchema.MODALITY_TEXT.equals(chunk.getModality()) ? speech : frames).add(chunk);
    }
    assertFalse(frames.isEmpty(), "the clip keeps frames");
    assertTrue(speech.size() > 1, "the clip's speech makes several chunks: " + speech.size());

    Round round = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());

    assertEquals(1, round.getCompleted().size(), round.toString());
    assertEquals(1, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(tableType, EmbeddingStreamers.metaClient(table(), storage()).getTableType());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), round.getCommit().get()));
    assertTrue(round.getErrors().isEmpty(), round.toString());
    assertEquals(expected.size(), round.getRowsWritten());

    Map<String, Row> rows = rows(table());
    Set<String> expectedKeys = new TreeSet<>();
    for (int k = 0; k < frames.size(); k++) {
      expectedKeys.add(key(video, EmbeddingTableSchema.MODALITY_IMAGE, k));
    }
    for (int k = 0; k < speech.size(); k++) {
      expectedKeys.add(key(video, EmbeddingTableSchema.MODALITY_TEXT, k));
    }
    assertEquals(expectedKeys, rows.keySet());

    long previousStart = -1;
    for (int k = 0; k < speech.size(); k++) {
      Chunk chunk = speech.get(k);
      Row row = rows.get(key(video, EmbeddingTableSchema.MODALITY_TEXT, k));
      assertEquals(video.sha, row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA));
      assertEquals(video.sourcePath, row.getAs(EmbeddingTableSchema.SOURCE_FILE_PATH));
      assertEquals(chunk.getText(), row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK), "speech chunk " + k);
      assertTrue(chunk.getText().contains("Utterance of audio segment"), chunk.getText());
      assertNull(row.getAs(EmbeddingTableSchema.PREPARED_IMAGE), "a speech row stores no image");
      scala.collection.Map<String, String> aux = row.getAs(EmbeddingTableSchema.AUX);
      for (String auxKey : Arrays.asList(AudioRows.AUX_START_MS, AudioRows.AUX_END_MS)) {
        assertEquals(chunk.getAux().get(auxKey), aux.apply(auxKey), "speech chunk " + k + " " + auxKey);
      }
      assertEquals(prepLogicVersion("audio"), aux.apply(EmbeddingTableSchema.AUX_PREP_LOGIC_VERSION));
      long start = Long.parseLong(aux.apply(AudioRows.AUX_START_MS));
      long end = Long.parseLong(aux.apply(AudioRows.AUX_END_MS));
      assertTrue(start < end && start >= previousStart, "chunk " + k + " spans [" + start + ", " + end + ") after "
          + previousStart);
      previousStart = start;
      if (EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
        assertFalse(Arrays.asList(row.schema().fieldNames()).contains(EmbeddingStreamers.MODEL_COLUMN),
            "prep_only writes no model column: " + row.schema().simpleString());
      } else {
        assertArrayEquals(DummyEmbeddingClient.vectorFor(video.sha, k, EmbeddingStreamers.MODEL_DIMENSIONS), vector(row),
            "speech chunk " + k);
      }
    }
    for (int k = 0; k < frames.size(); k++) {
      Row row = rows.get(key(video, EmbeddingTableSchema.MODALITY_IMAGE, k));
      assertNull(row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
      scala.collection.Map<String, String> aux = row.getAs(EmbeddingTableSchema.AUX);
      assertEquals(String.valueOf(k), aux.apply(EmbeddingTableSchema.AUX_FRAME_INDEX));
      if (!EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
        assertEquals(EmbeddingStreamers.MODEL_DIMENSIONS, vector(row).length, "frame " + k + " is embedded");
      }
    }

    // the same version announced again (same modification time and size): the same keys and values, no new rows
    List<String> before = describeByContent(rows);
    Version version = versionOf(video);
    announce(video);
    touch(video, Instant.ofEpochMilli(version.modificationTime));
    Round rerun = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());
    assertEquals(1, rerun.getCompleted().size(), rerun.toString());
    assertEquals(before, describeByContent(rows(table())));
  }

  /**
   * An audio-only mp4 (no video stream) beside a video, in one round, in both modes: the audio-only file's speech rows are
   * written (and, under prep_and_compute, embedded) and the file then fails (its prep error names the missing video stream:
   * the speech is prepared whatever the frames do); the video's frames and speech are written with no error; one commit.
   */
  @ParameterizedTest(name = "{0}, {1}")
  @CsvSource({"COPY_ON_WRITE, prep_only", "COPY_ON_WRITE, prep_and_compute",
      "MERGE_ON_READ, prep_only", "MERGE_ON_READ, prep_and_compute"})
  public void anAudioOnlyFileWritesItsSpeechThenFailsForItsMissingVideoStream(HoodieTableType tableType, String mode)
      throws Exception {
    FixtureFile video = object("videos/speech.mp4", Files.readAllBytes(CLIP));
    FixtureFile audioOnly = object("videos/audio-only.mp4", Files.readAllBytes(AUDIO_ONLY));
    announce(video, audioOnly);
    TypedProperties props = speechProps(mode);
    List<Chunk> expectedVideo = expectedChunks(CLIP, video, props);
    List<Chunk> expectedAudioOnly = expectedChunks(AUDIO_ONLY, audioOnly, props);
    assertFalse(expectedAudioOnly.isEmpty(), "the audio-only file has speech");
    assertTrue(expectedAudioOnly.stream().allMatch(chunk -> EmbeddingTableSchema.MODALITY_TEXT.equals(chunk.getModality())),
        "no frame");

    Round round = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());

    assertEquals(1, round.getCompleted().size(), round.toString());
    assertEquals(1, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(expectedVideo.size() + expectedAudioOnly.size(), round.getRowsWritten(), round.toString());
    assertEquals(1, round.getErrors().size(), round.toString());
    Map<String, Object> error = round.getErrors().get(0);
    assertEquals(audioOnly.sha, error.get("source_path_sha"));
    assertEquals("prep", error.get("stage"));
    assertTrue(String.valueOf(error.get("error")).contains("has no video stream"), error.toString());

    Map<String, Row> rows = rows(table());
    for (int k = 0; k < expectedAudioOnly.size(); k++) {
      Row row = rows.get(key(audioOnly, EmbeddingTableSchema.MODALITY_TEXT, k));
      assertEquals(expectedAudioOnly.get(k).getText(), row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK), "speech chunk " + k);
      if (EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
        assertFalse(Arrays.asList(row.schema().fieldNames()).contains(EmbeddingStreamers.MODEL_COLUMN));
      } else {
        // the speech rows written before the file's failure are embedded in the same commit
        assertArrayEquals(DummyEmbeddingClient.vectorFor(audioOnly.sha, k, EmbeddingStreamers.MODEL_DIMENSIONS), vector(row),
            "speech chunk " + k);
      }
    }
    assertEquals(expectedVideo.size() + expectedAudioOnly.size(), rows.size(), rows.keySet().toString());
  }

  /** {@link #describe} with every inline byte array printed by content (a byte array prints by identity). */
  private static List<String> describeByContent(Map<String, Row> rows) {
    List<String> out = new ArrayList<>();
    for (Map.Entry<String, Row> entry : new TreeMap<>(rows).entrySet()) {
      Row row = entry.getValue();
      Row prepared = row.getAs(EmbeddingTableSchema.PREPARED_IMAGE);
      byte[] image = prepared == null ? null : prepared.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD);
      out.add(row.toString().replaceAll("\\[B@[0-9a-f]+", "") + " image=" + Arrays.toString(image));
    }
    return out;
  }
}
