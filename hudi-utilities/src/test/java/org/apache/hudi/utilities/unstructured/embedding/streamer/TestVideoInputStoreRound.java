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

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FailingPreCommitValidator;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyMultimodalEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreparer;
import org.apache.hudi.utilities.unstructured.embedding.input.EmbeddingInputConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.video.FfmpegKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;
import org.apache.hudi.utilities.unstructured.storage.ManagedBlobPaths;
import org.apache.hudi.utilities.unstructured.storage.ParquetInlineCap;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real-size video frames above Spark 3.5's 80 KiB inline cap through one real embedding round of the single writer, on both
 * table types and in both modes, with the input store's inline threshold at that cap: every kept frame (a 1280x720 JPEG of about
 * 200 KiB) is written out of line under the embedding table's {@code .hoodie/.blob} folder in the round's one commit, and its row
 * keeps a managed reference to that file; under {@code prep_and_compute} compute reads each frame back through the reference and
 * embeds it. An overwrite of the video adds the new version's frames, with files of their own, and keeps the old version's rows
 * and files. A failed round's payload files are removed by its rollback: at once for a failure inside the round, by the next
 * round for a failed commit.
 */
@Tag("functional")
class TestVideoInputStoreRound extends EmbeddingRoundTestBase {

  private static final Path CLIP = Paths.get("src/test/resources/video/large-frames/large-frames-720p.mp4");
  /** The video itself is above the blob threshold too: prep streams it from the object. */
  private static final long BLOB_INLINE_MAX_BYTES = ParquetInlineCap.MAX_BYTES;

  /** The Spark 3.5 settings: the input store's threshold at the cap a Parquet without per-column statistics allows. */
  private TypedProperties videoProps(String mode) {
    String inputInline = String.valueOf(ParquetInlineCap.MAX_BYTES);
    if (EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
      return props(table(), mode, BLOB_INLINE_MAX_BYTES, EmbeddingInputConfig.INLINE_MAX_BYTES.key(), inputInline);
    }
    String model = EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL;
    TypedProperties props = props(table(), mode, BLOB_INLINE_MAX_BYTES, EmbeddingInputConfig.INLINE_MAX_BYTES.key(), inputInline,
        model + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, DummyMultimodalEmbeddingClient.PROVIDER);
    // the provider decides the client: the base's scripted client class is removed
    props.remove(model + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX);
    return props;
  }

  /** The frames' JPEG bytes the FFmpeg source keeps for the clip under {@code props}, read straight from the file. */
  private static List<byte[]> expectedFrames(FixtureFile video, TypedProperties props) throws IOException {
    List<byte[]> images = new ArrayList<>();
    try (ClosableIterator<ExtractedFrame> it = new FfmpegKeyframeVideoFrameSource().frames(VideoClips.supplier(CLIP),
        new FileMeta(video.sha, video.extension, video.bytes.length), props)) {
      while (it.hasNext()) {
        try (InputStream in = it.next().getImage().open()) {
          images.add(in.readAllBytes());
        }
      }
    }
    return images;
  }

  /** The payload files under the embedding table's blob folder, relative to its base, the checksum files left out. */
  private List<String> payloadFiles() throws IOException {
    Path blob = localPath(table()).resolve(HoodieTableMetaClient.METAFOLDER_NAME + "/.blob");
    if (!Files.exists(blob)) {
      return Collections.emptyList();
    }
    try (Stream<Path> files = Files.walk(blob)) {
      return files.filter(Files::isRegularFile).filter(file -> !file.getFileName().toString().endsWith(".crc"))
          .map(file -> localPath(table()).relativize(file).toString()).sorted().collect(Collectors.toList());
    }
  }

  /**
   * Checks that row {@code key} stores {@code image} out of line in a managed payload file of the table and, under compute, has
   * the vector of that image; returns the file's relative path.
   */
  private String assertStoredOutOfLine(Map<String, Row> rows, String key, byte[] image, String mode, ImagePreparer preparer)
      throws IOException {
    Row row = rows.get(key);
    Row prepared = row.getAs(EmbeddingTableSchema.PREPARED_IMAGE);
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, prepared.getAs(HoodieSchema.Blob.TYPE), key);
    assertNull(prepared.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD), "the row holds the reference only: " + key);
    Row reference = prepared.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE);
    String path = reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH);
    assertTrue(ManagedBlobPaths.isManagedBlobPath(path) && path.endsWith(".bin"), path);
    assertEquals(0L, (long) reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET));
    assertEquals((long) image.length, (long) reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH));
    assertEquals(true, reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED), key);
    assertArrayEquals(image, Files.readAllBytes(localPath(table()).resolve(path)), "the payload file holds the frame: " + key);
    if (EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
      assertFalse(Arrays.asList(row.schema().fieldNames()).contains(EmbeddingStreamers.MODEL_COLUMN),
          "prep_only writes no model column: " + row.schema().simpleString());
    } else {
      float[] expected = DummyMultimodalEmbeddingClient.vectorForImage(
          preparer.prepare(ImageFixtures.source(image)).getEncoded(), EmbeddingStreamers.MODEL_DIMENSIONS);
      assertArrayEquals(expected, vector(row), key);
    }
    return path;
  }

  /** A text-only allow-list excludes input-storing preprocessors from the driver's heap-ceiling check. */
  @Test
  void inputStoreDriverCheckHonorsPrepAllowList() {
    TypedProperties props = videoProps(EmbeddingTableConfig.MODE_PREP_ONLY);
    props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "pdf");
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_HEAP_FRACTION.key(), "0.000000000001");

    assertDoesNotThrow(() -> EmbeddingTableTransformer.checkRound(EmbeddingTableConfig.from(props), spark(), Option.empty()));
  }

  /** Including a video extension still enforces the input store's heap ceiling before any round. */
  @Test
  void inputStoreDriverCheckStillRefusesPreparedVideo() {
    TypedProperties props = videoProps(EmbeddingTableConfig.MODE_PREP_ONLY);
    props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "pdf,mp4");
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_HEAP_FRACTION.key(), "0.000000000001");

    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    SparkSession spark = spark();
    Option<StructType> noTable = Option.empty();
    HoodieValidationException refused = assertThrows(HoodieValidationException.class, () -> EmbeddingTableTransformer.checkRound(config, spark, noTable));
    assertTrue(refused.getMessage().contains(EmbeddingInputConfig.INLINE_MAX_HEAP_FRACTION.key()), refused.getMessage());
  }

  @Test
  void inputThresholdFollowsTheParquetCapOfThisBuild() {
    long threshold = EmbeddingInputConfig.inlineMaxBytes(new TypedProperties());
    if (HoodieBaseParquetWriter.supportsColumnStatisticsSwitch()) {
      assertEquals(EmbeddingInputConfig.DEFAULT_INLINE_MAX_BYTES, threshold);
    } else {
      // the Spark 3.5 builds: 80 KiB by default, the most used there; a larger value is used as 80 KiB
      assertEquals(ParquetInlineCap.MAX_BYTES, threshold);
      TypedProperties large = new TypedProperties();
      large.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(1L << 20));
      assertEquals(ParquetInlineCap.MAX_BYTES, EmbeddingInputConfig.inlineMaxBytes(large));
    }
  }

  @ParameterizedTest(name = "{0}, {1}")
  @CsvSource({"COPY_ON_WRITE, prep_only", "COPY_ON_WRITE, prep_and_compute",
      "MERGE_ON_READ, prep_only", "MERGE_ON_READ, prep_and_compute"})
  void framesAboveTheInlineCapAreStoredOutOfLineAndAnOverwriteKeepsTheOldVersion(HoodieTableType tableType, String mode)
      throws Exception {
    FixtureFile video = object("videos/large.mp4", Files.readAllBytes(CLIP));
    assertTrue(video.bytes.length > BLOB_INLINE_MAX_BYTES, "the video is streamed from its object: " + video.bytes.length);
    announce(video);
    TypedProperties props = videoProps(mode);
    // with the error table on, a derived frame refused inline would be recorded as an error and the frames after it skipped,
    // and the round would still commit: every frame, the first included, must instead reach the table out of line
    assertEquals("true", props.getString(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key()), "the error table is on");
    List<byte[]> frames = expectedFrames(video, props);
    assertTrue(frames.size() > 1, "the clip keeps several frames: " + frames.size());
    for (byte[] frame : frames) {
      assertTrue(frame.length > ParquetInlineCap.MAX_BYTES, "every frame is above the 80 KiB cap: " + frame.length);
    }

    Round round = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());

    assertEquals(1, round.getCompleted().size(), round.toString());
    assertEquals(1, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(tableType, EmbeddingStreamers.metaClient(table(), storage()).getTableType());
    assertTrue(round.getErrors().isEmpty(), round.toString());
    assertEquals(frames.size(), round.getRowsWritten());
    Version first = versionOf(video);
    Map<String, Row> rows = rows(table());
    Set<String> firstKeys = new TreeSet<>();
    for (int k = 0; k < frames.size(); k++) {
      firstKeys.add(key(video, first, EmbeddingTableSchema.MODALITY_IMAGE, k));
    }
    assertEquals(firstKeys, rows.keySet());
    ImagePreparer preparer = new ImagePreparer(EmbeddingTableConfig.from(props).imagePreprocessing());
    List<String> firstFiles = new ArrayList<>();
    for (int k = 0; k < frames.size(); k++) {
      firstFiles.add(assertStoredOutOfLine(rows, key(video, first, EmbeddingTableSchema.MODALITY_IMAGE, k), frames.get(k), mode,
          preparer));
    }
    firstFiles.sort(null);
    assertEquals(firstFiles, payloadFiles(), "one payload file per frame, nothing else");

    // an overwrite (a later modification time): the new version's frames are added with files of their own; the old version's
    // rows and files stay
    announce(video);
    touch(video, Instant.ofEpochMilli(first.modificationTime + 60_000L));
    Version second = versionOf(video);
    Round overwrite = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());
    assertEquals(1, overwrite.getCompleted().size(), overwrite.toString());
    assertEquals(frames.size(), overwrite.getRowsWritten());
    Map<String, Row> after = rows(table());
    assertEquals(2 * frames.size(), after.size(), after.keySet().toString());
    List<String> allFiles = new ArrayList<>();
    for (int k = 0; k < frames.size(); k++) {
      String old = assertStoredOutOfLine(after, key(video, first, EmbeddingTableSchema.MODALITY_IMAGE, k), frames.get(k), mode,
          preparer);
      String current = assertStoredOutOfLine(after, key(video, second, EmbeddingTableSchema.MODALITY_IMAGE, k), frames.get(k),
          mode, preparer);
      assertNotEquals(old, current, "each version references its own file: " + old);
      allFiles.add(old);
      allFiles.add(current);
    }
    allFiles.sort(null);
    assertEquals(allFiles, payloadFiles(), "both versions' payload files, nothing else");
    assertTrue(allFiles.containsAll(firstFiles), "the old version's files are kept");
  }

  /**
   * A round that fails inside the round, after data prep wrote its payload files (here a second video that cannot be decoded,
   * with the error table off, which fails the round), is rolled back at once by the streamer through its markers: no payload
   * file is left, and nothing is committed. The rerun without the bad video commits the frames with fresh files.
   */
  @ParameterizedTest(name = "{0}")
  @CsvSource({"COPY_ON_WRITE", "MERGE_ON_READ"})
  void roundThatFailsAfterPrepWroteItsPayloadsIsRolledBackWithThem(HoodieTableType tableType) throws Exception {
    FixtureFile video = object("videos/large.mp4", Files.readAllBytes(CLIP));
    FixtureFile broken = object("videos/broken.mp4", "not a video".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    announce(video, broken);
    TypedProperties props = videoProps(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "false");
    List<byte[]> frames = expectedFrames(video, props);

    SparkSession spark = spark();
    Exception failure = assertThrows(Exception.class, () -> EmbeddingStreamers.round(spark, props, cfg -> cfg.tableType = tableType.name()));
    assertTrue(EmbeddingStreamers.causes(failure).contains("broken.mp4"), EmbeddingStreamers.causes(failure));
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty(), "nothing committed");
    assertEquals(Collections.emptyList(), payloadFiles(), "the rollback removes the failed round's payload files");
    // the failed round did write its frames out of line: its one rollback deleted exactly one payload file per frame
    HoodieTableMetaClient metaClient = EmbeddingStreamers.metaClient(table(), storage());
    List<HoodieInstant> rollbacks = metaClient.getActiveTimeline().getRollbackTimeline().filterCompletedInstants().getInstants();
    assertEquals(1, rollbacks.size(), rollbacks.toString());
    HoodieRollbackMetadata rollback = metaClient.getActiveTimeline().readRollbackMetadata(rollbacks.get(0));
    List<String> deletedPayloads = rollback.getPartitionMetadata().values().stream()
        .flatMap(partition -> partition.getSuccessDeleteFiles().stream())
        .filter(file -> file.contains(HoodieTableMetaClient.METAFOLDER_NAME + "/.blob/") && file.endsWith(".bin"))
        .collect(Collectors.toList());
    assertEquals(frames.size(), deletedPayloads.size(), "the rollback deleted the payload files: " + rollback);

    // the bad video is fixed by its owner: a later version that decodes (the same clip) commits with fresh files
    announce(object("videos/broken.mp4", Files.readAllBytes(CLIP)));
    Round rerun = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());
    assertEquals(1, rerun.getCompleted().size(), rerun.toString());
    assertEquals(2 * frames.size(), rows(table()).size());
    assertEquals(2 * frames.size(), payloadFiles().size(), payloadFiles().toString());
  }

  /**
   * A round whose commit fails after its write (a write-client pre-commit validator) leaves its instant pending, as today's
   * streamer does for any failed commit; the next round rolls it back, through its markers, before it writes: no payload file
   * of the failed round is left beside the committed round's.
   */
  @ParameterizedTest(name = "{0}")
  @CsvSource({"COPY_ON_WRITE", "MERGE_ON_READ"})
  void failedCommitsPayloadFilesAreRolledBackByTheNextRound(HoodieTableType tableType) throws Exception {
    FixtureFile video = object("videos/large.mp4", Files.readAllBytes(CLIP));
    announce(video);
    TypedProperties props = videoProps(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    List<byte[]> frames = expectedFrames(video, props);

    FailingPreCommitValidator.RECORDS_WRITTEN.set(-1);
    TypedProperties failing = TypedProperties.copy(props);
    failing.setProperty(HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.key(), FailingPreCommitValidator.class.getName());
    SparkSession spark = spark();
    Exception failure = assertThrows(Exception.class, () -> EmbeddingStreamers.round(spark, failing, cfg -> cfg.tableType = tableType.name()));
    assertTrue(EmbeddingStreamers.causeOf(failure, HoodieValidationException.class).isPresent(), EmbeddingStreamers.causes(failure));
    assertTrue(FailingPreCommitValidator.RECORDS_WRITTEN.get() >= frames.size(), "the failed round wrote its rows first");
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty(), "nothing committed");
    List<String> failedFiles = payloadFiles();
    assertEquals(frames.size(), failedFiles.size(), "the failed round's payload files wait for its rollback: " + failedFiles);

    Round rerun = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());
    assertEquals(1, rerun.getCompleted().size(), rerun.toString());
    Map<String, Row> rows = rows(table());
    assertEquals(frames.size(), rows.size());
    ImagePreparer preparer = new ImagePreparer(EmbeddingTableConfig.from(props).imagePreprocessing());
    List<String> files = new ArrayList<>();
    for (int k = 0; k < frames.size(); k++) {
      files.add(assertStoredOutOfLine(rows, key(video, EmbeddingTableSchema.MODALITY_IMAGE, k), frames.get(k),
          EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, preparer));
    }
    files.sort(null);
    assertEquals(files, payloadFiles(), "only the committed round's payload files: the failed round's were rolled back");
    for (String failed : failedFiles) {
      assertFalse(files.contains(failed), failed);
    }
  }
}
