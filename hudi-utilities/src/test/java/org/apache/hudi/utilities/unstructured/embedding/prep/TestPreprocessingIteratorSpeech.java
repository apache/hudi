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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioInput;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioRows;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioStubs;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubAudioSegmentSource;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubSegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptSegment;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.common.schema.HoodieSchema.Blob.INLINE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A video's speech rows in {@link PreprocessingIterator}: frames and text may interleave, each modality dense from 0 in its own
 * emit order; and an {@link Error} from the transcriber, under the real frame preprocessor with audio on, reaches the caller as
 * it is after the video's segments, the task's segment source and its transcriber are closed, once.
 */
public class TestPreprocessingIteratorSpeech {

  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType SOURCE_ROW = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.BLOB, BLOB, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType, false)});
  private static final byte[] FRAME = "jpeg-frame".getBytes(StandardCharsets.UTF_8);

  @TempDir
  Path temp;

  private String embeddingTable;

  @BeforeEach
  public void createTable() throws IOException {
    embeddingTable = temp.resolve("embedding").toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(UnstructuredTableTestProperties.layoutOne())
        .setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("embedding")
        .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS))
        .initTable(HadoopFSUtils.getStorageConf(new Configuration()), embeddingTable);
    ScriptedVideo.SCRIPT.clear();
  }

  /** No order between modalities: frames and text may interleave, each modality dense from 0 in its own emit order. */
  @Test
  public void framesAndTextInterleaveEachDenseInItsModality() {
    Chunk text = Chunk.builder(EmbeddingTableSchema.MODALITY_TEXT).text("words").tokenCount(1).build();
    ScriptedVideo.SCRIPT.addAll(Arrays.asList(frame(0), speech(0), frame(1), text));
    TypedProperties props = props();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), ScriptedVideo.class.getName());
    PreprocessingIterator iterator = iterator(props, "scripted".getBytes(StandardCharsets.UTF_8));
    List<Row> rows = new ArrayList<>();
    iterator.forEachRemaining(rows::add);
    List<String> keys = new ArrayList<>();
    for (Row row : rows) {
      keys.add(column(row, EmbeddingTableSchema.MODALITY) + "#" + column(row, EmbeddingTableSchema.CHUNK_ID));
    }
    assertEquals(Arrays.asList("image#0", "text#0", "image#1", "text#1"), keys);
    assertEquals(Collections.emptyList(), iterator.getSideOutputs(), "every file prepared");
  }

  /**
   * The real frame preprocessor with audio on, a transcriber that throws an {@link Error} at its first segment. The Error reaches
   * the caller as it is, and the segments, the segment source and the transcriber are closed, once.
   */
  @Test
  public void errorFromTheTranscriberClosesTheSegmentsTheSourceAndTheTranscriber() throws IOException {
    Path clip = VideoClips.generate(temp.resolve("clip.mp4"), 4, 10, 2, 160, 120, -1, -1);
    TypedProperties props = props();
    props.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    props.setProperty(AudioConfigKeys.SEGMENT_SOURCE_CLASS.key(), ClosingSource.class.getName());
    props.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), LinkErrorTranscriber.class.getName());
    AudioStubs.stubChunker(props);
    ClosingSource.reset();
    LinkErrorTranscriber.INSTANCES.clear();
    PreprocessingIterator iterator = iterator(props, Files.readAllBytes(clip));
    List<Row> rows = new ArrayList<>();
    UnsatisfiedLinkError error = assertThrows(UnsatisfiedLinkError.class, () -> iterator.forEachRemaining(rows::add));
    assertEquals(LinkErrorTranscriber.MESSAGE, error.getMessage());
    assertEquals(2, rows.size(), "the two frames before the speech were yielded: " + rows);
    assertEquals(1, ClosingSource.SEGMENTS_CLOSED.get(), "the video's segments are closed");
    assertAudioInstancesClosedOnce();
    iterator.close();
    assertAudioInstancesClosedOnce();
  }

  private static void assertAudioInstancesClosedOnce() {
    assertEquals(1L, ClosingSource.INSTANCES.stream().filter(source -> source.used).count(), "one source reads the video");
    assertFalse(ClosingSource.INSTANCES.stream().allMatch(source -> source.used), "logic identity uses a separate probe source");
    for (ClosingSource source : ClosingSource.INSTANCES) {
      assertEquals(1, source.closed.get(), source.used ? "runtime source closes once" : "probe source closes once");
    }
    assertEquals(1L, LinkErrorTranscriber.INSTANCES.stream().filter(transcriber -> transcriber.used).count(), "one transcriber handles speech");
    assertFalse(LinkErrorTranscriber.INSTANCES.stream().allMatch(transcriber -> transcriber.used), "logic identity uses a separate probe transcriber");
    for (LinkErrorTranscriber transcriber : LinkErrorTranscriber.INSTANCES) {
      assertEquals(1, transcriber.closed.get(), transcriber.used ? "runtime transcriber closes once" : "probe transcriber closes once");
    }
  }

  /** The stub segment source, counting the closes of its segments and of itself. */
  public static class ClosingSource extends StubAudioSegmentSource implements AutoCloseable {
    static final AtomicInteger SEGMENTS_CLOSED = new AtomicInteger();
    static final List<ClosingSource> INSTANCES = new ArrayList<>();
    private final AtomicInteger closed = new AtomicInteger();
    private boolean used;

    public ClosingSource() {
      INSTANCES.add(this);
    }

    static void reset() {
      SEGMENTS_CLOSED.set(0);
      INSTANCES.clear();
    }

    @Override
    public ClosableIterator<AudioSegment> segments(AudioInput audio, long startSegment, long startMs, TypedProperties props)
        throws IOException {
      used = true;
      ClosableIterator<AudioSegment> segments = super.segments(audio, startSegment, startMs, props);
      return new ClosableIterator<AudioSegment>() {
        @Override
        public boolean hasNext() {
          return segments.hasNext();
        }

        @Override
        public AudioSegment next() {
          return segments.next();
        }

        @Override
        public void close() {
          SEGMENTS_CLOSED.incrementAndGet();
          segments.close();
        }
      };
    }

    @Override
    public void close() {
      closed.incrementAndGet();
    }
  }

  /** A transcriber whose native engine does not link: an Error at the first segment; counts its closes. */
  public static class LinkErrorTranscriber extends StubSegmentTranscriber implements AutoCloseable {
    static final String MESSAGE = "no native engine for this platform (injected)";
    static final List<LinkErrorTranscriber> INSTANCES = new ArrayList<>();
    private final AtomicInteger closed = new AtomicInteger();
    private boolean used;

    public LinkErrorTranscriber() {
      INSTANCES.add(this);
    }

    @Override
    public ClosableIterator<TranscriptSegment> transcribe(AudioSegment segment, TypedProperties props) {
      used = true;
      throw new UnsatisfiedLinkError(MESSAGE);
    }

    @Override
    public void close() {
      closed.incrementAndGet();
    }
  }

  /** Replays {@link #SCRIPT} as a video preprocessor (stores inputs). Static state: these tests are Spark-free and single-threaded. */
  public static class ScriptedVideo implements FilePreprocessor {
    static final List<Chunk> SCRIPT = new ArrayList<>();

    @Override
    public String name() {
      return "scripted-video";
    }

    @Override
    public SourceKind sourceKind() {
      return SourceKind.VIDEO;
    }

    @Override
    public boolean storesInputs() {
      return true;
    }

    @Override
    public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props) {
      return ClosableIterator.wrap(new ArrayList<>(SCRIPT).iterator());
    }
  }

  // ---------------------------------------------------------------- helpers

  private static Chunk frame(long index) {
    Map<String, String> aux = new HashMap<>();
    aux.put(EmbeddingTableSchema.AUX_FRAME_INDEX, String.valueOf(index));
    return Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE).frameIndex(index).aux(aux)
        .payload(() -> new ByteArrayInputStream(FRAME), FRAME.length, "image/jpeg").build();
  }

  private static Chunk speech(long ordinal) {
    Map<String, String> aux = new HashMap<>();
    aux.put(AudioRows.AUX_START_MS, String.valueOf(1000L * ordinal));
    aux.put(AudioRows.AUX_END_MS, String.valueOf(1000L * ordinal + 900));
    return Chunk.builder(EmbeddingTableSchema.MODALITY_TEXT).text("speech " + ordinal).tokenCount(2).aux(aux).build();
  }

  private TypedProperties props() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), embeddingTable);
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    return props;
  }

  /** An iterator over one mp4 source row holding {@code bytes} inline. */
  private static PreprocessingIterator iterator(TypedProperties props, byte[] bytes) {
    Row blob = new GenericRowWithSchema(new Object[] {INLINE, bytes, null}, BLOB);
    Row source = new GenericRowWithSchema(new Object[] {"sha-video", "mp4", (long) bytes.length, 1L, blob, "s3://bucket/sha-video.mp4", "sha-video.mp4"}, SOURCE_ROW);
    PrepCounters counters = new PrepCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator(), new LongAccumulator());
    return new PreprocessingIterator(Collections.singletonList(source).iterator(), EmbeddingTableConfig.from(props), "001",
        new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())), new PreprocessorRegistry(props),
        new WordCountTokenizer(), counters, null);
  }

  private static Object column(Row row, String name) {
    return row.get(EmbeddingTableSchema.BASE_COLUMNS.indexOf(name));
  }
}
