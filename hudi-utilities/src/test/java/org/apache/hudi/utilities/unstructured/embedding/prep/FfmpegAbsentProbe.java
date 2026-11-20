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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.common.schema.HoodieSchema.Blob.INLINE;

/**
 * Runs, inside a class loader that has no {@code org.bytedeco} classes (no FFmpeg jars, as with the shipped bundles), what a job
 * does with the DEFAULT video preprocessor: the driver's registry and config check of a PDF-only job and of a video job, the
 * opening of one video, and two prep rounds through {@link PreprocessingIterator} (PDF only; PDF then video). Loaded and called by
 * {@link TestFfmpegAbsentClassGraph} through that loader; it reports what it saw as one line per fact and throws when a fact
 * does not hold.
 *
 * <p>Its class loader records the name of every class it is asked to find ({@link Supplier} of that list, a JDK type both
 * loaders share), so the report names the FFmpeg classes the driver checks tried to load: none.
 */
public class FfmpegAbsentProbe implements Callable<String> {

  /** A name prefix of the classes the driver checks must never load: FFmpeg's, and the FFmpeg strategy itself. */
  static final List<String> FFMPEG_CLASS_PREFIXES =
      Arrays.asList("org.bytedeco.", VideoFramePreprocessor.FFMPEG_STRATEGY_CLASS);

  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType SOURCE_ROW = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.BLOB, BLOB, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType, false)});
  private static final byte[] NOT_A_VIDEO = "not decoded: FFmpeg is absent".getBytes(StandardCharsets.UTF_8);

  @Override
  public String call() throws Exception {
    StringBuilder report = new StringBuilder();
    Tokenizer tokenizer = new WordCountTokenizer();

    // the driver side of a PDF-only job with the default configuration: every mapped extension is checked, video included
    TypedProperties pdfOnly = new TypedProperties();
    PreprocessorRegistry registry = new PreprocessorRegistry(pdfOnly);
    registry.validateConfig(pdfOnly, tokenizer);
    report.append("pdf validateConfig ok\n");
    // a video job: the video key named and the frame keys set
    TypedProperties videoJob = new TypedProperties();
    videoJob.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), VideoFramePreprocessor.class.getName());
    videoJob.setProperty(EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key(), "2000");
    videoJob.setProperty(EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key(), "sha256");
    new PreprocessorRegistry(videoJob).validateConfig(videoJob, tokenizer);
    report.append("video validateConfig ok\n");
    report.append("video preprocessor ").append(registry.forExtension("mp4").get().getClass().getName()).append('\n');
    report.append("validateConfig loaded ffmpeg classes ").append(ffmpegClassesAsked()).append('\n');
    // checked after the record above: this lookup itself asks the loader for an FFmpeg class
    try {
      Class.forName("org.bytedeco.javacv.FFmpegFrameGrabber", false, getClass().getClassLoader());
      throw new IllegalStateException("the probe's class loader can load org.bytedeco: the FFmpeg jars are not hidden");
    } catch (ClassNotFoundException expected) {
      report.append("bytedeco hidden\n");
    }

    // processing a video is what needs FFmpeg
    report.append(openVideo(registry, tokenizer, pdfOnly));
    report.append(rounds());
    return report.toString();
  }

  /** The FFmpeg classes this probe's class loader was asked to find so far. */
  private List<String> ffmpegClassesAsked() {
    List<?> asked = (List<?>) ((Supplier<?>) getClass().getClassLoader()).get();
    return asked.stream().map(String::valueOf).filter(name -> FFMPEG_CLASS_PREFIXES.stream().anyMatch(name::startsWith))
        .distinct().collect(Collectors.toList());
  }

  /** Reports the unavailable video's quarantine outcome (and the older configuration refusal for the baseline replay). */
  private static String openVideo(PreprocessorRegistry registry, Tokenizer tokenizer, TypedProperties props) throws Exception {
    FilePreprocessor preprocessor = registry.forExtension("mp4").get();
    // seekable, as the prep framework's source rows are: the FFmpeg source refuses any other bytes before it links FFmpeg
    try (ClosableIterator<Chunk> chunks = preprocessor.open(new InMemorySeekableBytes(NOT_A_VIDEO),
        new FileMeta("v1", "mp4", NOT_A_VIDEO.length), tokenizer, props)) {
      throw new IllegalStateException("video opened without FFmpeg: " + chunks.hasNext());
    } catch (PreprocessingException e) {
      return "video quarantined " + e.getMessage() + '\n';
    } catch (PreprocessorConfigurationException e) {
      return "video refused " + e.getMessage() + '\n' + "video cause " + e.getCause() + '\n';
    }
  }

  /**
   * A PDF-only round passes; a mixed round retains the PDF's rows and quarantines the unavailable video.
   */
  private static String rounds() throws IOException {
    StringBuilder report = new StringBuilder();
    byte[] pdf = PdfFixtures.text("alpha beta gamma delta");
    PreprocessingIterator pdfRound = iterator(sourceRow("sha-pdf", "pdf", pdf));
    List<Row> pdfRows = drain(pdfRound);
    report.append("pdf round rows ").append(pdfRows.size()).append(" side outputs ").append(pdfRound.getSideOutputs().size())
        .append('\n');

    PreprocessingIterator mixedRound = iterator(sourceRow("sha-pdf", "pdf", pdf), sourceRow("sha-video", "mp4", NOT_A_VIDEO));
    List<Row> mixedRows = new ArrayList<>();
    try {
      while (mixedRound.hasNext()) {
        mixedRows.add(mixedRound.next());
      }
      report.append("mixed round passed\n");
      report.append("video error ").append(mixedRound.getSideOutputs().get(0).getError()).append('\n');
    } catch (PreprocessorConfigurationException e) {
      report.append("mixed round failed ").append(e.getMessage()).append('\n');
    }
    report.append("mixed round rows ").append(mixedRows.size()).append(" side outputs ")
        .append(mixedRound.getSideOutputs().size()).append('\n');
    return report.toString();
  }

  private static Row sourceRow(String sha, String extension, byte[] bytes) {
    Row blob = new GenericRowWithSchema(new Object[] {INLINE, bytes, null}, BLOB);
    return new GenericRowWithSchema(new Object[] {sha, extension, (long) bytes.length, 1L, blob, "s3://bucket/" + sha + "." + extension, sha + "." + extension}, SOURCE_ROW);
  }

  private static PreprocessingIterator iterator(Row... rows) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), "/tmp/ffmpeg-absent-probe-unused");
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    PrepCounters counters = new PrepCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator(), new LongAccumulator());
    return new PreprocessingIterator(
        Arrays.asList(rows).iterator(),
        EmbeddingTableConfig.from(props),
        new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())),
        new PreprocessorRegistry(props),
        new WordCountTokenizer(),
        counters,
        null);
  }

  private static List<Row> drain(PreprocessingIterator iterator) {
    List<Row> rows = new ArrayList<>();
    while (iterator.hasNext()) {
      rows.add(iterator.next());
    }
    return rows;
  }

  /** Random access over bytes in memory. */
  private static final class InMemorySeekableBytes implements SeekableBlobBytesSupplier {
    private final byte[] bytes;

    private InMemorySeekableBytes(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public InputStream open() {
      return new ByteArrayInputStream(bytes);
    }

    @Override
    public SeekableBlob openSeekable() {
      return new SeekableBlob() {
        private int pos;

        @Override
        public long length() {
          return bytes.length;
        }

        @Override
        public long getPos() {
          return pos;
        }

        @Override
        public void seek(long target) throws IOException {
          if (target < 0 || target > bytes.length) {
            throw new EOFException("seek to " + target + " outside " + bytes.length + " bytes");
          }
          pos = (int) target;
        }

        @Override
        public int read(byte[] buffer, int offset, int len) {
          if (pos >= bytes.length) {
            return -1;
          }
          int n = Math.min(len, bytes.length - pos);
          System.arraycopy(bytes, pos, buffer, offset, n);
          pos += n;
          return n;
        }

        @Override
        public void close() {
          // the bytes are in memory: there is no resource to release
        }
      };
    }
  }
}
