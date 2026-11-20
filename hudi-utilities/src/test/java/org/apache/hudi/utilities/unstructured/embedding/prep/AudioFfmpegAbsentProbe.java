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
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.StubSegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.FrameExtractionStrategy;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
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
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.common.schema.HoodieSchema.Blob.INLINE;

/**
 * Runs, inside a class loader that has no {@code org.bytedeco} classes (no FFmpeg jars, as with the shipped bundles), what a job
 * with audio on does with the DEFAULT FFmpeg audio segment source: the driver's config check, the speech of one video, and a prep
 * round through {@link PreprocessingIterator} holding a PDF then a video. The frames come from {@link NoFramesVideoPreprocessor}
 * (a strategy that needs no FFmpeg and keeps no frame), so only the audio path meets FFmpeg's absence. Loaded and called by
 * {@link TestAudioFfmpegAbsent} through that loader; it reports what it saw as one line per fact.
 */
public class AudioFfmpegAbsentProbe implements Callable<String> {

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

  /** Audio on with the default (FFmpeg) segment source and the stub transcriber (the default whisper one needs a model). */
  static TypedProperties audioOn() {
    TypedProperties props = new TypedProperties();
    props.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    props.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), StubSegmentTranscriber.class.getName());
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "mp4.class", NoFramesVideoPreprocessor.class.getName());
    return props;
  }

  @Override
  public String call() throws Exception {
    StringBuilder report = new StringBuilder();
    Tokenizer tokenizer = new WordCountTokenizer();
    TypedProperties props = audioOn();
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    registry.validateConfig(props, tokenizer);
    report.append("audio validateConfig ok\n");
    report.append("validateConfig loaded bytedeco classes ").append(bytedecoClassesAsked()).append('\n');
    try {
      Class.forName("org.bytedeco.javacv.FFmpegFrameGrabber", false, getClass().getClassLoader());
      throw new IllegalStateException("the probe's class loader can load org.bytedeco: the FFmpeg jars are not hidden");
    } catch (ClassNotFoundException expected) {
      report.append("bytedeco hidden\n");
    }

    FilePreprocessor preprocessor = registry.forExtension("mp4").get();
    try (ClosableIterator<Chunk> chunks = preprocessor.open(new InMemorySeekableBytes(NOT_A_VIDEO),
        new FileMeta("v1", "mp4", NOT_A_VIDEO.length), tokenizer, props)) {
      throw new IllegalStateException("speech prepared without FFmpeg: " + chunks.hasNext());
    } catch (PreprocessorConfigurationException e) {
      report.append("speech refused ").append(e.getMessage().replace('\n', ' ')).append('\n');
      report.append("speech cause ").append(e.getCause()).append('\n');
    } finally {
      registry.close();
    }
    report.append(round());
    return report.toString();
  }

  /** The bytedeco classes this probe's class loader was asked to find so far. */
  private List<String> bytedecoClassesAsked() {
    List<?> asked = (List<?>) ((Supplier<?>) getClass().getClassLoader()).get();
    return asked.stream().map(String::valueOf).filter(name -> name.startsWith("org.bytedeco.")).distinct()
        .collect(Collectors.toList());
  }

  /** A round holding a PDF then a video fails with the configuration error after the PDF's rows, with no quarantine entry. */
  private static String round() throws IOException {
    byte[] pdf = PdfFixtures.text("alpha beta gamma delta");
    TypedProperties props = audioOn();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), "/tmp/audio-ffmpeg-absent-probe-unused");
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    PrepCounters counters = new PrepCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator(), new LongAccumulator());
    PreprocessingIterator iterator = new PreprocessingIterator(
        Arrays.asList(sourceRow("sha-pdf", "pdf", pdf), sourceRow("sha-video", "mp4", NOT_A_VIDEO)).iterator(),
        EmbeddingTableConfig.from(props), "001", new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())),
        new PreprocessorRegistry(props), new WordCountTokenizer(), counters, null);
    List<Row> rows = new ArrayList<>();
    StringBuilder report = new StringBuilder();
    try {
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }
      throw new IllegalStateException("the video round passed without FFmpeg: " + rows.size() + " rows");
    } catch (PreprocessorConfigurationException e) {
      report.append("round failed ").append(e.getMessage().replace('\n', ' ')).append('\n');
    }
    report.append("round rows ").append(rows.size()).append(" side outputs ").append(iterator.getSideOutputs().size())
        .append('\n');
    return report.toString();
  }

  private static Row sourceRow(String sha, String extension, byte[] bytes) {
    Row blob = new GenericRowWithSchema(new Object[] {INLINE, bytes, null}, BLOB);
    return new GenericRowWithSchema(new Object[] {sha, extension, (long) bytes.length, 1L, blob, "s3://bucket/" + sha + "." + extension, sha + "." + extension}, SOURCE_ROW);
  }

  /** The frame preprocessor with a strategy that keeps no frame and needs no FFmpeg: only its speech meets FFmpeg. */
  public static class NoFramesVideoPreprocessor extends VideoFramePreprocessor {
    public NoFramesVideoPreprocessor() {
      super(new FrameExtractionStrategy() {
        @Override
        public String name() {
          return "no-frames-v0";
        }

        @Override
        public ClosableIterator<ExtractedFrame> frames(BlobBytesSupplier video, FileMeta file, TypedProperties props) {
          return new ClosableIterator<ExtractedFrame>() {
            @Override
            public boolean hasNext() {
              return false;
            }

            @Override
            public ExtractedFrame next() {
              throw new NoSuchElementException();
            }

            @Override
            public void close() {
            }
          };
        }
      });
    }
  }

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
        }
      };
    }
  }
}
