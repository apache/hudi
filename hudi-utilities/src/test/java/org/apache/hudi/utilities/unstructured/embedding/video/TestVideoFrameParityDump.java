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

package org.apache.hudi.utilities.unstructured.embedding.video;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Frame-level output and read counts of the FFmpeg frame source over a corpus, written to files so two builds can be compared
 * byte for byte: per clip and dedup method, every kept frame (index, timestamp, size, dedup key, SHA-256 of its JPEG) and how
 * the clip ended (its exception class and message, or ok); per clip the blob opens, reads, bytes read and seeks of one pass,
 * and the median wall time of several passes. Runs only when {@code -Dvideo.parity.out=<dir>} is set; {@code
 * -Dvideo.parity.corpus=<dir>} adds every file of that folder to the generated clips.
 */
class TestVideoFrameParityDump {

  private static final int TIMED_PASSES = 5;

  @Test
  void dumpFramesAndReadCounts() throws IOException {
    String out = System.getProperty("video.parity.out");
    Assumptions.assumeTrue(out != null && !out.isEmpty(), "set -Dvideo.parity.out to dump");
    Path outDir = Files.createDirectories(Paths.get(out));
    Path work = Files.createTempDirectory("video-parity");
    List<Path> clips = new ArrayList<>(generated(work));
    String corpus = System.getProperty("video.parity.corpus");
    if (corpus != null && !corpus.isEmpty()) {
      try (Stream<Path> files = Files.list(Paths.get(corpus))) {
        List<Path> real = files.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
        clips.addAll(real);
        clips.addAll(truncatedCopies(work, real));
      }
    }
    List<String> frames = new ArrayList<>();
    List<String> reads = new ArrayList<>();
    reads.add("clip\topens\treads\tbytesRead\tseeks\tmedianMs");
    for (Path clip : clips) {
      for (String method : Arrays.asList("dhash", "sha256")) {
        TypedProperties props = new TypedProperties();
        props.setProperty(EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key(), method);
        frames.addAll(dump(clip, method, props, new Counts(), 0));
      }
      // the storage-failure case: reads past half the file fail as storage does
      frames.addAll(dump(clip, "dhash-storage-failure-at-half", new TypedProperties(), new Counts(), Files.size(clip) / 2));
      Counts counts = new Counts();
      dump(clip, "count", new TypedProperties(), counts, 0);
      long[] millis = new long[TIMED_PASSES];
      for (int i = 0; i < TIMED_PASSES; i++) {
        long start = System.nanoTime();
        dump(clip, "time", new TypedProperties(), new Counts(), 0);
        millis[i] = (System.nanoTime() - start) / 1_000_000L;
      }
      Arrays.sort(millis);
      reads.add(clip.getFileName() + "\t" + counts.opens + "\t" + counts.reads + "\t" + counts.bytes + "\t" + counts.seeks + "\t"
          + millis[TIMED_PASSES / 2]);
    }
    Files.write(outDir.resolve("frames.tsv"), frames, StandardCharsets.UTF_8);
    Files.write(outDir.resolve("reads.tsv"), reads, StandardCharsets.UTF_8);
    assertTrue(Files.size(outDir.resolve("frames.tsv")) > 0, "frames dumped to " + outDir);
  }

  /** Clips generated here: the main test clip, a wide one, and frame widths whose padded BGR rows read as 4 or 6 channels. */
  private static List<Path> generated(Path work) throws IOException {
    List<Path> clips = new ArrayList<>();
    clips.add(VideoClips.generate(work.resolve("gen-320x240-still.mp4"), 32, 10, 2, 320, 240, 10, 20));
    clips.add(VideoClips.generate(work.resolve("gen-1280x320.mp4"), 4, 10, 2, 1280, 320, -1, -1));
    clips.add(VideoClips.generate(work.resolve("gen-136x96.mp4"), 6, 10, 2, 136, 96, -1, -1));
    clips.add(VideoClips.generate(work.resolve("gen-80x64.mp4"), 6, 10, 2, 80, 64, -1, -1));
    clips.add(VideoClips.generate(work.resolve("gen-32x24.mp4"), 6, 10, 2, 32, 24, -1, -1));
    clips.add(VideoClips.generate(work.resolve("gen-192x108.mp4"), 6, 10, 2, 192, 108, -1, -1));
    return clips;
  }

  /** Each real clip cut at 70% of its bytes: a truncated file, refused at open or after the frames before the cut. */
  private static List<Path> truncatedCopies(Path work, List<Path> real) throws IOException {
    List<Path> cut = new ArrayList<>();
    for (Path file : real) {
      byte[] bytes = Files.readAllBytes(file);
      Path copy = work.resolve("cut70-" + file.getFileName());
      Files.write(copy, Arrays.copyOf(bytes, (int) (bytes.length * 0.7)));
      cut.add(copy);
    }
    return cut;
  }

  private static List<String> dump(Path clip, String label, TypedProperties props, Counts counts, long failAt) throws IOException {
    List<String> lines = new ArrayList<>();
    String name = clip.getFileName().toString();
    String extension = name.substring(name.lastIndexOf('.') + 1).toLowerCase(Locale.ROOT);
    FileMeta meta = new FileMeta("fid-" + name, extension, Files.size(clip));
    SeekableBlobBytesSupplier supplier = counting(clip, counts, failAt <= 0 ? Long.MAX_VALUE : failAt);
    String outcome = "ok";
    try (ClosableIterator<ExtractedFrame> iterator = new FfmpegKeyframeVideoFrameSource().frames(supplier, meta, props)) {
      while (iterator.hasNext()) {
        ExtractedFrame frame = iterator.next();
        byte[] jpeg;
        try (InputStream in = frame.getImage().open()) {
          jpeg = readAll(in);
        }
        lines.add(name + "\t" + label + "\t" + frame.getFrameIndex() + "\t" + frame.getTimeMs() + "\t" + frame.getWidth() + "x"
            + frame.getHeight() + "\t" + frame.getDedupKey() + "\t" + FrameDedup.sha256Hex(jpeg) + "\t" + jpeg.length);
      }
    } catch (RuntimeException | IOException | AssertionError e) {
      // an AssertionError: JavaCV's own assertion on a frame it cannot convert (tests run with -ea)
      outcome = describe(e);
    }
    lines.add(name + "\t" + label + "\tEND\t" + outcome);
    return lines;
  }

  private static String describe(Throwable e) {
    StringBuilder text = new StringBuilder(e.getClass().getSimpleName() + ": " + e.getMessage());
    for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
      text.append(" <- ").append(cause.getClass().getSimpleName());
    }
    return text.toString().replace('\n', ' ').replace('\t', ' ');
  }

  private static byte[] readAll(InputStream in) throws IOException {
    java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    for (int n = in.read(buffer); n >= 0; n = in.read(buffer)) {
      bytes.write(buffer, 0, n);
    }
    return bytes.toByteArray();
  }

  /** Blob opens, reads, bytes read and seeks to a position other than the current one. */
  private static final class Counts {
    long opens;
    long reads;
    long bytes;
    long seeks;
  }

  private static SeekableBlobBytesSupplier counting(Path file, Counts counts, long failAt) {
    return new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        counts.opens++;
        VideoClips.FileBlob blob = new VideoClips.FileBlob(file, failAt);
        return new SeekableBlob() {
          @Override
          public long length() {
            return blob.length();
          }

          @Override
          public long getPos() throws IOException {
            return blob.getPos();
          }

          @Override
          public void seek(long pos) throws IOException {
            if (pos != blob.getPos()) {
              counts.seeks++;
            }
            blob.seek(pos);
          }

          @Override
          public int read(byte[] buffer, int offset, int len) throws IOException {
            counts.reads++;
            int n = blob.read(buffer, offset, len);
            counts.bytes += Math.max(0, n);
            return n;
          }

          @Override
          public void close() throws IOException {
            blob.close();
          }
        };
      }

      @Override
      public InputStream open() throws IOException {
        return Files.newInputStream(file);
      }
    };
  }
}
