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

import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;

import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.ffmpeg.global.avutil;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Java2DFrameConverter;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

/**
 * Generated test videos (FFmpeg's built-in MPEG-4 Part 2 encoder in an mp4 container; the LGPL build has no H.264 encoder) and
 * blob views over a file. A clip has a keyframe exactly every {@code gopSeconds} (scene-cut keyframes off, fixed quantiser), a
 * frame content that changes every second, and optionally a STATIC segment whose frames are all one picture.
 */
public final class VideoClips {

  private VideoClips() {
  }

  /**
   * @param staticFrom first second of the static segment (inclusive), or -1 for none
   * @param staticTo   end second of the static segment (exclusive)
   */
  public static Path generate(Path out, int seconds, int fps, int gopSeconds, int width, int height, int staticFrom,
                              int staticTo) throws IOException {
    return generate(out, seconds, fps, gopSeconds, width, height, staticFrom, staticTo, 0, -1);
  }

  /**
   * As {@link #generate(Path, int, int, int, int, int, int, int)}, the first frame presented at {@code startSeconds} (an empty
   * edit before it) on a video track whose time base is 1/{@code timescale} ({@code -1}: the muxer's default).
   */
  public static Path generate(Path out, int seconds, int fps, int gopSeconds, int width, int height, int staticFrom,
                              int staticTo, long startSeconds, long timescale) throws IOException {
    avutil.av_log_set_level(avutil.AV_LOG_ERROR);
    Files.createDirectories(out.getParent());
    try (FFmpegFrameRecorder recorder = new FFmpegFrameRecorder(out.toFile(), width, height, 0);
         Java2DFrameConverter converter = new Java2DFrameConverter()) {
      recorder.setFormat("mp4");
      recorder.setVideoCodec(avcodec.AV_CODEC_ID_MPEG4);
      recorder.setFrameRate(fps);
      recorder.setGopSize(gopSeconds * fps);
      recorder.setVideoQuality(4);
      recorder.setVideoOption("sc_threshold", "1000000000");
      if (timescale > 0) {
        recorder.setOption("video_track_timescale", String.valueOf(timescale));
      }
      recorder.start();
      if (startSeconds > 0) {
        recorder.setTimestamp(startSeconds * 1_000_000L);
      }
      BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR);
      for (int i = 0; i < seconds * fps; i++) {
        int second = i / fps;
        draw(image, second, i % fps, staticFrom >= 0 && second >= staticFrom && second < staticTo);
        recorder.record(converter.convert(image));
      }
      recorder.stop();
    }
    return out;
  }

  private static void draw(BufferedImage image, int second, int frameInSecond, boolean still) {
    int width = image.getWidth();
    int height = image.getHeight();
    Graphics2D g = image.createGraphics();
    try {
      if (still) {
        g.setColor(Color.GRAY);
        g.fillRect(0, 0, width, height);
        g.setColor(Color.WHITE);
        g.fillOval(width / 3, height / 4, width / 3, height / 2);
        return;
      }
      // a mosaic of its own every second (a seeded 16x12 grid of grey levels, so no two seconds share a difference hash), and
      // a thin band that moves every frame
      Random random = new Random(1000L + second);
      int columns = 16;
      int rows = 12;
      for (int row = 0; row < rows; row++) {
        for (int column = 0; column < columns; column++) {
          int level = random.nextInt(256);
          g.setColor(new Color(level, (level * 7) % 256, 255 - level));
          g.fillRect(column * width / columns, row * height / rows, width / columns + 1, height / rows + 1);
        }
      }
      g.setColor(Color.WHITE);
      g.fillRect(0, (frameInSecond * 7) % height, width, 2);
    } finally {
      g.dispose();
    }
  }

  /** The file's bytes as a seekable blob supplier, as data prep hands a video to its frame extraction strategy. */
  public static SeekableBlobBytesSupplier supplier(Path file) {
    return new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        return new FileBlob(file, Long.MAX_VALUE);
      }

      @Override
      public InputStream open() throws IOException {
        return Files.newInputStream(file);
      }
    };
  }

  /** Like {@link #supplier} but every read past {@code failAfter} bytes fails as storage does (a connection reset). */
  public static SeekableBlobBytesSupplier failingSupplier(Path file, long failAfter) {
    return new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        return new FileBlob(file, failAfter);
      }

      @Override
      public InputStream open() throws IOException {
        return Files.newInputStream(file);
      }
    };
  }

  /**
   * Like {@link #supplier} but each opened view fails as storage does once it has served {@code budget} bytes in total, wherever
   * they lie: a connection that drops part way through reading the file.
   */
  public static SeekableBlobBytesSupplier failingAfterBytesRead(Path file, long budget) {
    return new SeekableBlobBytesSupplier() {
      @Override
      public SeekableBlob openSeekable() throws IOException {
        return new FileBlob(file, Long.MAX_VALUE, budget);
      }

      @Override
      public InputStream open() throws IOException {
        return Files.newInputStream(file);
      }
    };
  }

  /**
   * A local file as a {@link SeekableBlob}; reads at or past {@code failAt}, or past {@code budget} bytes served in total, throw a
   * storage IOException.
   */
  public static class FileBlob implements SeekableBlob {
    private final RandomAccessFile file;
    private final long failAt;
    private final long budget;
    private long served;

    public FileBlob(Path path, long failAt) throws IOException {
      this(path, failAt, Long.MAX_VALUE);
    }

    FileBlob(Path path, long failAt, long budget) throws IOException {
      this.file = new RandomAccessFile(path.toFile(), "r");
      this.failAt = failAt;
      this.budget = budget;
    }

    @Override
    public long length() {
      try {
        return file.length();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public long getPos() throws IOException {
      return file.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
      if (pos < 0 || pos > length()) {
        throw new EOFException("seek to " + pos + " outside " + length());
      }
      file.seek(pos);
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
      if (file.getFilePointer() + len > failAt || served + len > budget) {
        throw new IOException("connection reset (test storage failure)");
      }
      int n = file.read(buffer, offset, len);
      served += Math.max(0, n);
      return n;
    }

    @Override
    public void close() throws IOException {
      file.close();
    }
  }
}
