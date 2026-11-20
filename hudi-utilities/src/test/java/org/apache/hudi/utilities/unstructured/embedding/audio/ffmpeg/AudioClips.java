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

package org.apache.hudi.utilities.unstructured.embedding.audio.ffmpeg;

import org.apache.hudi.utilities.unstructured.embedding.audio.AudioInput;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;

import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.ffmpeg.global.avutil;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Java2DFrameConverter;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.ShortBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Generated test videos with an audio track: MPEG-4 Part 2 video (FFmpeg's own encoder, 64x48, 2 fps) in an mp4 container, and an
 * audio track of tone and digital silence laid out in milliseconds, so the speech detector's cuts are predictable. The tone is a
 * 440 Hz sine at a quarter of full scale (about -15 dBFS RMS); AAC uses FFmpeg's encoder, MP3 libmp3lame (both in the LGPL build).
 */
public final class AudioClips {

  /** Audio codec of a generated clip. */
  public enum Codec {
    AAC(avcodec.AV_CODEC_ID_AAC),
    MP3(avcodec.AV_CODEC_ID_MP3),
    /** Uncompressed 16-bit PCM (Matroska), at any sample rate its header can declare. */
    PCM_S16LE(avcodec.AV_CODEC_ID_PCM_S16LE);

    private final int id;

    Codec(int id) {
      this.id = id;
    }
  }

  private static final int FPS = 2;
  private static final double AMPLITUDE = 0.25;
  private static final double TONE_HZ = 440;

  private AudioClips() {
  }

  /**
   * @param layout alternating tone and silence lengths in ms, starting with tone (a leading silence is a tone of 0 ms)
   */
  public static Path generate(Path out, Codec codec, int sampleRate, int channels, long... layout) throws IOException {
    return generate(out, "mp4", codec, sampleRate, channels, layout);
  }

  /**
   * @param format FFmpeg muxer name, e.g. {@code mp4}, {@code matroska} (1 ms time base) or {@code mpegts} (90 kHz)
   */
  public static Path generate(Path out, String format, Codec codec, int sampleRate, int channels, long... layout)
      throws IOException {
    avutil.av_log_set_level(avutil.AV_LOG_ERROR);
    Files.createDirectories(out.getParent());
    long totalMs = 0;
    for (long ms : layout) {
      totalMs += ms;
    }
    long totalSamples = totalMs * sampleRate / 1000;
    try (FFmpegFrameRecorder recorder = new FFmpegFrameRecorder(out.toFile(), 64, 48, channels);
         Java2DFrameConverter converter = new Java2DFrameConverter()) {
      recorder.setFormat(format);
      recorder.setVideoCodec(avcodec.AV_CODEC_ID_MPEG4);
      recorder.setFrameRate(FPS);
      recorder.setGopSize(FPS);
      recorder.setAudioCodec(codec.id);
      recorder.setSampleRate(sampleRate);
      recorder.setAudioChannels(channels);
      recorder.setAudioBitrate(64000);
      recorder.start();
      BufferedImage image = new BufferedImage(64, 48, BufferedImage.TYPE_3BYTE_BGR);
      int chunk = sampleRate / FPS;
      short[] buffer = new short[chunk * channels];
      for (long first = 0; first < totalSamples; first += chunk) {
        recorder.record(converter.convert(image));
        int n = (int) Math.min(chunk, totalSamples - first);
        for (int i = 0; i < n; i++) {
          long sample = first + i;
          short value = isTone(sample * 1000 / sampleRate, layout)
              ? (short) Math.round(AMPLITUDE * 32767 * Math.sin(2 * Math.PI * TONE_HZ * sample / sampleRate)) : 0;
          for (int c = 0; c < channels; c++) {
            buffer[i * channels + c] = value;
          }
        }
        recorder.recordSamples(sampleRate, channels, ShortBuffer.wrap(buffer, 0, n * channels));
      }
      recorder.stop();
    }
    return out;
  }

  private static boolean isTone(long ms, long[] layout) {
    long end = 0;
    for (int i = 0; i < layout.length; i++) {
      end += layout[i];
      if (ms < end) {
        return i % 2 == 0;
      }
    }
    return false;
  }

  /** An {@link AudioInput} over a video's bytes. */
  public static AudioInput input(String fid, Path file, BlobBytesSupplier bytes) {
    return new AudioInput() {
      @Override
      public String sourcePathSha() {
        return fid;
      }

      @Override
      public String sourcePath() {
        return file.toString();
      }

      @Override
      public String sourcePartitionPath() {
        return null;
      }

      @Override
      public long sizeBytes() {
        return file.toFile().length();
      }

      @Override
      public String extension() {
        return "mp4";
      }

      @Override
      public BlobBytesSupplier bytes() {
        return bytes;
      }
    };
  }
}
