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

package org.apache.hudi.utilities.unstructured.memory;

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
 * Inputs of the speech memory cells, generated at test time with the JavaCV FFmpeg recorder (nothing committed): an mp4 with a
 * 64x48 MPEG-4 Part 2 video at 2 fps (one blank picture, so the frame source keeps one frame after dedup) and a 16 kHz mono AAC
 * track of speech-like bursts: in every period of {@value #PERIOD_MS} ms a sine tone of {@value #TONE_MS} ms at a quarter of full
 * scale (about -15 dBFS RMS, above the energy gate's -35 dBFS), then {@value #SILENCE_MS} ms of digital silence (longer than the
 * gate's 500 ms minimum silence). The FFmpeg audio segment source therefore cuts exactly one segment per burst, each under the
 * 30 s segment bound. A track's tone frequency follows its seed, so the tracks differ. The layout is computed per sample in
 * constant time, so a 2 h track costs only its encoding.
 */
public final class SpeechCellFixtures {

  static final int SAMPLE_RATE = 16000;
  static final long TONE_MS = 29_000;
  static final long SILENCE_MS = 1_000;
  static final long PERIOD_MS = TONE_MS + SILENCE_MS;
  private static final int WIDTH = 64;
  private static final int HEIGHT = 48;
  private static final int FPS = 2;
  private static final double AMPLITUDE = 0.25;

  private SpeechCellFixtures() {
  }

  /** The segments the FFmpeg audio segment source cuts from a track of {@code seconds}: one per burst. */
  public static int bursts(int seconds) {
    return (int) ((seconds * 1000L + PERIOD_MS - 1) / PERIOD_MS);
  }

  /**
   * Writes a track of {@code seconds} (a whole number of periods, so it ends with silence) to {@code out}: the video and the
   * bursts described above, the tone at {@code 300 + 20 * (seed % 20)} Hz.
   *
   * @return {@code out}
   */
  public static Path track(Path out, int seconds, long seed) throws IOException {
    if (seconds * 1000L % PERIOD_MS != 0) {
      throw new IllegalArgumentException("a track is a whole number of " + PERIOD_MS + " ms periods: " + seconds + " s");
    }
    avutil.av_log_set_level(avutil.AV_LOG_ERROR);
    Files.createDirectories(out.getParent());
    double toneHz = 300 + 20 * (seed % 20);
    long totalSamples = seconds * (long) SAMPLE_RATE;
    long toneSamples = TONE_MS * SAMPLE_RATE / 1000;
    long periodSamples = PERIOD_MS * SAMPLE_RATE / 1000;
    try (FFmpegFrameRecorder recorder = new FFmpegFrameRecorder(out.toFile(), WIDTH, HEIGHT, 1);
         Java2DFrameConverter converter = new Java2DFrameConverter()) {
      recorder.setFormat("mp4");
      recorder.setVideoCodec(avcodec.AV_CODEC_ID_MPEG4);
      recorder.setFrameRate(FPS);
      recorder.setGopSize(FPS);
      recorder.setAudioCodec(avcodec.AV_CODEC_ID_AAC);
      recorder.setSampleRate(SAMPLE_RATE);
      recorder.setAudioChannels(1);
      recorder.setAudioBitrate(64000);
      recorder.start();
      BufferedImage image = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_3BYTE_BGR);
      int chunk = SAMPLE_RATE / FPS;
      short[] buffer = new short[chunk];
      for (long first = 0; first < totalSamples; first += chunk) {
        // one picture per half second of audio: the 2 fps video spans the track
        recorder.record(converter.convert(image));
        int n = (int) Math.min(chunk, totalSamples - first);
        for (int i = 0; i < n; i++) {
          long sample = first + i;
          buffer[i] = sample % periodSamples < toneSamples
              ? (short) Math.round(AMPLITUDE * 32767 * Math.sin(2 * Math.PI * toneHz * sample / SAMPLE_RATE)) : 0;
        }
        recorder.recordSamples(SAMPLE_RATE, 1, ShortBuffer.wrap(buffer, 0, n));
      }
      recorder.stop();
    }
    return out;
  }
}
