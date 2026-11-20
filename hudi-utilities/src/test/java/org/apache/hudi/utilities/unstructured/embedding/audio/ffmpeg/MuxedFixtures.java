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

import org.bytedeco.ffmpeg.avcodec.AVPacket;
import org.bytedeco.ffmpeg.avformat.AVFormatContext;
import org.bytedeco.ffmpeg.avformat.AVIOContext;
import org.bytedeco.ffmpeg.avformat.AVStream;
import org.bytedeco.ffmpeg.avutil.AVDictionary;
import org.bytedeco.ffmpeg.avutil.AVRational;
import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.ffmpeg.global.avformat;
import org.bytedeco.ffmpeg.global.avutil;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Java2DFrameConverter;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.ShortBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Multi-stream test containers built in-process through the FFmpeg libraries, without the bundled ffmpeg program (which links
 * libavdevice and so needs host X11 and sound libraries). {@link #encode} stands in for the lavfi {@code color} and {@code sine}
 * sources: black 64x48 MPEG-4 Part 2 video at 2 fps with a GOP of 2 on one encoder thread, and a 440 Hz mono sine at lavfi's
 * default amplitude of 1/8. {@link #remux} is a stream copy: the chosen streams keep their packets, codec parameters and
 * timestamps (plus an optional per-stream offset), and an explicit disposition replaces the copied one.
 */
public final class MuxedFixtures {

  private static final int WIDTH = 64;
  private static final int HEIGHT = 48;
  private static final int FPS = 2;
  private static final double SINE_AMPLITUDE = 1.0 / 8;
  private static final double SINE_HZ = 440;
  private static final AVRational MICROS = avutil.av_make_q(1, 1_000_000);

  private MuxedFixtures() {
  }

  /**
   * Encodes an mp4 with video for {@code videoSeconds} (0 for none) and a mono sine for {@code audioSeconds} (0 for none).
   */
  public static Path encode(Path out, int videoSeconds, int audioSeconds, int audioCodec, int sampleRate, int audioBitrate)
      throws IOException {
    avutil.av_log_set_level(avutil.AV_LOG_ERROR);
    Files.createDirectories(out.getParent());
    int channels = audioSeconds > 0 ? 1 : 0;
    try (FFmpegFrameRecorder recorder = videoSeconds > 0
        ? new FFmpegFrameRecorder(out.toFile(), WIDTH, HEIGHT, channels)
        : new FFmpegFrameRecorder(out.toFile(), channels);
         Java2DFrameConverter converter = new Java2DFrameConverter()) {
      recorder.setFormat("mp4");
      if (videoSeconds > 0) {
        recorder.setVideoCodec(avcodec.AV_CODEC_ID_MPEG4);
        recorder.setFrameRate(FPS);
        recorder.setGopSize(FPS);
        recorder.setVideoOption("threads", "1");
      }
      if (channels > 0) {
        recorder.setAudioCodec(audioCodec);
        recorder.setSampleRate(sampleRate);
        recorder.setAudioChannels(channels);
        recorder.setAudioBitrate(audioBitrate);
      }
      recorder.start();
      BufferedImage black = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_3BYTE_BGR);
      long totalSamples = (long) audioSeconds * sampleRate;
      int chunk = sampleRate / FPS;
      short[] buffer = new short[Math.max(chunk, 1)];
      long steps = Math.max((long) videoSeconds * FPS, channels > 0 ? (totalSamples + chunk - 1) / chunk : 0);
      for (long step = 0; step < steps; step++) {
        if (step < (long) videoSeconds * FPS) {
          recorder.record(converter.convert(black));
        }
        long first = step * chunk;
        if (channels > 0 && first < totalSamples) {
          int n = (int) Math.min(chunk, totalSamples - first);
          for (int i = 0; i < n; i++) {
            buffer[i] = (short) Math.round(SINE_AMPLITUDE * 32767 * Math.sin(2 * Math.PI * SINE_HZ * (first + i) / sampleRate));
          }
          recorder.recordSamples(sampleRate, channels, ShortBuffer.wrap(buffer, 0, n));
        }
      }
      recorder.stop();
    }
    return out;
  }

  /** One stream to copy: the {@code nth} stream of {@code mediaType} in {@code file}. */
  public static final class Pick {
    private final Path file;
    private final int mediaType;
    private final int nth;
    private long offsetMicros;
    private Integer disposition;

    private Pick(Path file, int mediaType, int nth) {
      this.file = file;
      this.mediaType = mediaType;
      this.nth = nth;
    }

    public static Pick video(Path file, int nth) {
      return new Pick(file, avutil.AVMEDIA_TYPE_VIDEO, nth);
    }

    public static Pick audio(Path file, int nth) {
      return new Pick(file, avutil.AVMEDIA_TYPE_AUDIO, nth);
    }

    /** Shifts the stream's timestamps by {@code seconds}, as the ffmpeg program's {@code -itsoffset}. */
    public Pick offsetSeconds(long seconds) {
      this.offsetMicros = seconds * 1_000_000L;
      return this;
    }

    /** Sets the output stream's disposition flags, as the ffmpeg program's {@code -disposition}. */
    public Pick disposition(int flags) {
      this.disposition = flags;
      return this;
    }
  }

  /** Copies the picked streams, in order, into a new container whose muxer is chosen from {@code out}'s extension. */
  public static Path remux(Path out, Pick... picks) throws IOException {
    avutil.av_log_set_level(avutil.AV_LOG_ERROR);
    Files.createDirectories(out.getParent());
    Map<Path, AVFormatContext> inputs = new LinkedHashMap<>();
    AVFormatContext output = new AVFormatContext(null);
    List<AVPacket> packets = new ArrayList<>();
    try {
      for (Pick pick : picks) {
        if (!inputs.containsKey(pick.file)) {
          AVFormatContext input = new AVFormatContext(null);
          check(avformat.avformat_open_input(input, pick.file.toString(), null, null), "open " + pick.file);
          inputs.put(pick.file, input);
          check(avformat.avformat_find_stream_info(input, (AVDictionary) null), "probe " + pick.file);
        }
      }
      check(avformat.avformat_alloc_output_context2(output, null, null, out.toString()), "allocate " + out);
      int[][] targets = new int[picks.length][];
      for (int p = 0; p < picks.length; p++) {
        AVFormatContext input = inputs.get(picks[p].file);
        int source = streamIndex(input, picks[p]);
        AVStream from = input.streams(source);
        AVStream to = avformat.avformat_new_stream(output, null);
        check(avcodec.avcodec_parameters_copy(to.codecpar(), from.codecpar()), "copy parameters");
        to.codecpar().codec_tag(0);
        to.time_base(from.time_base());
        to.disposition(picks[p].disposition != null ? picks[p].disposition : from.disposition());
        targets[p] = new int[] {source, to.index()};
      }
      AVIOContext io = new AVIOContext(null);
      check(avformat.avio_open(io, out.toString(), avformat.AVIO_FLAG_WRITE), "create " + out);
      output.pb(io);
      for (Map.Entry<Path, AVFormatContext> entry : inputs.entrySet()) {
        AVFormatContext input = entry.getValue();
        AVPacket packet = avcodec.av_packet_alloc();
        while (avformat.av_read_frame(input, packet) >= 0) {
          for (int p = 0; p < picks.length; p++) {
            if (picks[p].file.equals(entry.getKey()) && targets[p][0] == packet.stream_index()) {
              AVPacket copy = avcodec.av_packet_clone(packet);
              AVRational base = input.streams(targets[p][0]).time_base();
              long shift = avutil.av_rescale_q(picks[p].offsetMicros, MICROS, base);
              if (copy.pts() != avutil.AV_NOPTS_VALUE) {
                copy.pts(copy.pts() + shift);
              }
              if (copy.dts() != avutil.AV_NOPTS_VALUE) {
                copy.dts(copy.dts() + shift);
              }
              copy.stream_index(targets[p][1]);
              copy.pos(-1);
              packets.add(copy);
            }
          }
          avcodec.av_packet_unref(packet);
        }
        avcodec.av_packet_free(packet);
      }
      check(avformat.avformat_write_header(output, (AVDictionary) null), "write header " + out);
      // The input time bases stay valid until the inputs close, so order the packets before writing them.
      List<long[]> order = new ArrayList<>();
      for (int i = 0; i < packets.size(); i++) {
        AVPacket packet = packets.get(i);
        AVRational base = sourceBase(inputs, picks, targets, packet.stream_index());
        long ts = packet.dts() != avutil.AV_NOPTS_VALUE ? packet.dts() : packet.pts();
        order.add(new long[] {avutil.av_rescale_q(ts, base, MICROS), i});
      }
      order.sort(Comparator.<long[]>comparingLong(o -> o[0]).thenComparingLong(o -> o[1]));
      for (long[] entry : order) {
        AVPacket packet = packets.get((int) entry[1]);
        AVRational base = sourceBase(inputs, picks, targets, packet.stream_index());
        avcodec.av_packet_rescale_ts(packet, base, output.streams(packet.stream_index()).time_base());
        check(avformat.av_interleaved_write_frame(output, packet), "write packet " + out);
      }
      check(avformat.av_write_trailer(output), "write trailer " + out);
    } finally {
      for (AVPacket packet : packets) {
        avcodec.av_packet_free(packet);
      }
      if (output.pb() != null && !output.pb().isNull()) {
        avformat.avio_closep(output.pb());
      }
      if (!output.isNull()) {
        avformat.avformat_free_context(output);
      }
      for (AVFormatContext input : inputs.values()) {
        avformat.avformat_close_input(input);
      }
    }
    return out;
  }

  private static int streamIndex(AVFormatContext input, Pick pick) throws IOException {
    int seen = 0;
    for (int i = 0; i < input.nb_streams(); i++) {
      if (input.streams(i).codecpar().codec_type() == pick.mediaType && seen++ == pick.nth) {
        return i;
      }
    }
    throw new IOException("no stream " + pick.nth + " of media type " + pick.mediaType + " in " + pick.file);
  }

  private static AVRational sourceBase(Map<Path, AVFormatContext> inputs, Pick[] picks, int[][] targets, int outputIndex) {
    for (int p = 0; p < picks.length; p++) {
      if (targets[p][1] == outputIndex) {
        return inputs.get(picks[p].file).streams(targets[p][0]).time_base();
      }
    }
    throw new IllegalStateException("unmapped output stream " + outputIndex);
  }

  private static void check(int result, String what) throws IOException {
    if (result < 0) {
      byte[] message = new byte[256];
      avutil.av_strerror(result, message, message.length);
      int end = 0;
      while (end < message.length && message[end] != 0) {
        end++;
      }
      throw new IOException(what + " failed: " + new String(message, 0, end, StandardCharsets.US_ASCII));
    }
  }
}
