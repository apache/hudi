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
import org.jcodec.api.transcode.PixelStore;
import org.jcodec.api.transcode.SinkImpl;
import org.jcodec.api.transcode.VideoFrameWithPacket;
import org.jcodec.codecs.h264.H264Encoder;
import org.jcodec.common.Codec;
import org.jcodec.common.Format;
import org.jcodec.common.VideoEncoder;
import org.jcodec.common.model.ColorSpace;
import org.jcodec.common.model.Packet;
import org.jcodec.common.model.Picture;
import org.jcodec.scale.AWTUtil;
import org.jcodec.scale.ColorUtil;
import org.jcodec.scale.Transform;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

/**
 * Videos of the video memory cells, generated at test time from seeded pictures (the same arguments write the same pictures).
 * Each second of a video is one still picture of its own, so a keyframe at the start of every second is a frame no
 * neighbouring second repeats. Two encoders:
 * <ul>
 *   <li>{@link #ffmpegMp4}: FFmpeg's built-in MPEG-4 Part 2 encoder through the JavaCV recorder (the LGPL FFmpeg build has no
 *   H.264 encoder), in an mp4, a keyframe exactly every second (scene-cut keyframes off, fixed quantiser);</li>
 *   <li>{@link #jcodecMp4}: JCodec's pure-Java H.264 encoder (8-bit 4:2:0, progressive) in a non-fragmented mp4, the one video
 *   type the JCodec frame source decodes, a keyframe (IDR) exactly every second.</li>
 * </ul>
 * Neither needs an {@code ffmpeg} program: FFmpeg's libraries come from the JavaCV jars.
 */
public final class VideoMemoryCellFixtures {

  /** The picture of each second. */
  public enum Scene {
    /**
     * A seeded 16x12 mosaic of flat tiles: a frame JPEG of about 16 KB at 320x240 (inline) and about 100 KB at 1080p (above the
     * 80 KiB inline cap of Spark 3.5's Parquet, so stored out of line).
     */
    MOSAIC,
    /**
     * A zone plate whose ring frequency steps every second (in a cycle of four), over a cosine grating whose direction turns every
     * second, with two chroma gratings: fine detail a q90 JPEG keeps at great cost, so a 720p frame JPEG is well above 80 KiB
     * (the picture of the {@code large-frames-720p.mp4} test fixture).
     */
    ZONE_PLATE
  }

  private VideoMemoryCellFixtures() {
  }

  /**
   * Writes an mp4 of {@code seconds} seconds at {@code fps} frames per second through FFmpeg's MPEG-4 Part 2 encoder: a keyframe
   * at the start of every second.
   */
  public static Path ffmpegMp4(Path out, int seconds, int fps, int width, int height, Scene scene) throws IOException {
    avutil.av_log_set_level(avutil.AV_LOG_ERROR);
    Files.createDirectories(out.getParent());
    try (FFmpegFrameRecorder recorder = new FFmpegFrameRecorder(out.toFile(), width, height, 0);
         Java2DFrameConverter converter = new Java2DFrameConverter()) {
      recorder.setFormat("mp4");
      recorder.setVideoCodec(avcodec.AV_CODEC_ID_MPEG4);
      recorder.setFrameRate(fps);
      recorder.setGopSize(fps);
      recorder.setVideoQuality(4);
      recorder.setVideoOption("sc_threshold", "1000000000");
      recorder.start();
      BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR);
      for (int second = 0; second < seconds; second++) {
        draw(image, scene, second);
        for (int frame = 0; frame < fps; frame++) {
          recorder.record(converter.convert(image));
        }
      }
      recorder.stop();
    }
    return out;
  }

  /**
   * Writes an mp4 of {@code seconds} seconds at {@code fps} frames per second through JCodec's H.264 encoder: an IDR frame at the
   * start of every second.
   */
  public static Path jcodecMp4(Path out, int seconds, int fps, int width, int height, Scene scene) throws IOException {
    Files.createDirectories(out.getParent());
    KeyframeEverySecondSink sink = new KeyframeEverySecondSink(out, fps);
    sink.init();
    try {
      BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR);
      Transform toEncoder = ColorUtil.getTransform(ColorSpace.RGB, sink.getInputColor());
      long frameNo = 0;
      for (int second = 0; second < seconds; second++) {
        draw(image, scene, second);
        Picture yuv = Picture.create(width, height, sink.getInputColor());
        toEncoder.transform(AWTUtil.fromBufferedImageRGB(image), yuv);
        for (int frame = 0; frame < fps; frame++) {
          Packet packet = Packet.createPacket(null, frameNo, fps, 1, frameNo, Packet.FrameType.KEY, null);
          sink.outputVideoFrame(new VideoFrameWithPacket(packet, new PixelStore.LoanerPicture(yuv, 0)));
          frameNo++;
        }
      }
    } finally {
      sink.finish();
    }
    return out;
  }

  /** JCodec's mp4 sink whose H.264 encoder starts a key (IDR) frame every {@code keyInterval} frames (JCodec's default: 25). */
  private static final class KeyframeEverySecondSink extends SinkImpl {
    private final H264Encoder encoder = H264Encoder.createH264Encoder();

    KeyframeEverySecondSink(Path out, int keyInterval) {
      super(out.toString(), Format.MOV, Codec.H264, null);
      encoder.setKeyInterval(keyInterval);
    }

    @Override
    protected VideoEncoder.EncodedFrame encodeVideo(Picture frame, ByteBuffer buffer) {
      return encoder.encodeFrame(frame, buffer);
    }
  }

  /** Draws second {@code second}'s picture into {@code image} (TYPE_3BYTE_BGR). */
  static void draw(BufferedImage image, Scene scene, int second) {
    if (scene == Scene.MOSAIC) {
      drawMosaic(image, second);
    } else {
      drawZonePlate(image, second);
    }
  }

  private static void drawMosaic(BufferedImage image, int second) {
    int width = image.getWidth();
    int height = image.getHeight();
    Graphics2D g = image.createGraphics();
    try {
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
    } finally {
      g.dispose();
    }
  }

  private static void drawZonePlate(BufferedImage image, int second) {
    int width = image.getWidth();
    int height = image.getHeight();
    byte[] bgr = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
    double frequency = 0.0002 + 0.00005 * (second % 4);
    double direction = 1.1 * second;
    double cx = width / 2.0;
    double cy = height / 2.0;
    int chroma = 1 + second % 3;
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        double zone = Math.sin(((x - cx) * (x - cx) + (y - cy) * (y - cy)) * frequency);
        double grating = Math.cos(2 * Math.PI * ((x / cx) * Math.cos(direction) + (y / cy) * Math.sin(direction)));
        double luma = 128 + 80 * zone + 45 * grating;
        double cb = 60 * Math.sin(x * 0.05 + y * 0.031 * chroma);
        double cr = 60 * Math.cos(y * 0.043 - x * 0.02 * chroma);
        int i = (y * width + x) * 3;
        bgr[i] = clamp(luma + 1.772 * cb);
        bgr[i + 1] = clamp(luma - 0.344 * cb - 0.714 * cr);
        bgr[i + 2] = clamp(luma + 1.402 * cr);
      }
    }
  }

  private static byte clamp(double value) {
    return (byte) Math.max(0, Math.min(255, (int) Math.round(value)));
  }
}
