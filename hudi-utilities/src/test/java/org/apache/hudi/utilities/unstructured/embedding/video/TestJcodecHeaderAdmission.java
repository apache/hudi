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
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Short count-bearing headers are admitted before a decoder can allocate from them. */
class TestJcodecHeaderAdmission {
  private static final int LARGE_COUNT = 1 << 25;

  @TempDir
  Path dir;

  @Test
  void decodesAFreshSmallVideoWithinTheSameChildHeap() throws Exception {
    runChild(freshVideo(), "valid");
  }

  @ParameterizedTest
  @CsvSource({"sps-cycle,num_ref_frames_in_pic_order_cnt_cycle", "sps-hrd,cpb_cnt_minus1",
      "sps-id,seq_parameter_set_id", "pps-id,pic_parameter_set_id", "pps-groups,num_slice_groups_minus1",
      "pps-map,pic_size_in_map_units_minus1"})
  void refusesOversizedHeaderFieldsBeforeDecoderAllocation(String variant, String field) throws Exception {
    Path video = freshVideo();
    byte[] changed = rewriteBoxes(Files.readAllBytes(video), variant);
    Files.write(video, changed);
    runChild(video, field);
  }

  /** A copy of c01 (H.264 160x120, six keyframes, its media data before its index). */
  private Path freshVideo() throws Exception {
    Path video = dir.resolve("header-admission.mp4");
    Files.copy(Paths.get(TestJcodecHeaderAdmission.class.getResource("/video/codec-matrix/c01-h264-aac.mp4").toURI()), video,
        StandardCopyOption.REPLACE_EXISTING);
    return video;
  }

  private void runChild(Path video, String expected) throws Exception {
    Path log = dir.resolve(expected + ".log");
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Process child = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
        "-Xms64m", "-Xmx64m", "-XX:+UseSerialGC", "-XX:ActiveProcessorCount=1", "-Djava.awt.headless=true",
        "-cp", classpath, Probe.class.getName(), video.toString(), expected)
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    boolean finished = child.waitFor(30, TimeUnit.SECONDS);
    if (!finished) {
      child.destroyForcibly();
      child.waitFor();
    }
    String output = Files.readString(log);
    assertTrue(finished, "header admission did not finish: " + output);
    assertEquals(0, child.exitValue(), output);
    assertTrue(output.contains("MAX_HEAP="), output);
    assertTrue(output.contains("ADMISSION_OK"), output);
  }

  /** The probe catches allocation failure only to report it to the parent as a failed bounded admission. */
  public static class Probe {
    public static void main(String[] args) throws Exception {
      long heap = Runtime.getRuntime().maxMemory();
      System.out.println("MAX_HEAP=" + heap);
      if (heap > 64L * 1024 * 1024) {
        throw new AssertionError("the child heap cap was not applied");
      }
      Path video = Paths.get(args[0]);
      String expected = args[1];
      try (ClosableIterator<ExtractedFrame> frames = new JcodecKeyframeVideoFrameSource().frames(VideoClips.supplier(video),
          new FileMeta("header-admission", "mp4", Files.size(video)), new TypedProperties())) {
        int count = 0;
        while (frames.hasNext()) {
          frames.next();
          count++;
        }
        if (!"valid".equals(expected) || count == 0) {
          throw new AssertionError("unexpected decoded frame count " + count + " for " + expected);
        }
      } catch (VideoDecodeException refusal) {
        if ("valid".equals(expected) || !refusal.getMessage().contains(expected)) {
          throw new AssertionError("the refusal must identify the bounded header field " + expected, refusal);
        }
      } catch (OutOfMemoryError failure) {
        failure.printStackTrace();
        System.exit(3);
      }
      System.out.println("ADMISSION_OK");
    }
  }

  /** Rebuilds only the avcC's ancestors; c01's media data precedes moov, so its sample offsets stay unchanged. */
  private static byte[] rewriteBoxes(byte[] data, String variant) {
    ByteBuffer in = ByteBuffer.wrap(data);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    while (in.hasRemaining()) {
      int start = in.position();
      long size = in.getInt() & 0xffffffffL;
      byte[] typeBytes = new byte[4];
      in.get(typeBytes);
      String type = new String(typeBytes, StandardCharsets.US_ASCII);
      int header = 8;
      if (size == 1) {
        size = in.getLong();                           // a 64-bit size follows the type
        header = 16;
      } else if (size == 0) {
        size = data.length - start;                    // the box runs to the end of its parent
      }
      byte[] body = new byte[(int) size - header];
      in.get(body);
      if ("avcC".equals(type)) {
        body = replaceParameterSet(body, variant);
      } else {
        int prefix = 0;
        boolean container = type.equals("moov") || type.equals("trak") || type.equals("mdia")
            || type.equals("minf") || type.equals("stbl");
        if ("stsd".equals(type)) {
          container = true;
          prefix = 8;
        } else if ("avc1".equals(type)) {
          container = true;
          prefix = 78;
        }
        if (container) {
          byte[] children = java.util.Arrays.copyOfRange(body, prefix, body.length);
          byte[] changed = rewriteBoxes(children, variant);
          byte[] rebuilt = new byte[prefix + changed.length];
          System.arraycopy(body, 0, rebuilt, 0, prefix);
          System.arraycopy(changed, 0, rebuilt, prefix, changed.length);
          body = rebuilt;
        }
      }
      byte[] boxHeader = ByteBuffer.allocate(8).putInt(body.length + 8).put(typeBytes).array();
      out.write(boxHeader, 0, boxHeader.length);
      out.write(body, 0, body.length);
      assertTrue(in.position() > start, "each generated box advances the cursor");
    }
    return out.toByteArray();
  }

  private static byte[] replaceParameterSet(byte[] avcC, String variant) {
    ByteBuffer in = ByteBuffer.wrap(avcC);
    byte[] config = new byte[6];
    in.get(config);
    assertEquals(1, config[5] & 31, "the generated video has one SPS");
    byte[] sps = new byte[in.getShort() & 0xffff];
    in.get(sps);
    assertEquals(1, in.get() & 0xff, "the generated video has one PPS");
    byte[] pps = new byte[in.getShort() & 0xffff];
    in.get(pps);
    if (variant.startsWith("sps-")) {
      sps = sequenceHeader(variant);
    } else {
      pps = pictureHeader(variant);
    }
    return ByteBuffer.allocate(6 + 2 + sps.length + 1 + 2 + pps.length).put(config)
        .putShort((short) sps.length).put(sps).put((byte) 1).putShort((short) pps.length).put(pps).array();
  }

  private static byte[] sequenceHeader(String variant) {
    Bits bits = new Bits();
    bits.fixed(66, 8);
    bits.fixed(0, 8);
    bits.fixed(10, 8);
    bits.ue("sps-id".equals(variant) ? LARGE_COUNT : 0);
    bits.ue(0);
    bits.ue("sps-cycle".equals(variant) ? 1 : 0);
    if ("sps-cycle".equals(variant)) {
      bits.fixed(1, 1);
      bits.ue(0);
      bits.ue(0);
      bits.ue(LARGE_COUNT);
    } else {
      bits.ue(0);
    }
    bits.ue(1);
    bits.fixed(0, 1);
    bits.ue(3);
    bits.ue(3);
    bits.fixed(1, 1);
    bits.fixed(1, 1);
    bits.fixed(0, 1);
    bits.fixed("sps-hrd".equals(variant) ? 1 : 0, 1);
    if ("sps-hrd".equals(variant)) {
      bits.fixed(0, 5);
      bits.fixed(1, 1);
      bits.ue(LARGE_COUNT);
    }
    return bits.nal(0x67);
  }

  private static byte[] pictureHeader(String variant) {
    Bits bits = new Bits();
    bits.ue("pps-id".equals(variant) ? LARGE_COUNT : 0);
    bits.ue(0);
    bits.fixed(0, 2);
    bits.ue("pps-groups".equals(variant) ? LARGE_COUNT : ("pps-map".equals(variant) ? 1 : 0));
    if ("pps-map".equals(variant)) {
      bits.ue(6);
      bits.ue(LARGE_COUNT);
    }
    bits.ue(0);
    bits.ue(0);
    bits.fixed(0, 3);
    bits.ue(0);
    bits.ue(0);
    bits.ue(0);
    bits.fixed(1, 1);
    bits.fixed(0, 2);
    return bits.nal(0x68);
  }

  /** A fresh bounded syntax fixture, including emulation-prevention bytes and RBSP trailing bits. */
  private static class Bits {
    private final StringBuilder bits = new StringBuilder();

    void fixed(int value, int width) {
      for (int bit = width - 1; bit >= 0; bit--) {
        bits.append((value >>> bit) & 1);
      }
    }

    void ue(int value) {
      int coded = value + 1;
      int width = 32 - Integer.numberOfLeadingZeros(coded);
      fixed(0, width - 1);
      fixed(coded, width);
    }

    byte[] nal(int header) {
      bits.append('1');
      while (bits.length() % 8 != 0) {
        bits.append('0');
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(header);
      int zeros = 0;
      for (int offset = 0; offset < bits.length(); offset += 8) {
        int value = Integer.parseInt(bits.substring(offset, offset + 8), 2);
        if (zeros == 2 && value <= 3) {
          out.write(3);
          zeros = 0;
        }
        out.write(value);
        zeros = value == 0 ? zeros + 1 : 0;
      }
      return out.toByteArray();
    }
  }
}
