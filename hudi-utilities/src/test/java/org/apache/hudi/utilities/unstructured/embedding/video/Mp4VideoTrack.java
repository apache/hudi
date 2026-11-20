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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A test oracle of MP4 presentation times, independent of FFmpeg: it reads the FIRST video track's boxes ({@code mdhd} timescale,
 * {@code stts} sample durations, {@code ctts} composition offsets, {@code stss} sync samples, {@code edts/elst} edit list — one
 * edit, as an encoder's delay-skipping edit is) and gives each keyframe's presentation time on the edited timeline:
 * {@code dts + ctts - edit media time} (ISO/IEC 14496-12 §8.6.6). Only what the checked-in fixtures use is read.
 */
final class Mp4VideoTrack {

  final long timescale;
  final long editMediaTime;
  private final List<Long> keyframePresentationTicks;

  private Mp4VideoTrack(long timescale, long editMediaTime, List<Long> keyframePresentationTicks) {
    this.timescale = timescale;
    this.editMediaTime = editMediaTime;
    this.keyframePresentationTicks = keyframePresentationTicks;
  }

  static Mp4VideoTrack read(byte[] file) {
    ByteBuffer moov = child(ByteBuffer.wrap(file), "moov");
    ByteBuffer trak = moov.duplicate();
    while (trak.hasRemaining()) {
      ByteBuffer candidate = nextChild(trak, "trak");
      if (candidate == null) {
        break;
      }
      ByteBuffer mdia = child(candidate.duplicate(), "mdia");
      ByteBuffer hdlr = child(mdia.duplicate(), "hdlr");
      byte[] handler = new byte[4];
      hdlr.position(hdlr.position() + 8);
      hdlr.get(handler);
      if (!"vide".equals(new String(handler, StandardCharsets.ISO_8859_1))) {
        continue;
      }
      long mediaTime = editMediaTime(candidate.duplicate());
      long timescale = timescale(child(mdia.duplicate(), "mdhd"));
      ByteBuffer stbl = child(child(mdia.duplicate(), "minf"), "stbl");
      return new Mp4VideoTrack(timescale, mediaTime, keyframeTicks(stbl, mediaTime));
    }
    throw new IllegalArgumentException("no video track");
  }

  /**
   * The presentation times of the keyframes the edit presents (at or after its start), in milliseconds on the edited timeline, in
   * presentation order.
   */
  List<Long> presentedKeyframeMillis() {
    List<Long> millis = new ArrayList<>();
    for (long ticks : keyframePresentationTicks) {
      if (ticks >= 0) {
        millis.add(Math.round(ticks * 1000.0 / timescale));
      }
    }
    return millis;
  }

  private static List<Long> keyframeTicks(ByteBuffer stbl, long mediaTime) {
    long[] durations = expand(child(stbl.duplicate(), "stts"));
    ByteBuffer cttsBox = optionalChild(stbl.duplicate(), "ctts");
    long[] offsets = cttsBox == null ? new long[durations.length] : expand(cttsBox);
    ByteBuffer stss = child(stbl.duplicate(), "stss");
    stss.position(stss.position() + 4);
    int syncCount = stss.getInt();
    long[] dts = new long[durations.length];
    for (int i = 1; i < durations.length; i++) {
      dts[i] = dts[i - 1] + durations[i - 1];
    }
    List<Long> ticks = new ArrayList<>();
    for (int i = 0; i < syncCount; i++) {
      int sample = stss.getInt() - 1;
      ticks.add(dts[sample] + offsets[sample] - mediaTime);
    }
    ticks.sort(Long::compare);
    return ticks;
  }

  /** An stts or ctts box as one value per sample (version 0 counts and values; a ctts value read as signed). */
  private static long[] expand(ByteBuffer box) {
    box.position(box.position() + 4);
    int entries = box.getInt();
    List<long[]> runs = new ArrayList<>();
    int samples = 0;
    for (int i = 0; i < entries; i++) {
      long count = box.getInt() & 0xFFFFFFFFL;
      long value = box.getInt();
      runs.add(new long[] {count, value});
      samples += (int) count;
    }
    long[] values = new long[samples];
    int at = 0;
    for (long[] run : runs) {
      for (long k = 0; k < run[0]; k++) {
        values[at++] = run[1];
      }
    }
    return values;
  }

  private static long editMediaTime(ByteBuffer trak) {
    ByteBuffer edts = optionalChild(trak, "edts");
    if (edts == null) {
      return 0L;
    }
    ByteBuffer elst = child(edts, "elst");
    int version = elst.get() & 0xFF;
    elst.position(elst.position() + 3);
    int entries = elst.getInt();
    if (entries != 1) {
      throw new IllegalArgumentException("the oracle reads one edit, the file has " + entries);
    }
    if (version == 1) {
      elst.getLong();
      return elst.getLong();
    }
    elst.getInt();
    return elst.getInt();
  }

  private static long timescale(ByteBuffer mdhd) {
    int version = mdhd.get() & 0xFF;
    mdhd.position(mdhd.position() + 3 + (version == 1 ? 16 : 8));
    return mdhd.getInt() & 0xFFFFFFFFL;
  }

  private static ByteBuffer child(ByteBuffer parent, String type) {
    ByteBuffer found = optionalChild(parent, type);
    if (found == null) {
      throw new IllegalArgumentException("no " + type + " box");
    }
    return found;
  }

  private static ByteBuffer optionalChild(ByteBuffer parent, String type) {
    ByteBuffer scan = parent.duplicate();
    while (scan.remaining() >= 8) {
      ByteBuffer found = nextChild(scan, type);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /** The body of the next box of {@code type} at or after {@code scan}'s position (advancing {@code scan} past it), or null. */
  private static ByteBuffer nextChild(ByteBuffer scan, String type) {
    while (scan.remaining() >= 8) {
      int start = scan.position();
      long size = scan.getInt() & 0xFFFFFFFFL;
      byte[] name = new byte[4];
      scan.get(name);
      int header = 8;
      if (size == 1) {
        size = scan.getLong();
        header = 16;
      } else if (size == 0) {
        size = scan.limit() - start;
      }
      int end = (int) (start + size);
      if (type.equals(new String(name, StandardCharsets.ISO_8859_1))) {
        ByteBuffer body = scan.duplicate();
        body.position(start + header);
        body.limit(end);
        scan.position(end);
        return body.slice();
      }
      scan.position(end);
    }
    return null;
  }
}
