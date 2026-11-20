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

import org.jcodec.codecs.h264.H264Utils;
import org.jcodec.codecs.h264.io.model.PictureParameterSet;
import org.jcodec.codecs.h264.io.model.SeqParameterSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Each bounded count or id is admitted at its bound, read where JCodec reads it, and refused one above it. */
class TestH264ParameterSetAdmission {

  @ParameterizedTest
  @CsvSource({"seq_parameter_set_id,31", "num_ref_frames_in_pic_order_cnt_cycle,255", "cpb_cnt_minus1,31"})
  void sequenceParameterSetCountsAreBounded(String field, int max) {
    SeqParameterSet atBound = SeqParameterSet.read(ByteBuffer.wrap(unescaped(sps(field, max))));
    assertEquals(max, valueOf(atBound, field), "JCodec reads the element where the admission reads it");
    assertNull(H264ParameterSetAdmission.spsRefusal(ByteBuffer.wrap(sps(field, max))), field + " at " + max);
    String refusal = H264ParameterSetAdmission.spsRefusal(ByteBuffer.wrap(sps(field, max + 1)));
    assertNotNull(refusal, field + " above " + max);
    assertTrue(refusal.contains(field + " " + (max + 1)), refusal);
  }

  @ParameterizedTest
  @CsvSource({"pic_parameter_set_id,255", "seq_parameter_set_id,31", "num_slice_groups_minus1,7",
      "pic_size_in_map_units_minus1,9111"})
  void pictureParameterSetCountsAreBounded(String field, int max) {
    assertEquals(H264ParameterSetAdmission.MAX_MAP_UNITS - 1, 9111, "the map-unit bound is 1920x1080 coded with headroom");
    PictureParameterSet atBound = PictureParameterSet.read(ByteBuffer.wrap(unescaped(pps(field, max))));
    assertEquals(max, valueOf(atBound, field), "JCodec reads the element where the admission reads it");
    assertNull(H264ParameterSetAdmission.ppsRefusal(ByteBuffer.wrap(pps(field, max))), field + " at " + max);
    String refusal = H264ParameterSetAdmission.ppsRefusal(ByteBuffer.wrap(pps(field, max + 1)));
    assertNotNull(refusal, field + " above " + max);
    assertTrue(refusal.contains(field + " " + (max + 1)), refusal);
  }

  @Test
  void parameterSetCutShortIsRefused() {
    byte[] whole = sps("num_ref_frames_in_pic_order_cnt_cycle", 3);
    for (int length = 0; length < 4; length++) {
      String refusal = H264ParameterSetAdmission.spsRefusal(ByteBuffer.wrap(whole, 0, length));
      assertNotNull(refusal, "cut to " + length + " bytes");
      assertTrue(refusal.contains("ends inside"), refusal);
    }
    assertTrue(H264ParameterSetAdmission.ppsRefusal(ByteBuffer.allocate(0)).contains("ends inside pic_parameter_set_id"));
  }

  @Test
  void expGolombCodeLongerThan32BitsIsRefused() {
    String refusal = H264ParameterSetAdmission.ppsRefusal(ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0, 1}));
    assertTrue(refusal.contains("pic_parameter_set_id is coded in more than 32 bits"), refusal);
  }

  /** An emulation-prevention byte is dropped as JCodec drops it, so the bits after it are the ones JCodec reads. */
  @Test
  void emulationPreventionBytesAreDroppedBeforeReading() {
    byte[] escaped = new byte[] {0, 0, 3, 0, 0, 3, (byte) 0x80};  // 32 zero bits, then a stop bit, escaped twice
    String refusal = H264ParameterSetAdmission.ppsRefusal(ByteBuffer.wrap(escaped));
    assertTrue(refusal.contains("pic_parameter_set_id is coded in more than 32 bits"), refusal);
    byte[] kept = new byte[] {0, 0, 3, 3, (byte) 0x80};         // only the first 3 after two zero bytes is dropped
    assertEquals(unescaped(kept).length, 4);
    assertTrue(H264ParameterSetAdmission.ppsRefusal(ByteBuffer.wrap(kept)).contains("pic_parameter_set_id"));
  }

  /** A baseline SPS with one bounded element set to {@code value}; the rest minimal and valid. */
  static byte[] sps(String field, int value) {
    Bits bits = new Bits();
    bits.fixed(66, 8);
    bits.fixed(0, 8);
    bits.fixed(30, 8);
    bits.ue("seq_parameter_set_id".equals(field) ? value : 0);
    bits.ue(0);                                                    // log2_max_frame_num_minus4
    boolean cycle = "num_ref_frames_in_pic_order_cnt_cycle".equals(field);
    bits.ue(cycle ? 1 : 0);                                        // pic_order_cnt_type
    if (cycle) {
      bits.fixed(1, 1);
      bits.ue(0);
      bits.ue(0);
      bits.ue(value);
      for (int i = 0; i < value; i++) {
        bits.ue(0);
      }
    } else {
      bits.ue(0);                                                  // log2_max_pic_order_cnt_lsb_minus4
    }
    bits.ue(1);                                                    // max_num_ref_frames
    bits.fixed(0, 1);
    bits.ue(9);                                                    // 160 pixels wide
    bits.ue(7);                                                    // 128 high
    bits.fixed(1, 1);                                              // frame_mbs_only_flag
    bits.fixed(1, 1);
    bits.fixed(0, 1);                                              // no cropping
    boolean hrd = "cpb_cnt_minus1".equals(field);
    bits.fixed(hrd ? 1 : 0, 1);                                    // vui_parameters_present_flag
    if (hrd) {
      bits.fixed(0, 5);                                            // no aspect, overscan, signal, chroma loc, timing
      bits.fixed(1, 1);                                            // nal_hrd_parameters_present_flag
      bits.ue(value);
      bits.fixed(0, 8);
      for (int i = 0; i <= value; i++) {
        bits.ue(0);
        bits.ue(0);
        bits.fixed(0, 1);
      }
      bits.fixed(0, 20);
      bits.fixed(0, 1);                                            // no VCL HRD
      bits.fixed(0, 1);                                            // low_delay_hrd_flag
      bits.fixed(0, 2);                                            // no pic_struct, no bitstream restriction
    }
    return bits.escaped();
  }

  /** A PPS with one bounded element set to {@code value}; the rest minimal and valid. */
  static byte[] pps(String field, int value) {
    Bits bits = new Bits();
    bits.ue("pic_parameter_set_id".equals(field) ? value : 0);
    bits.ue("seq_parameter_set_id".equals(field) ? value : 0);
    bits.fixed(0, 2);
    boolean map = "pic_size_in_map_units_minus1".equals(field);
    int groupsMinus1 = "num_slice_groups_minus1".equals(field) ? value : (map ? 1 : 0);
    bits.ue(groupsMinus1);
    if (map) {
      bits.ue(6);
      bits.ue(value);
      for (int i = 0; i <= value; i++) {
        bits.fixed(0, 1);                                          // one bit per map unit for two groups
      }
    } else if (groupsMinus1 > 0) {
      bits.ue(3);                                                  // box-out: one flag and one rate
      bits.fixed(0, 1);
      bits.ue(0);
    }
    bits.ue(0);
    bits.ue(0);
    bits.fixed(0, 3);
    bits.ue(0);
    bits.ue(0);
    bits.ue(0);
    bits.fixed(0, 3);
    return bits.escaped();
  }

  private static int valueOf(SeqParameterSet sps, String field) {
    switch (field) {
      case "seq_parameter_set_id":
        return sps.seqParameterSetId;
      case "num_ref_frames_in_pic_order_cnt_cycle":
        return sps.numRefFramesInPicOrderCntCycle;
      default:
        return sps.vuiParams.nalHRDParams.cpbCntMinus1;
    }
  }

  private static int valueOf(PictureParameterSet pps, String field) {
    switch (field) {
      case "pic_parameter_set_id":
        return pps.picParameterSetId;
      case "seq_parameter_set_id":
        return pps.seqParameterSetId;
      case "num_slice_groups_minus1":
        return pps.numSliceGroupsMinus1;
      default:
        return pps.sliceGroupId.length - 1;
    }
  }

  private static byte[] unescaped(byte[] escaped) {
    ByteBuffer copy = ByteBuffer.wrap(escaped.clone());
    H264Utils.unescapeNAL(copy);
    byte[] out = new byte[copy.remaining()];
    copy.get(out);
    return out;
  }

  /** Exp-Golomb and fixed-width bits, closed with RBSP trailing bits and escaped as H.264 escapes a NAL payload. */
  private static final class Bits {
    private final StringBuilder bits = new StringBuilder();

    void fixed(int value, int width) {
      for (int bit = width - 1; bit >= 0; bit--) {
        bits.append((value >>> bit) & 1);
      }
    }

    void ue(int value) {
      long coded = value + 1L;
      int width = 64 - Long.numberOfLeadingZeros(coded);
      fixed(0, width - 1);
      for (int bit = width - 1; bit >= 0; bit--) {
        bits.append((coded >>> bit) & 1);
      }
    }

    byte[] escaped() {
      StringBuilder all = new StringBuilder(bits).append('1');
      while (all.length() % 8 != 0) {
        all.append('0');
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      int zeros = 0;
      for (int offset = 0; offset < all.length(); offset += 8) {
        int value = Integer.parseInt(all.substring(offset, offset + 8), 2);
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
