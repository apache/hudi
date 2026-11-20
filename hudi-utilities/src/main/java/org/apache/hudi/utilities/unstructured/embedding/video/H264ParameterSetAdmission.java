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

/**
 * Admission of an H.264 parameter set before JCodec 0.2.5 parses it. JCodec's {@code SeqParameterSet} and
 * {@code PictureParameterSet} readers size arrays from counts a few header bits carry, and its decoder keeps parameter sets
 * in tables indexed by their ids, so a short header can ask for a large allocation before any frame-size guard runs. This
 * reads the same syntax elements in JCodec's order, from the same unescaped bytes, and bounds each count and id as H.264
 * bounds it (7.4.2.1.1, 7.4.2.2):
 * <ul>
 *   <li>sequence parameter set: {@code seq_parameter_set_id} at most {@value #MAX_SPS_ID},
 *       {@code num_ref_frames_in_pic_order_cnt_cycle} at most {@value #MAX_POC_CYCLE}, and {@code cpb_cnt_minus1} of each
 *       HRD at most {@value #MAX_CPB_CNT_MINUS1};</li>
 *   <li>picture parameter set: {@code pic_parameter_set_id} at most {@value #MAX_PPS_ID}, {@code seq_parameter_set_id} at
 *       most {@value #MAX_SPS_ID}, {@code num_slice_groups_minus1} at most {@value #MAX_SLICE_GROUPS_MINUS1}, and
 *       {@code pic_size_in_map_units_minus1} below the macroblocks of the largest coded picture JCodec decodes.</li>
 * </ul>
 * A parameter set that ends before those elements, or an Exp-Golomb code longer than 32 bits, is refused as well.
 */
final class H264ParameterSetAdmission {

  static final int MAX_SPS_ID = 31;
  static final int MAX_PPS_ID = 255;
  static final int MAX_POC_CYCLE = 255;
  static final int MAX_CPB_CNT_MINUS1 = 31;
  static final int MAX_SLICE_GROUPS_MINUS1 = 7;
  /** Map units (macroblocks) of the largest coded picture JCodec decodes. */
  static final long MAX_MAP_UNITS = JcodecKeyframeVideoFrameSource.MAX_CODED_PIXELS / (16 * 16);

  private static final String SPS = "sequence parameter set";
  private static final String PPS = "picture parameter set";

  private H264ParameterSetAdmission() {
  }

  /**
   * Why JCodec must not parse the sequence parameter set {@code payload} (the NAL unit after its header byte, still escaped),
   * or null.
   */
  static String spsRefusal(ByteBuffer payload) {
    Bits bits = new Bits(payload, SPS);
    try {
      int profile = bits.read(8, "profile_idc");
      bits.read(16, "constraint and level");
      bits.bounded("seq_parameter_set_id", MAX_SPS_ID);
      if (profile == 100 || profile == 110 || profile == 122 || profile == 144) {   // JCodec's high-profile set
        long chroma = bits.ue("chroma_format_idc");
        if (chroma == 3) {
          bits.read(1, "separate_colour_plane_flag");
        }
        bits.ue("bit_depth_luma_minus8");
        bits.ue("bit_depth_chroma_minus8");
        bits.read(1, "qpprime_y_zero_transform_bypass_flag");
        if (bits.flag("seq_scaling_matrix_present_flag")) {
          for (int i = 0; i < 8; i++) {
            if (bits.flag("seq_scaling_list_present_flag")) {
              scalingList(bits, i < 6 ? 16 : 64);
            }
          }
        }
      }
      bits.ue("log2_max_frame_num_minus4");
      long pocType = bits.ue("pic_order_cnt_type");
      if (pocType == 0) {
        bits.ue("log2_max_pic_order_cnt_lsb_minus4");
      } else if (pocType == 1) {
        bits.read(1, "delta_pic_order_always_zero_flag");
        bits.se("offset_for_non_ref_pic");
        bits.se("offset_for_top_to_bottom_field");
        long cycle = bits.bounded("num_ref_frames_in_pic_order_cnt_cycle", MAX_POC_CYCLE);
        for (long i = 0; i < cycle; i++) {
          bits.se("offset_for_ref_frame");
        }
      }
      bits.ue("max_num_ref_frames");
      bits.read(1, "gaps_in_frame_num_value_allowed_flag");
      bits.ue("pic_width_in_mbs_minus1");
      bits.ue("pic_height_in_map_units_minus1");
      if (!bits.flag("frame_mbs_only_flag")) {
        bits.read(1, "mb_adaptive_frame_field_flag");
      }
      bits.read(1, "direct_8x8_inference_flag");
      if (bits.flag("frame_cropping_flag")) {
        for (int i = 0; i < 4; i++) {
          bits.ue("frame_crop_offset");
        }
      }
      if (bits.flag("vui_parameters_present_flag")) {
        vui(bits);
      }
      return null;
    } catch (Refusal refusal) {
      return refusal.getMessage();
    }
  }

  /**
   * Why JCodec must not parse the picture parameter set {@code payload} (the NAL unit after its header byte, still escaped),
   * or null.
   */
  static String ppsRefusal(ByteBuffer payload) {
    Bits bits = new Bits(payload, PPS);
    try {
      bits.bounded("pic_parameter_set_id", MAX_PPS_ID);
      bits.bounded("seq_parameter_set_id", MAX_SPS_ID);
      bits.read(1, "entropy_coding_mode_flag");
      bits.read(1, "bottom_field_pic_order_in_frame_present_flag");
      long groupsMinus1 = bits.bounded("num_slice_groups_minus1", MAX_SLICE_GROUPS_MINUS1);
      if (groupsMinus1 > 0) {
        long mapType = bits.ue("slice_group_map_type");
        if (mapType == 0) {
          for (long i = 0; i <= groupsMinus1; i++) {
            bits.ue("run_length_minus1");
          }
        } else if (mapType == 2) {
          for (long i = 0; i < groupsMinus1; i++) {
            bits.ue("top_left");
            bits.ue("bottom_right");
          }
        } else if (mapType == 3 || mapType == 4 || mapType == 5) {
          bits.read(1, "slice_group_change_direction_flag");
          bits.ue("slice_group_change_rate_minus1");
        } else if (mapType == 6) {
          bits.bounded("pic_size_in_map_units_minus1", MAX_MAP_UNITS - 1);
        }
      }
      return null;
    } catch (Refusal refusal) {
      return refusal.getMessage();
    }
  }

  /** JCodec's readScalingList: delta-coded entries until the list is full or its first entry ends it. */
  private static void scalingList(Bits bits, int size) throws Refusal {
    int last = 8;
    int next = 8;
    for (int j = 0; j < size; j++) {
      if (next != 0) {
        long delta = bits.se("delta_scale");
        next = (int) ((last + delta + 256) % 256);
        if (j == 0 && next == 0) {
          return;
        }
      }
      last = next == 0 ? last : next;
    }
  }

  /** JCodec's readVUIParameters up to its last HRD, the last element it sizes an array from. */
  private static void vui(Bits bits) throws Refusal {
    if (bits.flag("aspect_ratio_info_present_flag") && bits.read(8, "aspect_ratio_idc") == 255) {
      bits.read(32, "sar_width and sar_height");
    }
    if (bits.flag("overscan_info_present_flag")) {
      bits.read(1, "overscan_appropriate_flag");
    }
    if (bits.flag("video_signal_type_present_flag")) {
      bits.read(4, "video_format and video_full_range_flag");
      if (bits.flag("colour_description_present_flag")) {
        bits.read(24, "colour description");
      }
    }
    if (bits.flag("chroma_loc_info_present_flag")) {
      bits.ue("chroma_sample_loc_type_top_field");
      bits.ue("chroma_sample_loc_type_bottom_field");
    }
    if (bits.flag("timing_info_present_flag")) {
      bits.read(32, "num_units_in_tick");
      bits.read(32, "time_scale");
      bits.read(1, "fixed_frame_rate_flag");
    }
    if (bits.flag("nal_hrd_parameters_present_flag")) {
      hrd(bits);
    }
    if (bits.flag("vcl_hrd_parameters_present_flag")) {
      hrd(bits);
    }
  }

  private static void hrd(Bits bits) throws Refusal {
    long cpbCntMinus1 = bits.bounded("cpb_cnt_minus1", MAX_CPB_CNT_MINUS1);
    bits.read(8, "bit_rate_scale and cpb_size_scale");
    for (long i = 0; i <= cpbCntMinus1; i++) {
      bits.ue("bit_rate_value_minus1");
      bits.ue("cpb_size_value_minus1");
      bits.read(1, "cbr_flag");
    }
    bits.read(20, "hrd delays and lengths");
  }

  /** A refusal message, thrown out of the reading. */
  private static final class Refusal extends Exception {
    Refusal(String message) {
      super(message, null, false, false);
    }
  }

  /** Big-endian bits of an unescaped parameter set, as JCodec's BitReader and CAVLCReader read them. */
  private static final class Bits {
    private final byte[] data;
    private final int length;
    private final String what;
    private long position;

    Bits(ByteBuffer payload, String what) {
      this.what = what;
      ByteBuffer in = payload.duplicate();
      this.data = new byte[in.remaining()];
      int out = 0;
      int previous = -1;
      int last = -1;
      while (in.hasRemaining()) {
        int b = in.get() & 0xff;
        if (!(previous == 0 && last == 0 && b == 3)) {   // H264Utils.unescapeNAL: a 3 after two zero input bytes is dropped
          data[out++] = (byte) b;
        }
        previous = last;
        last = b;
      }
      this.length = out;
    }

    int read(int width, String field) throws Refusal {
      if (position + width > (long) length * 8) {
        throw new Refusal("has an H.264 " + what + " that ends inside " + field);
      }
      long value = 0;
      for (int i = 0; i < width; i++) {
        int bit = (data[(int) (position >>> 3)] >>> (7 - (int) (position & 7))) & 1;
        value = (value << 1) | bit;
        position++;
      }
      return (int) value;
    }

    boolean flag(String field) throws Refusal {
      return read(1, field) == 1;
    }

    long ue(String field) throws Refusal {
      int zeros = 0;
      while (read(1, field) == 0) {
        if (++zeros > 31) {
          throw new Refusal("has an H.264 " + what + " whose " + field + " is coded in more than 32 bits");
        }
      }
      long rest = 0;
      for (int i = 0; i < zeros; i++) {
        rest = (rest << 1) | read(1, field);
      }
      return (1L << zeros) - 1 + rest;
    }

    long se(String field) throws Refusal {
      long code = ue(field);
      return (code & 1) == 1 ? (code + 1) / 2 : -(code / 2);
    }

    long bounded(String field, long max) throws Refusal {
      long value = ue(field);
      if (value > max) {
        throw new Refusal("has an H.264 " + what + " with " + field + " " + value + ", above the " + max
            + " H.264 allows; JCodec would allocate from it before decoding");
      }
      return value;
    }
  }
}
