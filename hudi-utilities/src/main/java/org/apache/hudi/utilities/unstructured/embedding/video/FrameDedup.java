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

import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;

/**
 * Dedup of consecutive frames of one video ({@link VideoFrameConfig.DedupMethod}): each candidate is compared with the last
 * KEPT frame, never with the last candidate, so a slow drift cannot drop a whole segment one small step at a time. It also makes
 * the frame's dedup key: the 16-hex dHash for {@code dhash}, the hex SHA-256 of the encoded frame otherwise. One instance per
 * video; its state is the last kept frame's hash.
 */
final class FrameDedup {

  /** The dHash grid: 9 columns compared pairwise give 8 bits per row, 8 rows. */
  private static final int GRID_COLUMNS = 9;
  private static final int GRID_ROWS = 8;

  private final VideoFrameConfig.DedupMethod method;
  private final int maxDistance;
  private boolean anyKept;
  private long lastHash;
  private String lastKey;

  FrameDedup(VideoFrameConfig.DedupMethod method, int maxDistance) {
    this.method = method;
    this.maxDistance = maxDistance;
  }

  /** Whether the decision needs only the decoded frame ({@link #keepDecoded}), so a dropped frame is never encoded. */
  boolean decidesBeforePrepare() {
    return method == VideoFrameConfig.DedupMethod.DHASH;
  }

  /**
   * {@code dhash} only: decides on the decoded frame and records it when kept.
   *
   * @return the frame's dedup key when it is kept, null when it repeats the last kept frame
   */
  String keepDecoded(BufferedImage frame) {
    long hash = dhash(frame);
    if (anyKept && Long.bitCount(hash ^ lastHash) <= maxDistance) {
      return null;
    }
    anyKept = true;
    lastHash = hash;
    return String.format(Locale.ROOT, "%016x", hash);
  }

  /**
   * {@code sha256} / {@code none}: decides on the encoded frame's bytes and records it when kept.
   *
   * @return the frame's dedup key when it is kept, null when it repeats the last kept frame
   */
  String keepPrepared(byte[] encoded) {
    String key = sha256Hex(encoded);
    if (method == VideoFrameConfig.DedupMethod.SHA256 && key.equals(lastKey)) {
      return null;
    }
    lastKey = key;
    return key;
  }

  /**
   * The 64-bit difference hash of a 3-byte BGR frame: luma (integer BT.601 weights) box-averaged over a 9×8 grid, one bit per
   * horizontally adjacent cell pair (set when the left cell is darker), rows top to bottom, most significant bit first. Integer
   * arithmetic only, so the hash of a frame is the same on every JVM. Axes smaller than the grid reuse nearest pixels.
   */
  static long dhash(BufferedImage frame) {
    if (frame.getType() != BufferedImage.TYPE_3BYTE_BGR) {
      throw new IllegalArgumentException("dhash expects a TYPE_3BYTE_BGR frame, got type " + frame.getType());
    }
    int sourceWidth = frame.getWidth();
    int sourceHeight = frame.getHeight();
    // Reuse nearest source pixels for a grid axis smaller than its sample count, without allocating an upscaled raster.
    int width = Math.max(sourceWidth, GRID_COLUMNS);
    int height = Math.max(sourceHeight, GRID_ROWS);
    byte[] bgr = ((DataBufferByte) frame.getRaster().getDataBuffer()).getData();
    long[] sums = new long[GRID_COLUMNS * GRID_ROWS];
    long[] counts = new long[GRID_COLUMNS * GRID_ROWS];
    for (int y = 0; y < height; y++) {
      int row = (int) ((long) y * GRID_ROWS / height);
      int offset = (int) ((long) y * sourceHeight / height) * sourceWidth * 3;
      for (int x = 0; x < width; x++) {
        int column = (int) ((long) x * GRID_COLUMNS / width);
        int i = offset + (int) ((long) x * sourceWidth / width) * 3;
        int luma = (114 * (bgr[i] & 0xff) + 587 * (bgr[i + 1] & 0xff) + 299 * (bgr[i + 2] & 0xff)) / 1000;
        sums[row * GRID_COLUMNS + column] += luma;
        counts[row * GRID_COLUMNS + column]++;
      }
    }
    long hash = 0;
    for (int row = 0; row < GRID_ROWS; row++) {
      for (int column = 0; column < GRID_COLUMNS - 1; column++) {
        int left = row * GRID_COLUMNS + column;
        // compare averages without division: left/countLeft < right/countRight
        boolean darker = sums[left] * counts[left + 1] < sums[left + 1] * counts[left];
        hash = (hash << 1) | (darker ? 1 : 0);
      }
    }
    return hash;
  }

  static String sha256Hex(byte[] bytes) {
    return digestHex(bytes, "SHA-256");
  }

  /** The lower-case hex {@code algorithm} digest of {@code bytes}; a test seam for a JVM without the digest. */
  static String digestHex(byte[] bytes, String algorithm) {
    try {
      byte[] digest = MessageDigest.getInstance(algorithm).digest(bytes);
      StringBuilder hex = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        hex.append(String.format(Locale.ROOT, "%02x", b));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      // a JVM without the digest cannot key any frame: the executor's, never the video's (class P)
      throw new PreprocessorConfigurationException(algorithm + " is not available in this JVM", e);
    }
  }
}
