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
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;

import org.junit.jupiter.api.Test;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Consecutive-frame dedup: the difference hash, the threshold, the last-kept reference and the exact methods. */
class TestFrameDedup {

  @Test
  void framesBelowTheHashGridUseDeterministicNearestPixels() {
    VideoFrameConfig defaults = VideoFrameConfig.from(new TypedProperties());
    FrameDedup dedup = new FrameDedup(defaults.getDedupMethod(), defaults.getMaxDistance());
    BufferedImage tiny = new BufferedImage(1, 1, BufferedImage.TYPE_3BYTE_BGR);
    assertEquals("0000000000000000", dedup.keepDecoded(tiny));
    assertNull(dedup.keepDecoded(tiny));
    BufferedImage ramp = new BufferedImage(2, 1, BufferedImage.TYPE_3BYTE_BGR);
    ramp.setRGB(1, 0, 0xFFFFFF);
    assertEquals("0808080808080808", dedup.keepDecoded(ramp));
    assertNull(dedup.keepDecoded(ramp));
  }

  @Test
  void dhashBitsAreTheLeftRightOrderOfAGreyGrid() {
    // every row brightening to the right: all 64 bits set; a row darkening to the right clears that row's 8 bits
    assertEquals(-1L, FrameDedup.dhash(grid(0)));
    assertEquals(0x00FFFFFFFFFFFFFFL, FrameDedup.dhash(grid(1)));
    assertEquals(0x0000FFFFFFFFFFFFL, FrameDedup.dhash(grid(2)));
    // the same pixels hash the same, whatever the frame size (box averages, integer maths)
    assertEquals(FrameDedup.dhash(grid(1)), FrameDedup.dhash(scaled(grid(1), 3)));
  }

  @Test
  void theThresholdDecidesWhetherANearFrameIsARepeat() {
    // grid(1) is 8 bits from grid(0)
    FrameDedup strict = new FrameDedup(VideoFrameConfig.DedupMethod.DHASH, 4);
    assertNotNull(strict.keepDecoded(grid(0)));
    assertNotNull(strict.keepDecoded(grid(1)), "8 bits apart is a new frame under threshold 4");
    FrameDedup loose = new FrameDedup(VideoFrameConfig.DedupMethod.DHASH, 8);
    assertNotNull(loose.keepDecoded(grid(0)));
    assertNull(loose.keepDecoded(grid(1)), "8 bits apart is a repeat under threshold 8");
    assertNull(loose.keepDecoded(grid(0)), "an identical frame is always a repeat");
  }

  @Test
  void framesAreComparedWithTheLastKeptFrameNotTheLastCandidate() {
    // 0 → 1 → 2 drifts 8 bits per step: under threshold 8, 1 repeats 0 (dropped), 2 is 16 bits from the kept 0 (kept)
    FrameDedup dedup = new FrameDedup(VideoFrameConfig.DedupMethod.DHASH, 8);
    assertEquals("ffffffffffffffff", dedup.keepDecoded(grid(0)));
    assertNull(dedup.keepDecoded(grid(1)));
    assertEquals("0000ffffffffffff", dedup.keepDecoded(grid(2)));
  }

  @Test
  void sha256DropsOnlyExactRepeatsAndNoneDropsNothing() {
    byte[] a = "frame a".getBytes(StandardCharsets.UTF_8);
    byte[] b = "frame b".getBytes(StandardCharsets.UTF_8);
    FrameDedup exact = new FrameDedup(VideoFrameConfig.DedupMethod.SHA256, 4);
    assertEquals(FrameDedup.sha256Hex(a), exact.keepPrepared(a));
    assertNull(exact.keepPrepared(a.clone()));
    assertEquals(FrameDedup.sha256Hex(b), exact.keepPrepared(b));
    assertEquals(64, exact.keepPrepared(a).length(), "a after b is new again");
    FrameDedup none = new FrameDedup(VideoFrameConfig.DedupMethod.NONE, 4);
    assertNotNull(none.keepPrepared(a));
    assertNotNull(none.keepPrepared(a));
  }

  @Test
  void missingDigestIsAConfigurationError() {
    // a JVM without the digest cannot key any frame: the platform's, never the video's (class P)
    assertThrows(PreprocessorConfigurationException.class, () -> FrameDedup.digestHex(new byte[1], "NO-SUCH-DIGEST"));
    assertEquals(64, FrameDedup.sha256Hex(new byte[1]).length());
  }

  @Test
  void dhashRefusesFramesItCannotHash() {
    BufferedImage intRgb = new BufferedImage(90, 80, BufferedImage.TYPE_INT_RGB);
    assertThrows(IllegalArgumentException.class, () -> FrameDedup.dhash(intRgb));
  }

  /** A 90x80 grey frame of 9x8 cells of 10x10 pixels; the first {@code darkeningRows} rows darken to the right. */
  private static BufferedImage grid(int darkeningRows) {
    BufferedImage image = new BufferedImage(90, 80, BufferedImage.TYPE_3BYTE_BGR);
    byte[] bgr = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
    for (int y = 0; y < 80; y++) {
      for (int x = 0; x < 90; x++) {
        int column = x / 10;
        int level = 20 + 25 * (y / 10 < darkeningRows ? 8 - column : column);
        int i = (y * 90 + x) * 3;
        bgr[i] = (byte) level;
        bgr[i + 1] = (byte) level;
        bgr[i + 2] = (byte) level;
      }
    }
    return image;
  }

  private static BufferedImage scaled(BufferedImage source, int factor) {
    BufferedImage image = new BufferedImage(source.getWidth() * factor, source.getHeight() * factor, BufferedImage.TYPE_3BYTE_BGR);
    for (int y = 0; y < image.getHeight(); y++) {
      for (int x = 0; x < image.getWidth(); x++) {
        image.setRGB(x, y, source.getRGB(x / factor, y / factor));
      }
    }
    return image;
  }
}
