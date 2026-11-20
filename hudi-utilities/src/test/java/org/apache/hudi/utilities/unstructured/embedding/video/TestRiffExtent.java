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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** {@link RiffExtent} over synthetic RIFF layouts: what fits, what a cut leaves, and the sizes it cannot judge. */
class TestRiffExtent {

  @TempDir
  Path dir;

  @Test
  void chunksThatFitTheFileAreAccepted() throws IOException {
    assertNull(extent(riff("AVI ", 100)));
    // an AVI past 1 GiB continues in AVIX chunks: each is walked
    assertNull(extent(concat(riff("AVI ", 100), riff("AVIX", 64))));
    // an odd-sized chunk is padded to even, except the last one of a file that ends right after its data
    assertNull(extent(concat(riff("AVI ", 101), new byte[1], riff("AVIX", 33))));
    // data after the last chunk that is not a RIFF chunk is not judged
    assertNull(extent(concat(riff("AVI ", 100), "JUNKJUNKJUNK".getBytes(StandardCharsets.US_ASCII))));
  }

  @Test
  void chunkThatOverrunsTheFileIsATruncation() throws IOException {
    byte[] whole = riff("AVI ", 100);
    assertEquals("has a RIFF chunk at offset 0 declaring 104 bytes, but the file ends at 60",
        extent(Arrays.copyOf(whole, 60)));
    byte[] two = concat(riff("AVI ", 100), riff("AVIX", 64));
    assertEquals("has a RIFF chunk at offset 112 declaring 68 bytes, but the file ends at 150",
        extent(Arrays.copyOf(two, 150)));
  }

  /** A size the writer never patched (FFmpeg's placeholder on a pipe) or zero says nothing about the file's length. */
  @Test
  void unpatchedSizeIsNotJudged() throws IOException {
    byte[] unpatched = riff("AVI ", 100);
    unpatched[4] = (byte) 0xFF;
    unpatched[5] = (byte) 0xFF;
    unpatched[6] = (byte) 0xFF;
    unpatched[7] = (byte) 0xFF;
    assertNull(extent(unpatched));
    byte[] zero = riff("AVI ", 100);
    zero[4] = 0;
    zero[5] = 0;
    zero[6] = 0;
    zero[7] = 0;
    assertNull(extent(zero));
  }

  private String extent(byte[] bytes) throws IOException {
    Path file = Files.write(Files.createTempFile(dir, "riff", ".avi"), bytes);
    return RiffExtent.truncation(VideoClips.supplier(file), "fid x");
  }

  /** A RIFF chunk of {@code form} whose data (form type included) is {@code 4 + payload} bytes, unpadded. */
  private static byte[] riff(String form, int payload) {
    int size = 4 + payload;
    byte[] chunk = new byte[8 + size];
    System.arraycopy("RIFF".getBytes(StandardCharsets.US_ASCII), 0, chunk, 0, 4);
    chunk[4] = (byte) size;
    chunk[5] = (byte) (size >> 8);
    chunk[6] = (byte) (size >> 16);
    chunk[7] = (byte) (size >> 24);
    System.arraycopy(form.getBytes(StandardCharsets.US_ASCII), 0, chunk, 8, 4);
    return chunk;
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] part : parts) {
      out.write(part, 0, part.length);
    }
    return out.toByteArray();
  }
}
