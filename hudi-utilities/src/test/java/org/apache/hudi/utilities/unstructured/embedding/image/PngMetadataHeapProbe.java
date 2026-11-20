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

package org.apache.hudi.utilities.unstructured.embedding.image;

import javax.imageio.stream.FileImageInputStream;

import java.awt.color.ColorSpace;
import java.awt.color.ICC_Profile;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;

/** A small heap makes a redundant ancillary-profile copy observable without a heap dump. */
public final class PngMetadataHeapProbe {
  private PngMetadataHeapProbe() {
  }

  public static void main(String[] args) throws Exception {
    Path directory = Paths.get(args[0]);
    Path small = Files.createTempFile(directory, "png-profile-control-", ".png");
    Path large = Files.createTempFile(directory, "png-profile-large-", ".png");
    try {
      ByteArrayOutputStream profile = new ByteArrayOutputStream();
      try (DeflaterOutputStream zip = new DeflaterOutputStream(profile)) {
        zip.write(ICC_Profile.getInstance(ColorSpace.CS_sRGB).getData());
      }
      writePng(small, profile.toByteArray(), 0);
      ImagePreparer preparer = new ImagePreparer(new ImagePreprocessingSpec(512, 16L << 20, 100_000_000L, 20L << 20,
          InProcessImageDecoder.class.getName(), ImagePreprocessingSpec.ENCODING_PNG, 90));
      checkPixel(preparer.prepare(() -> new FileImageInputStream(small.toFile())));
      System.out.println("CONTROL_OK max_heap=" + Runtime.getRuntime().maxMemory());
      writePng(large, new byte[0], 18 * 1024 * 1024);
      try {
        checkPixel(preparer.prepare(() -> new FileImageInputStream(large.toFile())));
        System.out.println("LARGE_PROFILE_HANDLED accepted");
      } catch (IOException contentFailure) {
        // A reader may reject the unused large profile; it must do so without exhausting the task heap.
        System.out.println("LARGE_PROFILE_HANDLED content_refused=" + contentFailure.getClass().getSimpleName());
      }
    } finally {
      Files.deleteIfExists(small);
      Files.deleteIfExists(large);
    }
  }

  private static void checkPixel(PreparedImage image) {
    if (image.getWidth() != 1 || image.getHeight() != 1) {
      throw new AssertionError("Expected a one-pixel model input");
    }
  }

  private static void writePng(Path file, byte[] profile, int padding) throws IOException {
    try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file)))) {
      out.writeLong(0x89504e470d0a1a0aL);
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream header = new DataOutputStream(bytes);
      header.writeInt(1);
      header.writeInt(1);
      header.write(new byte[] {8, 3, 0, 0, 0});
      chunk(out, "IHDR", bytes.toByteArray());
      chunk(out, "PLTE", new byte[] {0, 0, 0, (byte) 255, (byte) 255, (byte) 255});
      byte[] name = "iCCP".getBytes(StandardCharsets.US_ASCII);
      byte[] prefix = "profile\0\0".getBytes(StandardCharsets.US_ASCII);
      CRC32 crc = new CRC32();
      crc.update(name);
      crc.update(prefix);
      crc.update(profile);
      out.writeInt(prefix.length + profile.length + padding);
      out.write(name);
      out.write(prefix);
      out.write(profile);
      byte[] block = new byte[8192];
      while (padding > 0) {
        int size = Math.min(block.length, padding);
        out.write(block, 0, size);
        crc.update(block, 0, size);
        padding -= size;
      }
      out.writeInt((int) crc.getValue());
      ByteArrayOutputStream pixels = new ByteArrayOutputStream();
      try (DeflaterOutputStream zip = new DeflaterOutputStream(pixels)) {
        zip.write(new byte[] {0, 0});
      }
      chunk(out, "IDAT", pixels.toByteArray());
      chunk(out, "IEND", new byte[0]);
    }
  }

  private static void chunk(DataOutputStream out, String name, byte[] bytes) throws IOException {
    byte[] tag = name.getBytes(StandardCharsets.US_ASCII);
    CRC32 crc = new CRC32();
    crc.update(tag);
    crc.update(bytes);
    out.writeInt(bytes.length);
    out.write(tag);
    out.write(bytes);
    out.writeInt((int) crc.getValue());
  }
}
