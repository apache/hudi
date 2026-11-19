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

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Source rows must fit the intermediate budget even when subsampling makes the destination small. */
public class TestDecoderSourceRows {

  @TempDir
  Path temporary;

  @BeforeEach
  public void resetCounter() {
    CountingImageDecoder.CALLS.set(0);
  }

  @ParameterizedTest
  @CsvSource({"1,0,1500", "2,0,3000", "4,0,6000", "8,0,12000", "16,0,24000",
      "1,3,1500", "8,3,12000", "8,2,36000", "16,2,72000", "8,4,24000", "16,4,48000",
      "8,6,48000", "16,6,96000"})
  public void pngRowsAreChargedBeforeDecode(int depth, int color, long rowBytes) throws IOException {
    byte[] png = pngRow(4000, depth, color);
    ImageRefusedException refusal = assertThrows(ImageRefusedException.class,
        () -> new ImagePreparer(spec(rowBytes - 1)).prepare(ImageFixtures.source(png)));
    assertTrue(refusal.getMessage().startsWith(ImageRefusedException.REFUSED_BEFORE_DECODE));
    assertEquals(0, CountingImageDecoder.CALLS.get());
  }

  @ParameterizedTest
  @CsvSource({"1,0,1500", "2,0,3000", "4,0,6000", "8,0,12000", "16,0,24000",
      "1,3,1500", "8,3,12000", "8,2,36000", "16,2,72000", "8,4,24000", "16,4,48000",
      "8,6,48000", "16,6,96000"})
  public void pngRowsAtBudgetDecode(int depth, int color, long rowBytes) throws IOException {
    PreparedImage image = new ImagePreparer(spec(rowBytes)).prepare(ImageFixtures.source(pngRow(4000, depth, color)));
    assertEquals(512, image.getWidth());
    assertEquals(1, image.getHeight());
    assertEquals(1, CountingImageDecoder.CALLS.get());
  }

  @ParameterizedTest
  @CsvSource({"bmp,12000", "gif,4000"})
  public void neighboringSourceRowsAreChargedBeforeDecode(String format, long rowBytes) throws IOException {
    byte[] bytes = ImageFixtures.image(format, 4000, 1);
    if ("bmp".equals(format)) {
      assertEquals(24, (bytes[28] & 0xff) | (bytes[29] & 0xff) << 8, "the BMP fixture has a 24-bit source row");
    }
    assertThrows(ImageRefusedException.class,
        () -> new ImagePreparer(spec(rowBytes - 1)).prepare(ImageFixtures.source(bytes)));
    assertEquals(0, CountingImageDecoder.CALLS.get());
  }

  @ParameterizedTest
  @CsvSource({"bmp,12000", "gif,4000"})
  public void neighboringSourceRowsAtBudgetDecode(String format, long rowBytes) throws IOException {
    byte[] bytes = ImageFixtures.image(format, 4000, 1);
    if ("bmp".equals(format)) {
      assertEquals(24, (bytes[28] & 0xff) | (bytes[29] & 0xff) << 8, "the BMP fixture has a 24-bit source row");
    }
    PreparedImage image = new ImagePreparer(spec(rowBytes)).prepare(ImageFixtures.source(bytes));
    assertEquals(512, image.getWidth());
    assertEquals(1, CountingImageDecoder.CALLS.get());
  }

  @Test
  public void smallPngRemainsWithinIntermediateBudget() throws IOException {
    PreparedImage image = new ImagePreparer(spec(1L << 20)).prepare(ImageFixtures.source(pngRow(512, 8, 6)));
    assertEquals(512, image.getWidth());
    assertEquals(1, CountingImageDecoder.CALLS.get());
  }

  @Test
  public void ancillaryProfileDoesNotRequireASecondTaskHeapCopy() throws Exception {
    Set<String> classPath = new LinkedHashSet<>();
    for (Class<?> type : new Class<?>[] {PngMetadataHeapProbe.class, ImagePreparer.class, HoodieException.class,
        StoragePath.class, Logger.class}) {
      classPath.add(Paths.get(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toString());
    }
    Path output = temporary.resolve("png-metadata-child.log");
    Process child = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
        "-Xmx32m", "-XX:-HeapDumpOnOutOfMemoryError", "--class-path", String.join(File.pathSeparator, classPath),
        PngMetadataHeapProbe.class.getName(), temporary.toString()).redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      assertTrue(child.waitFor(60, TimeUnit.SECONDS), "the bounded profile fixture must terminate");
      String report = Files.readString(output);
      System.out.print(report);
      assertEquals(0, child.exitValue(), report);
      assertTrue(report.contains("CONTROL_OK"), report);
      assertTrue(report.contains("LARGE_PROFILE_HANDLED"), report);
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly().waitFor();
      }
    }
  }

  private static ImagePreprocessingSpec spec(long budget) {
    return new ImagePreprocessingSpec(512, 16L << 20, 100_000_000L, budget,
        CountingImageDecoder.class.getName(), ImagePreprocessingSpec.ENCODING_PNG, 90);
  }

  /** One valid, uniform PNG row; generates compressed bytes without a full source raster. */
  private static byte[] pngRow(int width, int depth, int color) throws IOException {
    int bands = color == 2 ? 3 : color == 4 ? 2 : color == 6 ? 4 : 1;
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(encoded);
    out.writeLong(0x89504e470d0a1a0aL);
    ByteArrayOutputStream ihdr = new ByteArrayOutputStream();
    DataOutputStream header = new DataOutputStream(ihdr);
    header.writeInt(width);
    header.writeInt(1);
    header.write(new byte[] {(byte) depth, (byte) color, 0, 0, 0});
    chunk(out, "IHDR", ihdr.toByteArray());
    if (color == 3) {
      chunk(out, "PLTE", new byte[] {0, 0, 0, (byte) 255, (byte) 255, (byte) 255});
    }
    ByteArrayOutputStream zipped = new ByteArrayOutputStream();
    try (DeflaterOutputStream deflater = new DeflaterOutputStream(zipped)) {
      deflater.write(0);
      int remaining = (width * depth * bands + 7) / 8;
      byte[] block = new byte[4096];
      while (remaining > 0) {
        int size = Math.min(block.length, remaining);
        deflater.write(block, 0, size);
        remaining -= size;
      }
    }
    chunk(out, "IDAT", zipped.toByteArray());
    chunk(out, "IEND", new byte[0]);
    return encoded.toByteArray();
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
