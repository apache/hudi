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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The compute-side image preparation: header-only reads, the raster plan asserted before decode, the decode/resize logic,
 * the decoder seam and the compute image settings.
 */
class TestImagePreparation {

  /** Lossless PNG output: these cases assert exact pixels. The default JPEG q90 output is covered by its own cases below. */
  private static final ImagePreprocessingSpec DEFAULTS = new ImagePreprocessingSpec(
      ImagePreprocessingSpec.DEFAULT_LONG_SIDE, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
      ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, ImagePreprocessingSpec.DEFAULT_INTERMEDIATE_BUDGET_BYTES,
      InProcessImageDecoder.class.getName(), ImagePreprocessingSpec.ENCODING_PNG, ImagePreprocessingSpec.DEFAULT_JPEG_QUALITY);
  /** The defaults: JPEG at quality 90. */
  private static final ImagePreprocessingSpec JPEG_DEFAULTS = new ImagePreprocessingSpec(
      ImagePreprocessingSpec.DEFAULT_LONG_SIDE, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
      ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, InProcessImageDecoder.class.getName());

  @BeforeEach
  void resetCounters() {
    CountingImageDecoder.CALLS.set(0);
  }

  @ParameterizedTest
  @CsvSource({"png,png", "jpeg,jpeg", "gif,gif", "bmp,bmp", "tiff,tiff"})
  void headerOfEachFormat(String writeFormat, String expectedFormat) throws IOException {
    ImageHeader header = header(ImageFixtures.image(writeFormat, 123, 45));
    assertEquals(expectedFormat, header.getFormat());
    assertEquals(123, header.getWidth());
    assertEquals(45, header.getHeight());
    assertEquals(123L * 45, header.pixels());
  }

  @Test
  void webpHeaderIsParsed() throws IOException {
    ImageHeader header = header(ImageFixtures.webpHeader(300, 200));
    assertEquals("webp", header.getFormat());
    assertEquals(300, header.getWidth());
    assertEquals(200, header.getHeight());
  }

  @Test
  void corruptHeaderThrows() {
    byte[] truncatedPng = ImageFixtures.truncatedPng();
    ImageHeaderException truncated = assertThrows(ImageHeaderException.class, () -> header(truncatedPng));
    assertTrue(truncated.getMessage().startsWith("corrupt png image header"), truncated.getMessage());
    byte[] garbageBytes = ImageFixtures.garbage();
    ImageHeaderException garbage = assertThrows(ImageHeaderException.class, () -> header(garbageBytes));
    assertEquals("no image reader recognises the content", garbage.getMessage());
  }

  @Test
  void smallImageIsNotUpscaled() throws IOException {
    PreparedImage prepared = new ImagePreparer(DEFAULTS).prepare(ImageFixtures.source(ImageFixtures.image("png", 50, 30)));
    assertEquals(1, prepared.getSubsampling());
    assertEquals(50, prepared.getWidth());
    assertEquals(30, prepared.getHeight());
    assertEquals("image/png", prepared.getMimeType());
    BufferedImage decoded = ImageFixtures.decode(prepared.getEncoded());
    assertEquals(50, decoded.getWidth());
    assertEquals(30, decoded.getHeight());
  }

  @Test
  void subsamplingIsChosenFromTheHeader() throws IOException {
    byte[] jpeg = ImageFixtures.image("jpeg", 4000, 3000);
    RasterPlan plan = new ImagePreparer(DEFAULTS).plan(ImageFixtures.source(jpeg));
    // floor(4000 / 512) = 7; ceil(4000/7) x ceil(3000/7) x 3 bytes = 572 x 429 x 3 = 736164 <= 16 MiB
    assertEquals(7, plan.subsampling());
    assertEquals(572, plan.decodedWidth());
    assertEquals(429, plan.decodedHeight());
    assertEquals(572L * 429 * 3, plan.predictedBytes());
    PreparedImage prepared = new ImagePreparer(DEFAULTS).prepare(ImageFixtures.source(jpeg));
    assertEquals(7, prepared.getSubsampling());
    assertEquals(512, prepared.getWidth());
    assertEquals(384, prepared.getHeight());
  }

  @Test
  void rasterBudgetGrowsTheSubsampling() throws IOException {
    ImagePreprocessingSpec tight = new ImagePreprocessingSpec(512, 100_000, ImagePreprocessingSpec.DEFAULT_MAX_PIXELS,
        InProcessImageDecoder.class.getName());
    RasterPlan plan = new ImagePreparer(tight).plan(ImageFixtures.source(ImageFixtures.image("png", 1000, 800)));
    // floor rule gives s=1 (2.4 MB at 3 B/px); the budget pushes s until ceil(1000/s)*ceil(800/s)*3 <= 100000:
    // s=4 is 250x200x3 = 150000, s=5 is 200x160x3 = 96000
    assertEquals(5, plan.subsampling());
    assertEquals(96000, plan.predictedBytes());
    assertTrue(plan.predictedBytes() <= 100_000, plan.toString());
    PreparedImage prepared = new ImagePreparer(tight).prepare(ImageFixtures.source(ImageFixtures.image("png", 1000, 800)));
    assertEquals(200, prepared.getWidth());
    assertEquals(160, prepared.getHeight());
  }

  /**
   * A BMP whose bitmap is an embedded JPEG or PNG (compression 4 or 5), which a reader decodes at full size outside the
   * intermediate budget's estimate, is refused by name before decode (the JDK readers fail it before decode too, with no image
   * type, which was reported as a corrupt header); an uncompressed BMP of the same size is prepared.
   */
  @ParameterizedTest
  @CsvSource({"BI_JPEG", "BI_PNG"})
  void bmpWithAnEmbeddedJpegOrPngIsRefusedBeforeDecode(String compression) throws IOException {
    ImagePreparer preparer = new ImagePreparer(new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
        ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, CountingImageDecoder.class.getName()));
    ImageSource embedded = ImageFixtures.source(bmp(compression));
    ImageRefusedException refused = assertThrows(ImageRefusedException.class, () -> preparer.prepare(embedded));
    assertTrue(refused.getMessage().startsWith(ImageRefusedException.REFUSED_BEFORE_DECODE), refused.getMessage());
    assertTrue(refused.getMessage().contains("embedded JPEG or PNG"), refused.getMessage());
    assertEquals(0, CountingImageDecoder.CALLS.get(), "nothing is decoded");
    PreparedImage control = preparer.prepare(ImageFixtures.source(bmp("BI_RGB")));
    assertEquals(64, control.getWidth());
  }

  /** A 64 x 48 RGB BMP written by the JDK writer with {@code compression}. */
  private static byte[] bmp(String compression) throws IOException {
    ImageWriter writer = ImageIO.getImageWritersByFormatName("bmp").next();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ImageOutputStream stream = ImageIO.createImageOutputStream(out)) {
      writer.setOutput(stream);
      ImageWriteParam param = writer.getDefaultWriteParam();
      param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
      param.setCompressionType(compression);
      writer.write(null, new IIOImage(new BufferedImage(64, 48, BufferedImage.TYPE_INT_RGB), null, null), param);
    } finally {
      writer.dispose();
    }
    return out.toByteArray();
  }

  @Test
  void oversizedImageIsRefusedBeforeDecode() {
    ImagePreprocessingSpec guarded = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES, 1000,
        CountingImageDecoder.class.getName());
    ImagePreparer preparer = new ImagePreparer(guarded);
    ImageSource source = ImageFixtures.source(ImageFixtures.image("png", 100, 100));
    ImageRefusedException refused = assertThrows(ImageRefusedException.class, () -> preparer.prepare(source));
    assertTrue(refused.getMessage().startsWith(ImageRefusedException.REFUSED_BEFORE_DECODE), refused.getMessage());
    assertTrue(refused.getMessage().contains("10000 pixels"), refused.getMessage());
    assertEquals(0, CountingImageDecoder.CALLS.get());
  }

  @Test
  void controlWithinTheGuardIsDecoded() throws IOException {
    ImagePreprocessingSpec guarded = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES, 10000,
        CountingImageDecoder.class.getName());
    new ImagePreparer(guarded).prepare(ImageFixtures.source(ImageFixtures.image("png", 100, 100)));
    assertEquals(1, CountingImageDecoder.CALLS.get());
  }

  @Test
  void webpIsRefusedWithoutADecoder() {
    ImagePreprocessingSpec counting = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
        ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, CountingImageDecoder.class.getName());
    ImagePreparer preparer = new ImagePreparer(counting);
    ImageSource source = ImageFixtures.source(ImageFixtures.webpHeader(300, 200));
    ImageRefusedException refused = assertThrows(ImageRefusedException.class, () -> preparer.prepare(source));
    assertTrue(refused.getMessage().startsWith(ImageRefusedException.NO_DECODER), refused.getMessage());
    assertEquals(0, CountingImageDecoder.CALLS.get());
  }

  @Test
  void grayPaletteAndAlphaBecomeRgb() throws IOException {
    for (byte[] png : new byte[][] {ImageFixtures.grayPng(64, 48), ImageFixtures.palettePng(64, 48), ImageFixtures.alphaPng(64, 48)}) {
      BufferedImage out = ImageFixtures.decode(new ImagePreparer(DEFAULTS).prepare(ImageFixtures.source(png)).getEncoded());
      assertEquals(3, out.getColorModel().getNumComponents());
      assertEquals(64, out.getWidth());
    }
    // fully transparent left column is flattened on white
    BufferedImage alpha = ImageFixtures.decode(new ImagePreparer(DEFAULTS)
        .prepare(ImageFixtures.source(ImageFixtures.alphaPng(64, 48))).getEncoded());
    assertEquals(0xffffff, alpha.getRGB(0, 10) & 0xffffff);
  }

  @Test
  void multiFrameGifReadsTheFirstImage() throws IOException {
    PreparedImage prepared = new ImagePreparer(DEFAULTS).prepare(ImageFixtures.source(ImageFixtures.image("gif", 80, 60)));
    assertEquals(80, prepared.getWidth());
    assertEquals(60, prepared.getHeight());
  }

  /**
   * The default model input is JPEG at quality 90: a JPEG stream of the resized size, and at quality 90 smaller
   * than the same pixels at quality 100 while decoding to the same dimensions; PNG stays available and is lossless.
   */
  @Test
  void defaultOutputIsJpegAtQuality90() throws IOException {
    byte[] source = ImageFixtures.image("png", 1500, 1000);
    PreparedImage jpeg = new ImagePreparer(JPEG_DEFAULTS).prepare(ImageFixtures.source(source));
    assertEquals("image/jpeg", jpeg.getMimeType());
    assertEquals((byte) 0xFF, jpeg.getEncoded()[0]);
    assertEquals((byte) 0xD8, jpeg.getEncoded()[1]);
    BufferedImage decoded = ImageFixtures.decode(jpeg.getEncoded());
    assertEquals(512, decoded.getWidth());
    assertEquals(341, decoded.getHeight());
    ImagePreprocessingSpec q100 = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
        ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, ImagePreprocessingSpec.DEFAULT_INTERMEDIATE_BUDGET_BYTES,
        InProcessImageDecoder.class.getName(), ImagePreprocessingSpec.ENCODING_JPEG, 100);
    PreparedImage best = new ImagePreparer(q100).prepare(ImageFixtures.source(source));
    assertTrue(jpeg.getEncoded().length < best.getEncoded().length, jpeg.getEncoded().length + " vs " + best.getEncoded().length);
    PreparedImage png = new ImagePreparer(DEFAULTS).prepare(ImageFixtures.source(source));
    assertEquals("image/png", png.getMimeType());
    // the same resized pixels under both encodings; JPEG differs from them by its loss only
    assertEquals(512, ImageFixtures.decode(png.getEncoded()).getWidth());
  }

  @Test
  void encodingAndQualityAreValidated() {
    String decoder = InProcessImageDecoder.class.getName();
    assertThrows(IllegalArgumentException.class, () -> new ImagePreprocessingSpec(512, 1L << 24, 1000, 1L << 26, decoder, "webp", 90));
    assertThrows(IllegalArgumentException.class, () -> new ImagePreprocessingSpec(512, 1L << 24, 1000, 1L << 26, decoder, ImagePreprocessingSpec.ENCODING_JPEG, 0));
    assertThrows(IllegalArgumentException.class, () -> new ImagePreprocessingSpec(512, 1L << 24, 1000, 1L << 26, decoder, ImagePreprocessingSpec.ENCODING_JPEG, 101));
  }

  @Test
  void anotherCodePathWithTheSameLogicGivesTheSameBytes() throws IOException {
    ImagePreprocessingSpec handoff = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
        ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, ThreadHandoffImageDecoder.class.getName());
    byte[] jpeg = ImageFixtures.image("jpeg", 1500, 1000);
    PreparedImage inProcess = new ImagePreparer(JPEG_DEFAULTS).prepare(ImageFixtures.source(jpeg));
    PreparedImage handedOff = new ImagePreparer(handoff).prepare(ImageFixtures.source(jpeg));
    assertEquals("image-decoder-handoff", ThreadHandoffImageDecoder.LAST_THREAD.get());
    assertArrayEquals(inProcess.getEncoded(), handedOff.getEncoded());
  }

  @Test
  void decoderRunningAnotherLogicIsRefused() {
    ImagePreprocessingSpec foreign = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
        ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, ForeignLogicImageDecoder.class.getName());
    assertThrows(ImageDecoderLogicMismatchException.class, () -> new ImagePreparer(foreign));
  }

  @Test
  void specFromComputeConfig() {
    TypedProperties props = new TypedProperties();
    ImagePreprocessingSpec defaults = EmbeddingTableConfig.from(props).imagePreprocessing();
    assertEquals(ImagePreprocessingSpec.DEFAULT_LONG_SIDE, defaults.longSide());
    assertEquals(ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES, defaults.rasterBudgetBytes());
    assertEquals(ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, defaults.maxPixels());
    assertEquals(ImagePreprocessingSpec.DEFAULT_INTERMEDIATE_BUDGET_BYTES, defaults.intermediateBudgetBytes());
    assertEquals(InProcessImageDecoder.class.getName(), defaults.decoderClass());
    assertEquals(ImagePreprocessingSpec.ENCODING_JPEG, defaults.encoding());
    assertEquals(ImagePreprocessingSpec.DEFAULT_JPEG_QUALITY, defaults.jpegQuality());
    props.setProperty(EmbeddingTableConfig.COMPUTE_IMAGE_LONG_SIDE.key(), "256");
    props.setProperty(EmbeddingTableConfig.COMPUTE_IMAGE_MAX_PIXELS.key(), "5000");
    props.setProperty(EmbeddingTableConfig.COMPUTE_IMAGE_INTERMEDIATE_BUDGET_BYTES.key(), "4096");
    props.setProperty(EmbeddingTableConfig.COMPUTE_IMAGE_ENCODING.key(), "PNG");
    props.setProperty(EmbeddingTableConfig.COMPUTE_IMAGE_JPEG_QUALITY.key(), "75");
    ImagePreprocessingSpec spec = EmbeddingTableConfig.from(props).imagePreprocessing();
    assertEquals(256, spec.longSide());
    assertEquals(5000, spec.maxPixels());
    assertEquals(4096, spec.intermediateBudgetBytes());
    assertEquals(ImagePreprocessingSpec.ENCODING_PNG, spec.encoding());
    assertEquals(75, spec.jpegQuality());
    for (String[] bad : new String[][] {{EmbeddingTableConfig.COMPUTE_IMAGE_RASTER_BUDGET_BYTES.key(), "-1"},
        {EmbeddingTableConfig.COMPUTE_IMAGE_LONG_SIDE.key(), "0"}, {EmbeddingTableConfig.COMPUTE_IMAGE_MAX_PIXELS.key(), "many"},
        {EmbeddingTableConfig.COMPUTE_IMAGE_JPEG_QUALITY.key(), "101"}, {EmbeddingTableConfig.COMPUTE_IMAGE_ENCODING.key(), "webp"},
        {EmbeddingTableConfig.COMPUTE_IMAGE_DECODER_CLASS.key(), String.class.getName()}}) {
      TypedProperties badProps = new TypedProperties();
      badProps.setProperty(bad[0], bad[1]);
      EmbeddingTableConfig badConfig = EmbeddingTableConfig.from(badProps);
      IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, badConfig::imagePreprocessing, bad[0]);
      assertTrue(refused.getMessage().contains(bad[0]), refused.getMessage());
    }
    props.setProperty(EmbeddingTableConfig.COMPUTE_IMAGE_DECODER_CLASS.key(), ForeignLogicImageDecoder.class.getName());
    EmbeddingTableConfig foreignConfig = EmbeddingTableConfig.from(props);
    assertThrows(ImageDecoderLogicMismatchException.class, foreignConfig::imagePreprocessing);
  }

  @Test
  void onlyJdkReadersDecideTheHeaderAndThePixels() throws IOException {
    // a third-party plugin ordered first must not change what image-resize-v1 decodes
    ForeignPngReaderSpi foreign = ForeignPngReaderSpi.installFirst();
    try {
      byte[] png = ImageFixtures.image("png", 123, 45);
      ImageHeader header = header(png);
      assertEquals(123, header.getWidth());
      assertEquals(45, header.getHeight());
      PreparedImage prepared = new ImagePreparer(DEFAULTS).prepare(ImageFixtures.source(png));
      assertEquals(123, prepared.getWidth());
      assertEquals(45, prepared.getHeight());
    } finally {
      ForeignPngReaderSpi.uninstall(foreign);
    }
  }

  @Test
  void singleStripTiffAboveTheIntermediateBudgetIsRefusedBeforeDecode() throws IOException {
    // the JDK TIFF reader decodes a whole strip at full resolution before subsampling; 400x300x3 = 360000 B
    byte[] tiff = ImageFixtures.singleStripTiff(400, 300);
    ImagePreprocessingSpec tight = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
        ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, 100_000, CountingImageDecoder.class.getName());
    ImagePreparer preparer = new ImagePreparer(tight);
    ImageSource source = ImageFixtures.source(tiff);
    ImageRefusedException refused = assertThrows(ImageRefusedException.class, () -> preparer.prepare(source));
    assertTrue(refused.getMessage().startsWith(ImageRefusedException.REFUSED_BEFORE_DECODE), refused.getMessage());
    assertTrue(refused.getMessage().contains("360000"), refused.getMessage());
    assertEquals(0, CountingImageDecoder.CALLS.get());
    // control: the same file under the default budget decodes
    PreparedImage prepared = new ImagePreparer(DEFAULTS).prepare(ImageFixtures.source(tiff));
    assertEquals(400, prepared.getWidth());
    assertEquals(300, prepared.getHeight());
  }

  @Test
  void tiledTiffIsChargedOneTile() throws IOException {
    // neighbour: 64x64 tiles cost 64*64*3 = 12288 B, under a 100000 B budget although the image is 640000 B
    byte[] tiff = ImageFixtures.tiledTiff(400, 300, 64);
    ImagePreprocessingSpec tight = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
        ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, 100_000, InProcessImageDecoder.class.getName());
    PreparedImage prepared = new ImagePreparer(tight).prepare(ImageFixtures.source(tiff));
    assertEquals(400, prepared.getWidth());
  }

  @Test
  void progressiveJpegAboveTheIntermediateBudgetIsRefusedBeforeDecode() throws IOException {
    // libjpeg holds every DCT coefficient of a progressive JPEG; 800x600 4:2:0 = (100*75 + 2*50*38) blocks * 128 B
    byte[] progressive = ImageFixtures.progressiveJpeg(800, 600);
    ImagePreprocessingSpec tight = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
        ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, 1_000_000, CountingImageDecoder.class.getName());
    ImagePreparer preparer = new ImagePreparer(tight);
    ImageSource source = ImageFixtures.source(progressive);
    ImageRefusedException refused = assertThrows(ImageRefusedException.class, () -> preparer.prepare(source));
    assertTrue(refused.getMessage().contains(String.valueOf((100L * 75 + 2L * 50 * 38) * 128)), refused.getMessage());
    assertEquals(0, CountingImageDecoder.CALLS.get());
    // neighbour: a baseline JPEG of the same size has no whole-image coefficient buffer and passes the same budget
    PreparedImage baseline = new ImagePreparer(tight).prepare(ImageFixtures.source(ImageFixtures.image("jpeg", 800, 600)));
    assertEquals(512, baseline.getWidth());
    // control: the progressive file under the default budget decodes
    assertEquals(512, new ImagePreparer(DEFAULTS).prepare(ImageFixtures.source(progressive)).getWidth());
  }

  @Test
  void wideShortPngIsRefusedBeforeDecode() {
    // the JDK PNG reader holds the current and prior scanline and a one-row raster at full width before subsampling:
    // 60,000,000 x 1 RGBA is 60,000,000 pixels (under the 100-million pixel guard) but 3 x 240,000,000 B of rows
    byte[] wide = ImageFixtures.zeroPng(60_000_000, 1, 8, 6);
    ImagePreprocessingSpec defaults = new ImagePreprocessingSpec(ImagePreprocessingSpec.DEFAULT_LONG_SIDE,
        ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES, ImagePreprocessingSpec.DEFAULT_MAX_PIXELS,
        ImagePreprocessingSpec.DEFAULT_INTERMEDIATE_BUDGET_BYTES, CountingImageDecoder.class.getName());
    ImagePreparer preparer = new ImagePreparer(defaults);
    ImageSource source = ImageFixtures.source(wide);
    ImageRefusedException refused = assertThrows(ImageRefusedException.class, () -> preparer.prepare(source));
    assertTrue(refused.getMessage().startsWith(ImageRefusedException.REFUSED_BEFORE_DECODE), refused.getMessage());
    assertTrue(refused.getMessage().contains("720000000"), refused.getMessage());
    assertEquals(0, CountingImageDecoder.CALLS.get());
  }

  @Test
  void widePngWithinTheIntermediateBudgetIsDecoded() throws IOException {
    // control: 1,000,000 x 1 RGBA needs 3 x 4,000,000 B of rows, under the default budget, and decodes
    byte[] wide = ImageFixtures.zeroPng(1_000_000, 1, 8, 6);
    ImagePreprocessingSpec defaults = new ImagePreprocessingSpec(ImagePreprocessingSpec.DEFAULT_LONG_SIDE,
        ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES, ImagePreprocessingSpec.DEFAULT_MAX_PIXELS,
        ImagePreprocessingSpec.DEFAULT_INTERMEDIATE_BUDGET_BYTES, CountingImageDecoder.class.getName());
    PreparedImage prepared = new ImagePreparer(defaults).prepare(ImageFixtures.source(wide));
    assertEquals(1, CountingImageDecoder.CALLS.get());
    assertTrue(prepared.getWidth() <= ImagePreprocessingSpec.DEFAULT_LONG_SIDE, String.valueOf(prepared.getWidth()));
    // the same file under a 10,000,000 B budget is refused before decode
    ImagePreprocessingSpec tight = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
        ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, 10_000_000, CountingImageDecoder.class.getName());
    ImagePreparer preparer = new ImagePreparer(tight);
    ImageSource source = ImageFixtures.source(wide);
    ImageRefusedException refused = assertThrows(ImageRefusedException.class, () -> preparer.prepare(source));
    assertTrue(refused.getMessage().contains("12000000"), refused.getMessage());
    assertEquals(1, CountingImageDecoder.CALLS.get());
    // neighbour: an ordinary 1000x800 PNG needs 3 x 3,000 B of rows and decodes under that budget
    assertEquals(512, new ImagePreparer(tight).prepare(ImageFixtures.source(ImageFixtures.image("png", 1000, 800))).getWidth());
  }

  /** Three full-width rows of {@code ceil(width x samples x bitDepth / 8)} bytes: refused one byte below, admitted at it. */
  @ParameterizedTest
  @CsvSource({
      "1000, 8, 6, 12000",
      "1000, 16, 6, 24000",
      "1000, 8, 2, 9000",
      "1000, 16, 4, 12000",
      "1000, 8, 3, 3000",
      "1001, 4, 3, 1503",
      "1001, 1, 0, 378",
      "1000, 16, 0, 6000"})
  void pngIsChargedThreeFullWidthRows(int width, int bitDepth, int colorType, long expected) throws IOException {
    byte[] png = ImageFixtures.zeroPng(width, 2, bitDepth, colorType);
    assertChargedExactly(png, expected);
  }

  /** One full-width row of a BI_RGB BMP, padded to 4 bytes: refused one byte below, admitted at it. */
  @ParameterizedTest
  @CsvSource({
      "1001, 1, 128",
      "1001, 4, 504",
      "1001, 8, 1004",
      "1001, 16, 2004",
      "1001, 24, 3004",
      "1000, 32, 4000"})
  void uncompressedBmpIsChargedOneFullWidthRow(int width, int bitsPerPixel, long expected) throws IOException {
    assertChargedExactly(ImageFixtures.uncompressedBmp(width, 2, bitsPerPixel), expected);
  }

  @Test
  void wideShortUncompressedBmpIsRefusedBeforeDecode() throws IOException {
    // the JDK BMP reader reads one full-width row before subsampling it: 400,000 x 1 x 24 bits is 1,200,000 B
    byte[] wide = ImageFixtures.uncompressedBmp(400_000, 1, 24);
    ImagePreprocessingSpec tight = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
        ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, 1_000_000, CountingImageDecoder.class.getName());
    ImagePreparer preparer = new ImagePreparer(tight);
    ImageSource source = ImageFixtures.source(wide);
    ImageRefusedException refused = assertThrows(ImageRefusedException.class, () -> preparer.prepare(source));
    assertTrue(refused.getMessage().startsWith(ImageRefusedException.REFUSED_BEFORE_DECODE), refused.getMessage());
    assertTrue(refused.getMessage().contains("1200000"), refused.getMessage());
    assertEquals(0, CountingImageDecoder.CALLS.get());
    // neighbour: 300,000 x 1 is 900,000 B and decodes under the same budget
    new ImagePreparer(tight).prepare(ImageFixtures.source(ImageFixtures.uncompressedBmp(300_000, 1, 24)));
    assertEquals(1, CountingImageDecoder.CALLS.get());
  }

  private static void assertChargedExactly(byte[] image, long expected) throws IOException {
    ImagePreprocessingSpec below = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
        ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, expected - 1, CountingImageDecoder.class.getName());
    ImagePreparer preparer = new ImagePreparer(below);
    ImageSource source = ImageFixtures.source(image);
    ImageRefusedException refused = assertThrows(ImageRefusedException.class, () -> preparer.plan(source));
    assertTrue(refused.getMessage().contains("needs about " + expected + " bytes"), refused.getMessage());
    ImagePreprocessingSpec at = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
        ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, expected, CountingImageDecoder.class.getName());
    new ImagePreparer(at).plan(ImageFixtures.source(image));
  }

  private static ImageHeader header(byte[] bytes) throws IOException {
    try (ImageInputStream in = ImageFixtures.source(bytes).open()) {
      return ImageHeaderReader.read(in);
    }
  }
}
