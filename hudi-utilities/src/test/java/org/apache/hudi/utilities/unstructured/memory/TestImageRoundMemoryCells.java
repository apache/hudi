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

package org.apache.hudi.utilities.unstructured.memory;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.sources.blob.BlobFileNames;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BucketFolderFileSystem;
import org.apache.hudi.utilities.sources.blob.S3EventsTableFixture;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFormatFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageRefusedException;
import org.apache.hudi.utilities.unstructured.embedding.prep.ImageInputPreprocessor;

import org.apache.spark.sql.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Memory cells of image files through the single-writer embedding round. Each cell writes generated images to the bucket,
 * announces them in ONE events commit, then runs the embedding job over the unstructured data source on S3 ({@code prep_and_compute}, the
 * dummy multimodal client through {@link ConcurrencyCountingMultimodalClient}, compute parallelism {@value #COMPUTE_PARALLELISM},
 * every persist DISK_ONLY) in a child JVM ({@link ImageRoundMemoryChild}) whose heap is the reference heap
 * ({@value #REFERENCE_HEAP}, the embedding cells' one, unless {@code -Dimage.memory.heap} says otherwise), local[4]. On this path
 * prep reads only an image's header (random access over the source, capped at
 * {@code hoodie.unstructured.embedding.prep.image.header.max.bytes}) and stores the source row's own blob value as the row's
 * {@code prepared_image} (inline bytes for an object up to the inline threshold, else an unmanaged reference to the object, no
 * copy); compute opens the stored image as a seekable stream, plans the decode from the header (pixel guard, decoder
 * intermediate budget, subsampling until the raster fits its budget) and decodes and resizes it in memory. The cells pressure
 * that bound: images far larger in pixels, and in bytes, than the decoded raster, decoded by four tasks sharing one heap. A
 * cell passes when the child ends its round without an exhausted heap and the table holds one row per admitted image with its
 * vector; an image the guards refuse is refused cleanly (one error event naming it, and no vector). Each cell writes its report
 * ({@code MEMORY_CELL} lines) and its child's log under {@code target/memory-cells/}; an exhausted heap leaves
 * {@code <cell>-first-oom.hprof} there.
 *
 * <p>Gated: a cell runs only when named in {@code -Dimage.memory.cells} (a comma list), so the class costs nothing in the
 * ordinary test runs. Cells:
 * <ul>
 *   <li>{@code image-light}: the eight small images of {@link ImageFormatFixtures} (png, jpeg, gif, bmp RLE8 and RLE4, tiff
 *   Deflate, LZW and PackBits, 640x480) and three small images of {@link ImageCellFixtures} (16-bit stored PNG, uncompressed BMP,
 *   uncompressed strip TIFF), every one embedded;</li>
 *   <li>{@code image-formats}: the five stored types near the default 100 MP pixel guard, each in {@value #FORMAT_COPIES}
 *   copies (the near-bound PNG, baseline JPEG, progressive JPEG and two-frame GIF of {@link ImageFormatFixtures}, an
 *   uncompressed 10000x9999 BMP and an uncompressed 10000x9999 TIFF of 64-row strips of {@link ImageCellFixtures}, about 300 MB
 *   each), so four tasks decode near-bound images at once, every one embedded; and once each the near-bound fixtures whose
 *   decoder intermediates are above the default budget ({@link ImageFormatFixtures.Fixture#refusedBeforeDecode()}: RLE8 BMPs,
 *   Deflate TIFF strips and tiles, oversized strip byte counts), each refused cleanly;</li>
 *   <li>{@code image-large}: one 9600x8738 16-bit RGBA PNG in stored deflate blocks (83.9 MP, under the default pixel guards;
 *   about 671 MB, larger than the whole reference heap): above the inline threshold, so its row stores the reference to the
 *   object, and compute decodes it subsampled from a seekable stream, never holding the file whole.</li>
 * </ul>
 */
class TestImageRoundMemoryCells extends EmbeddingRoundTestBase {

  static final String REFERENCE_HEAP = TestEmbeddingRoundMemoryCells.REFERENCE_HEAP;
  private static final long CELL_TIMEOUT_MINUTES = 15;
  private static final int COMPUTE_PARALLELISM = 4;
  /** Copies of each admitted near-bound image in {@code image-formats}: at least one per compute task. */
  private static final int FORMAT_COPIES = 4;
  private static final int NEAR_WIDTH = 10000;
  private static final int NEAR_HEIGHT = 9999;
  private static final int NEAR_TIFF_ROWS_PER_STRIP = 64;
  /** 83.9 MP, under the 100 MP default pixel guards of prep and compute; at 8 bytes per pixel, about 671 MB stored. */
  private static final int LARGE_WIDTH = 9600;
  private static final int LARGE_HEIGHT = 8738;
  private static final String IMAGE_EXTENSIONS = "png,jpg,gif,bmp,tif";

  /** One announced image and what its row must hold. */
  private static final class Expected {
    private final String key;
    private final String sha;
    private final String format;
    private final int width;
    private final int height;
    private final boolean admitted;

    Expected(String key, String format, int width, int height, boolean admitted) {
      this.key = key;
      this.sha = BlobFileNames.sourcePathSha(BlobJobHarness.sourcePath(key));
      this.format = format;
      this.width = width;
      this.height = height;
      this.admitted = admitted;
    }
  }

  private static List<String> selectedCells() {
    String cells = System.getProperty("image.memory.cells", "");
    return Arrays.stream(cells.split(",")).map(String::trim).filter(cell -> !cell.isEmpty()).collect(Collectors.toList());
  }

  @ParameterizedTest(name = "cell {0}")
  @ValueSource(strings = {"image-light", "image-formats", "image-large"})
  void cellPassesAtTheReferenceHeap(String cell) throws Exception {
    assumeTrue(selectedCells().contains(cell), "memory cell " + cell + " not selected (-Dimage.memory.cells)");
    Path work = localPath(basePath()).resolve("cell-" + cell);
    Path generated = Files.createDirectories(work.resolve("generated"));
    String heap = System.getProperty("image.memory.heap", REFERENCE_HEAP);
    List<S3EventsTableFixture.Event> events = new ArrayList<>();
    List<Expected> expected = new ArrayList<>();
    long largeSize = -1;
    switch (cell) {
      case "image-light":
        for (ImageFormatFixtures.Fixture fixture : ImageFormatFixtures.catalog()) {
          if (!fixture.isNearBound()) {
            Path file = ImageFormatFixtures.write(fixture, generated);
            expected.add(put(file, "cells/light/" + fixture.name(), fixture.format(), fixture.width(), fixture.height(), true, events));
          }
        }
        Path png16 = generated.resolve("png16.png");
        ImageCellFixtures.writeStoredPng16(png16, 96, 64, 1L);
        expected.add(put(png16, "cells/light/png16.png", "png", 96, 64, true, events));
        Path bmp = generated.resolve("bmp24.bmp");
        ImageCellFixtures.writeBmp24(bmp, 301, 199, 2L);
        expected.add(put(bmp, "cells/light/bmp24.bmp", "bmp", 301, 199, true, events));
        Path tiff = generated.resolve("strips.tif");
        ImageCellFixtures.writeStripTiff(tiff, 301, 199, 16, 3L);
        expected.add(put(tiff, "cells/light/strips.tif", "tiff", 301, 199, true, events));
        break;
      case "image-formats":
        List<Path> admitted = new ArrayList<>();
        List<String> admittedFormats = new ArrayList<>();
        List<int[]> admittedSizes = new ArrayList<>();
        for (ImageFormatFixtures.Fixture fixture : ImageFormatFixtures.catalog()) {
          if (!fixture.isNearBound()) {
            continue;
          }
          Path file = ImageFormatFixtures.write(fixture, generated);
          if (fixture.refusedBeforeDecode()) {
            expected.add(put(file, "cells/formats/refused/" + fixture.name(), fixture.format(), fixture.width(), fixture.height(),
                false, events));
          } else {
            admitted.add(file);
            admittedFormats.add(fixture.format());
            admittedSizes.add(new int[] {fixture.width(), fixture.height()});
          }
        }
        Path nearBmp = generated.resolve("near-bmp24.bmp");
        ImageCellFixtures.writeBmp24(nearBmp, NEAR_WIDTH, NEAR_HEIGHT, 4L);
        admitted.add(nearBmp);
        admittedFormats.add("bmp");
        admittedSizes.add(new int[] {NEAR_WIDTH, NEAR_HEIGHT});
        Path nearTiff = generated.resolve("near-tiff-short-strips.tif");
        ImageCellFixtures.writeStripTiff(nearTiff, NEAR_WIDTH, NEAR_HEIGHT, NEAR_TIFF_ROWS_PER_STRIP, 5L);
        admitted.add(nearTiff);
        admittedFormats.add("tiff");
        admittedSizes.add(new int[] {NEAR_WIDTH, NEAR_HEIGHT});
        for (int copy = 0; copy < FORMAT_COPIES; copy++) {
          for (int i = 0; i < admitted.size(); i++) {
            String name = admitted.get(i).getFileName().toString();
            expected.add(put(admitted.get(i), "cells/formats/copy-" + copy + "/" + name, admittedFormats.get(i),
                admittedSizes.get(i)[0], admittedSizes.get(i)[1], true, events));
          }
        }
        break;
      case "image-large":
        Path large = generated.resolve("large-png16.png");
        largeSize = ImageCellFixtures.writeStoredPng16(large, LARGE_WIDTH, LARGE_HEIGHT, 6L);
        assertTrue(largeSize > heapBytes(heap), "the image (" + largeSize + " B) is larger than the child's whole heap (" + heap + ")");
        expected.add(put(large, "cells/large/large-png16.png", "png", LARGE_WIDTH, LARGE_HEIGHT, true, events));
        break;
      default:
        throw new IllegalArgumentException(cell);
    }
    objects().announce(spark(), events);

    TypedProperties props = cellProps();
    File propsFile = work.resolve("job.properties").toFile();
    try (OutputStream out = new FileOutputStream(propsFile)) {
      props.store(out, "memory cell " + cell);
    }

    long start = System.nanoTime();
    Path reports = Paths.get("target", "memory-cells");
    Files.createDirectories(reports);
    Path childLog = reports.resolve(cell + "-child.log");
    List<String> output = runChild(heap, propsFile, reports.resolve(cell + "-first-oom.hprof"), childLog);
    long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);

    // graded on the child's own report, the committed table and the error table
    assertTrue(output.contains("RESULT ok"), "the child failed at " + heap + ", see " + childLog.toAbsolutePath());
    List<String> rounds = output.stream().filter(line -> line.startsWith("ROUND ")).collect(Collectors.toList());
    assertEquals(1, rounds.size(), "one round takes the one events commit: " + rounds);
    Map<String, Row> rows = rows(table());
    List<Map<String, Object>> errors = EmbeddingStreamers.payloads(EmbeddingStreamers.errorTablePath(props));
    int admittedCount = 0;
    int refusedAtPrep = 0;
    int refusedAtCompute = 0;
    for (Expected image : expected) {
      List<Row> imageRows = rows.values().stream().filter(row -> image.sha.equals(row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA)))
          .collect(Collectors.toList());
      List<Map<String, Object>> imageErrors = errors.stream().filter(error -> image.sha.equals(error.get("source_path_sha")))
          .collect(Collectors.toList());
      if (image.admitted) {
        admittedCount++;
        assertEquals(1, imageRows.size(), "one image row of " + image.key);
        assertTrue(imageErrors.isEmpty(), image.key + " has no error event: " + imageErrors);
        Row row = imageRows.get(0);
        assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, row.getAs(EmbeddingTableSchema.MODALITY));
        scala.collection.Map<String, String> aux = row.getAs(EmbeddingTableSchema.AUX);
        assertEquals(image.format, aux.apply(ImageInputPreprocessor.AUX_FORMAT), image.key);
        assertEquals(String.valueOf(image.width), aux.apply(ImageInputPreprocessor.AUX_WIDTH), image.key);
        assertEquals(String.valueOf(image.height), aux.apply(ImageInputPreprocessor.AUX_HEIGHT), image.key);
        assertNotNull(vector(row), image.key + " is embedded");
        assertEquals(EmbeddingStreamers.MODEL_DIMENSIONS, vector(row).length, image.key);
      } else {
        // refused before decode by the guards, at prep (a quarantine, no row) or at compute (the row, no vector): either way one
        // error event names it and the round commits
        assertEquals(1, imageErrors.size(), "one error event of the refused " + image.key + ": " + imageErrors);
        if (imageRows.isEmpty()) {
          refusedAtPrep++;
        } else {
          refusedAtCompute++;
          assertEquals(1, imageRows.size(), image.key);
          assertNull(vector(imageRows.get(0)), "the refused " + image.key + " has no vector");
          assertTrue(String.valueOf(imageErrors.get(0).get("error")).contains(ImageRefusedException.REFUSED_BEFORE_DECODE),
              "refused before decode: " + imageErrors.get(0));
        }
      }
    }
    assertEquals(expected.size() - admittedCount, errors.size(), "the error events are the refused images': " + errors);
    String imageInputs = value(output, "IMAGE_INPUTS ");
    assertTrue(imageInputs.startsWith("ok=" + admittedCount + " "), "every admitted image is embedded once: " + imageInputs);
    if ("image-formats".equals(cell)) {
      // the cell's claim is COMPUTE_PARALLELISM near-bound decodes at once at this heap: counted over the images that came back
      // OK only, so an input refused before decode cannot make up the number
      assertTrue(imageInputs.contains(" max_ok_in_flight=" + COMPUTE_PARALLELISM + " "),
          COMPUTE_PARALLELISM + " successful image decodes run at once: " + imageInputs);
      // the near-bound images are refused at compute, before decode; a refusal at prep would be another failure
      assertEquals(0, refusedAtPrep, "the near-bound images are refused at compute, not at prep: " + errors);
      String tasks = imageInputs.substring(imageInputs.indexOf(" tasks=") + " tasks=".length());
      assertEquals(COMPUTE_PARALLELISM, tasks.split(",").length, "the images are decoded by " + COMPUTE_PARALLELISM + " tasks: "
          + imageInputs);
    }

    if ("image-large".equals(cell)) {
      Row image = rows.values().iterator().next().getAs(EmbeddingTableSchema.PREPARED_IMAGE);
      assertEquals(HoodieSchema.Blob.OUT_OF_LINE, image.getAs(HoodieSchema.Blob.TYPE), "above the inline threshold");
      Row reference = image.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE);
      assertEquals(BlobJobHarness.sourcePath(expected.get(0).key), reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH));
      assertEquals(Boolean.FALSE, reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED),
          "an unmanaged reference to the source object: no copy of the file is written");
    }

    long peak = Long.parseLong(value(output, "HEAP_POOL_PEAKS_SUM_BYTES "));
    List<String> report = new ArrayList<>();
    report.add("MEMORY_CELL " + cell + " heap=" + heap + " rounds=" + rounds.size() + " images=" + expected.size() + " embedded="
        + admittedCount + " refused_at_prep=" + refusedAtPrep + " refused_at_compute=" + refusedAtCompute + " rows=" + rows.size()
        + " heap_pool_peaks_sum_bytes=" + peak + " seconds=" + seconds + MemoryCellJvm.gcFields(childLog, output));
    report.add("MEMORY_CELL " + cell + " image_inputs " + imageInputs + (largeSize < 0 ? "" : " large_file_bytes=" + largeSize));
    report.forEach(System.out::println);
    Files.write(reports.resolve(cell + ".txt"), report, StandardCharsets.UTF_8);
  }

  /**
   * Places {@code file} in the bucket at {@code key} (a hard link, so the copies of a large image cost no disk; a copy where
   * links are not supported), adds its ObjectCreated event to {@code events} and returns what its row must hold.
   */
  private Expected put(Path file, String key, String format, int width, int height, boolean admitted,
                       List<S3EventsTableFixture.Event> events) throws IOException {
    Path object = objects().file(key);
    Files.createDirectories(object.getParent());
    try {
      Files.createLink(object, file);
    } catch (IOException | UnsupportedOperationException e) {
      Files.copy(file, object);
    }
    events.add(S3EventsTableFixture.created(BlobJobHarness.BUCKET, key, Files.size(object), BlobJobHarness.EVENT_TIME));
    return new Expected(key, format, width, height, admitted);
  }

  /**
   * The cell's job: {@code prep_and_compute}, the image extensions by the image preprocessor (the base's text mapping of
   * {@code tif} removed; without video in the allow-list the video frame heap check does not apply), the counting multimodal
   * client, compute parallelism {@value #COMPUTE_PARALLELISM}, DISK_ONLY persists, the default balance.
   */
  private TypedProperties cellProps() {
    TypedProperties props = props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    props.setProperty(BucketFolderFileSystem.ROOT, objects().objectsRoot().toAbsolutePath().toString());
    props.remove(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + TEXT_EXTENSION + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX);
    props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), IMAGE_EXTENSIONS);
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX,
        ConcurrencyCountingMultimodalClient.class.getName());
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX,
        "text,image");
    props.setProperty(EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), String.valueOf(COMPUTE_PARALLELISM));
    props.setProperty(HoodieWriteConfig.TAGGED_RECORD_STORAGE_LEVEL_VALUE.key(), "DISK_ONLY");
    props.remove(EmbeddingTableConfig.BALANCE_MIN_PARTITIONS.key());
    return props;
  }

  /** The bytes of a {@code -Xmx} value such as {@code 576m}. */
  private static long heapBytes(String heap) {
    String value = heap.trim().toLowerCase(Locale.ROOT);
    char unit = value.charAt(value.length() - 1);
    long multiplier = unit == 'g' ? 1L << 30 : unit == 'm' ? 1L << 20 : unit == 'k' ? 1L << 10 : 1L;
    return Long.parseLong(Character.isDigit(unit) ? value : value.substring(0, value.length() - 1)) * multiplier;
  }

  private static String value(List<String> output, String prefix) {
    return output.stream().filter(line -> line.startsWith(prefix)).map(line -> line.substring(prefix.length()))
        .reduce((first, last) -> last).orElseThrow(() -> new AssertionError("no " + prefix.trim() + " line"));
  }

  /**
   * Runs {@link ImageRoundMemoryChild} at {@code heap} (one round at most beyond the expected one); an exhausted heap writes
   * {@code heapDump} once (a later attempt does not overwrite it). Its output is copied to {@code log} and returned.
   */
  private static List<String> runChild(String heap, File propsFile, Path heapDump, Path log) throws IOException, InterruptedException {
    List<String> command = new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    command.addAll(MemoryCellJvm.heapOptions(heap, log));
    command.add("-XX:+ExitOnOutOfMemoryError");
    command.add("-XX:+HeapDumpOnOutOfMemoryError");
    command.add("-XX:HeapDumpPath=" + heapDump.toAbsolutePath());
    for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (argument.startsWith("--add-opens") || argument.startsWith("--add-exports")) {
        command.add(argument);
      }
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ImageRoundMemoryChild.class.getName());
    command.add(propsFile.getAbsolutePath());
    command.add("2");
    Process child = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    if (!child.waitFor(CELL_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
      child.destroyForcibly();
      throw new AssertionError("the cell's child did not finish in " + CELL_TIMEOUT_MINUTES + " minutes (TIMEOUT), see " + log);
    }
    List<String> output = Files.readAllLines(log, StandardCharsets.UTF_8);
    output.stream().filter(line -> line.startsWith("ROUND ") || line.startsWith("IMAGE_INPUTS ")
        || line.startsWith("HEAP_POOL_PEAKS_SUM_BYTES ") || line.startsWith("RESULT ")).forEach(line -> System.out.println("child: " + line));
    System.out.println("child exit " + child.exitValue());
    // the child prints RESULT ok before stopping Spark: a shutdown that throws or exhausts the heap fails the cell too
    assertEquals(0, child.exitValue(), "the cell's child exited nonzero, see " + log.toAbsolutePath());
    return output;
  }
}
