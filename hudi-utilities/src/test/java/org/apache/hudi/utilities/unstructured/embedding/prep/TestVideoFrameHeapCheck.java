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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The frame-heap start check of a job that preps video:
 * {@code slots × (6 · maxPixels + 16 MiB + 10 MiB) + reserve ≤ heap − reserved − fixed} (10 MiB: block reads and out-of-line writes), the read ahead admitted only where it fits,
 * heap and slots as the driver reads them, reserve the per-executor contiguity reserve of whole G1 regions.
 */
class TestVideoFrameHeapCheck {

  private static final long MIB = 1024L * 1024;

  /**
   * With the reserve: at 1080p k = 4 (slot term ≈ 37.9 MiB, reserve 4 x 6 regions of 1 MiB) needs 540 MiB with 300 MiB reserved and
   * 64 MiB fixed; at 2160p (reserve 4 x 24 regions of 1 MiB) it needs 754 MiB.
   */
  @Test
  void designAdmissionExamplesHold() {
    TypedProperties hd = props(1920L * 1080);
    // 4 x (12 441 600 + 16 MiB + 10 MiB) + 4 x 6 x 1 MiB = 183 984 128 <= 540 MiB - 300 MiB - 64 MiB = 184 549 376
    EmbeddingTableConfig hdConfig = EmbeddingTableConfig.from(hd);
    SparkConf cluster540m = cluster(4, "540m");
    assertDoesNotThrow(() -> VideoFrameHeapCheck.check(hdConfig, cluster540m));
    SparkConf cluster539m = cluster(4, "539m");
    assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(hdConfig, cluster539m));
    // the default 3840x2160 needs 4 x (49 766 400 + 16 MiB + 10 MiB) + 4 x 24 x 1 MiB = 408 780 800 bytes: refused at 753 MiB, admitted at 754
    TypedProperties uhd = props(EmbeddingTableConfig.DEFAULT_PREP_VIDEO_MAX_PIXELS);
    EmbeddingTableConfig uhdConfig = EmbeddingTableConfig.from(uhd);
    SparkConf cluster476m = cluster(4, "476m");
    HoodieValidationException refused = assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(uhdConfig, cluster476m));
    assertTrue(refused.getMessage().contains("4 task slots x (6 x 8294400 pixels") && refused.getMessage().contains(
        EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key()) && refused.getMessage().contains(EmbeddingTableConfig.PREP_VIDEO_HEAP_FIXED_BYTES.key()),
        refused.getMessage());
    SparkConf cluster753m = cluster(4, "753m");
    assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(uhdConfig, cluster753m));
    SparkConf cluster754m = cluster(4, "754m");
    assertDoesNotThrow(() -> VideoFrameHeapCheck.check(uhdConfig, cluster754m));
  }

  /**
   * The read ahead never changes admission: at the default 8 MiB, 1080p k = 4 is admitted at 540 MiB as without it, but reads
   * nothing ahead there (its two buffers per slot do not fit beside the frame decodes); from 604 MiB it reads 8 MiB ahead. A refusal
   * is the one without a read ahead, naming no read-ahead key.
   */
  @Test
  void theReadAheadRunsOnlyWhereItsBuffersFit() {
    TypedProperties hd = props(1920L * 1080);
    hd.remove(EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key());
    List<String> noArgs = Collections.emptyList();
    EmbeddingTableConfig hdConfig = EmbeddingTableConfig.from(hd);
    SparkConf cluster540m = cluster(4, "540m");
    assertDoesNotThrow(() -> VideoFrameHeapCheck.check(hdConfig, cluster540m));
    assertEquals(0L, VideoFrameHeapCheck.admittedPrefetchBytes(EmbeddingTableConfig.from(hd), cluster(4, "540m"), noArgs));
    // 4 x (12 441 600 + 16 MiB + 10 MiB + 2 x 8 MiB) + 4 x 6 x 1 MiB = 251 092 992 <= 604 MiB - 300 MiB - 64 MiB = 251 658 240
    assertEquals(0L, VideoFrameHeapCheck.admittedPrefetchBytes(EmbeddingTableConfig.from(hd), cluster(4, "603m"), noArgs));
    assertEquals(8L * MIB, VideoFrameHeapCheck.admittedPrefetchBytes(EmbeddingTableConfig.from(hd), cluster(4, "604m"), noArgs));
    SparkConf cluster539m = cluster(4, "539m");
    HoodieValidationException refused = assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(hdConfig, cluster539m));
    assertFalse(refused.getMessage().contains(EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key()), refused.getMessage());
    hd.setProperty(EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key(), "0");
    assertEquals(0L, VideoFrameHeapCheck.admittedPrefetchBytes(EmbeddingTableConfig.from(hd), cluster(4, "4g"), noArgs));
  }

  /** 2160p at 2 slots, nothing reserved: 2 x (49 766 400 + 16 MiB + 10 MiB) + 4 x 24 x 1 MiB = 254 722 048 bytes beside 64 MiB fixed. */
  @Test
  void twoSlotsAt192MiBAreRefusedAt2160p() {
    TypedProperties uhd = props(EmbeddingTableConfig.DEFAULT_PREP_VIDEO_MAX_PIXELS);
    EmbeddingTableConfig uhdConfig = EmbeddingTableConfig.from(uhd);
    SparkConf noReserved192m = clusterNoReserved(2, "192m");
    HoodieValidationException refused = assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(uhdConfig, noReserved192m));
    assertTrue(refused.getMessage().contains("reserve 4 frame rasters (3 x 8294400 bytes each) in 24 regions each x 1048576 (G1 region, G1 default for the heap) = 100663296"), refused.getMessage());
    SparkConf noReserved307m = clusterNoReserved(2, "307m");
    assertDoesNotThrow(() -> VideoFrameHeapCheck.check(uhdConfig, noReserved307m));
    // with an allow-list naming a video extension the refusal does not offer the allow-list
    uhd.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "mp4");
    HoodieValidationException listed = assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(uhdConfig, noReserved192m));
    assertFalse(listed.getMessage().contains(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key()), listed.getMessage());
    SparkConf noReserved306m = clusterNoReserved(2, "306m");
    assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(uhdConfig, noReserved306m));
  }

  /** 2160p at 1 slot, nothing reserved: 49 766 400 + 16 MiB + 10 MiB + 4 x 24 x 1 MiB = 177 692 672 bytes beside 64 MiB fixed. */
  @Test
  void oneSlotAt128MiBIsRefusedAt2160p() {
    TypedProperties uhd = props(EmbeddingTableConfig.DEFAULT_PREP_VIDEO_MAX_PIXELS);
    EmbeddingTableConfig uhdConfig = EmbeddingTableConfig.from(uhd);
    SparkConf noReserved128m = clusterNoReserved(1, "128m");
    assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(uhdConfig, noReserved128m));
    SparkConf noReserved234m = clusterNoReserved(1, "234m");
    assertDoesNotThrow(() -> VideoFrameHeapCheck.check(uhdConfig, noReserved234m));
    SparkConf noReserved233m = clusterNoReserved(1, "233m");
    assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(uhdConfig, noReserved233m));
  }

  @Test
  void theFixedTermIsConfigurable() {
    TypedProperties props = props(1920L * 1080);
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_HEAP_FIXED_BYTES.key(), String.valueOf(200 * MIB));
    EmbeddingTableConfig propsConfig = EmbeddingTableConfig.from(props);
    SparkConf cluster476m = cluster(4, "476m");
    assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(propsConfig, cluster476m));
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_HEAP_FIXED_BYTES.key(), "-1");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> VideoFrameHeapCheck.check(propsConfig, cluster476m));
    assertTrue(e.getMessage().contains(EmbeddingTableConfig.PREP_VIDEO_HEAP_FIXED_BYTES.key()), e.getMessage());
    assertEquals(EmbeddingTableConfig.DEFAULT_PREP_VIDEO_HEAP_FIXED_BYTES, 64 * MIB);
  }

  /** A pixel limit large enough to overflow the slot term or the reserve is refused, never admitted by a wrapped product. */
  @Test
  void anOverflowingPixelLimitIsRefused() {
    TypedProperties huge = props(Long.MAX_VALUE / 4);
    EmbeddingTableConfig hugeConfig = EmbeddingTableConfig.from(huge);
    SparkConf cluster64g = cluster(4, "64g");
    assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(hugeConfig, cluster64g));
    SparkConf oneMibRegions = cluster(4, "64g").set("spark.executor.extraJavaOptions", "-XX:G1HeapRegionSize=1m");
    assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(hugeConfig, oneMibRegions));
    assertEquals(Long.MAX_VALUE, VideoFrameHeapCheck.humongousReserveBytes(Long.MAX_VALUE / 4, MIB));
  }

  /** G1's ergonomic region size on JDK 17. */
  @Test
  void regionSizeFollowsTheHeap() {
    long gib = 1024 * MIB;
    assertEquals(MIB, VideoFrameHeapCheck.g1RegionBytes(256 * MIB, Collections.emptyList()));
    assertEquals(MIB, VideoFrameHeapCheck.g1RegionBytes(2 * gib, Collections.emptyList()));
    assertEquals(2 * MIB, VideoFrameHeapCheck.g1RegionBytes(3 * gib, Collections.emptyList()));
    assertEquals(2 * MIB, VideoFrameHeapCheck.g1RegionBytes(4 * gib, Collections.emptyList()));
    assertEquals(4 * MIB, VideoFrameHeapCheck.g1RegionBytes(6 * gib, Collections.emptyList()));
    assertEquals(16 * MIB, VideoFrameHeapCheck.g1RegionBytes(32 * gib, Collections.emptyList()));
    assertEquals(32 * MIB, VideoFrameHeapCheck.g1RegionBytes(64 * gib, Collections.emptyList()));
    assertEquals(32 * MIB, VideoFrameHeapCheck.g1RegionBytes(128 * gib, Collections.emptyList()));
  }

  @Test
  void anExplicitRegionSizeWinsLastOccurrence() {
    assertEquals(8 * MIB, VideoFrameHeapCheck.g1RegionBytes(256 * MIB,
        Arrays.asList("-Xss1m", "-XX:G1HeapRegionSize=4m", "-XX:+UseG1GC", "-XX:G1HeapRegionSize=8M")));
    assertEquals(2 * MIB, VideoFrameHeapCheck.g1RegionBytes(256 * MIB, Collections.singletonList("-XX:G1HeapRegionSize=2097152")));
    List<String> malformedRegion = Collections.singletonList("-XX:G1HeapRegionSize=abc");
    IllegalArgumentException malformed = assertThrows(IllegalArgumentException.class, () -> VideoFrameHeapCheck.g1RegionBytes(256 * MIB, malformedRegion));
    assertTrue(malformed.getMessage().contains("-XX:G1HeapRegionSize=abc"), malformed.getMessage());
    // on a cluster the message names the Spark key the option came from
    TypedProperties hd = props(1920L * 1080);
    EmbeddingTableConfig hdConfig = EmbeddingTableConfig.from(hd);
    SparkConf malformedOption = cluster(4, "4g").set("spark.executor.extraJavaOptions", "-XX:G1HeapRegionSize=abc");
    IllegalArgumentException fromConf = assertThrows(IllegalArgumentException.class, () -> VideoFrameHeapCheck.check(hdConfig, malformedOption));
    assertTrue(fromConf.getMessage().contains("spark.executor.extraJavaOptions"), fromConf.getMessage());
    // spark.executor.extraJavaOptions is read after spark.executor.defaultJavaOptions, so its region size wins
    TypedProperties uhd = props(EmbeddingTableConfig.DEFAULT_PREP_VIDEO_MAX_PIXELS);
    EmbeddingTableConfig uhdConfig = EmbeddingTableConfig.from(uhd);
    SparkConf bothOptions = clusterNoReserved(1, "128m").set("spark.executor.defaultJavaOptions", "-XX:G1HeapRegionSize=4m")
        .set("spark.executor.extraJavaOptions", "-Xss1m -XX:G1HeapRegionSize=8m");
    HoodieValidationException refused = assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(uhdConfig, bothOptions));
    assertTrue(refused.getMessage().contains("reserve 4 frame rasters (3 x 8294400 bytes each) in 3 regions each x 8388608 (G1 region, "
        + "-XX:G1HeapRegionSize in spark.executor.extraJavaOptions) = 100663296"), refused.getMessage());
    // a size given only in spark.executor.defaultJavaOptions is used, and the message names that key
    SparkConf defaultOptions = clusterNoReserved(1, "128m").set("spark.executor.defaultJavaOptions", "-XX:G1HeapRegionSize=4m");
    HoodieValidationException fromDefault = assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(uhdConfig, defaultOptions));
    assertTrue(fromDefault.getMessage().contains("in 6 regions each x 4194304 (G1 region, -XX:G1HeapRegionSize in "
        + "spark.executor.defaultJavaOptions) = 100663296"), fromDefault.getMessage());
  }

  /** The JVM rounds an explicit region size up to a power of two and clamps it to [1 MiB, 512 MiB]; =0 lets G1 choose. */
  @Test
  void anExplicitRegionSizeIsRoundedLikeTheJvm() {
    assertEquals(4 * MIB, VideoFrameHeapCheck.g1RegionBytes(256 * MIB, Collections.singletonList("-XX:G1HeapRegionSize=3m")));
    assertEquals(MIB, VideoFrameHeapCheck.g1RegionBytes(256 * MIB, Collections.singletonList("-XX:G1HeapRegionSize=512k")));
    assertEquals(512 * MIB, VideoFrameHeapCheck.g1RegionBytes(256 * MIB, Collections.singletonList("-XX:G1HeapRegionSize=1g")));
    assertEquals(512 * MIB, VideoFrameHeapCheck.g1RegionBytes(256 * MIB, Collections.singletonList("-XX:G1HeapRegionSize=513m")));
    assertEquals(2 * MIB, VideoFrameHeapCheck.g1RegionBytes(4 * 1024 * MIB, Collections.singletonList("-XX:G1HeapRegionSize=0")));
    // the check uses the rounded size: 3m counts 4 MiB regions, 6 per 2160p raster
    TypedProperties uhd = props(EmbeddingTableConfig.DEFAULT_PREP_VIDEO_MAX_PIXELS);
    EmbeddingTableConfig uhdConfig = EmbeddingTableConfig.from(uhd);
    SparkConf roundedRegion = clusterNoReserved(1, "128m").set("spark.executor.extraJavaOptions", "-XX:G1HeapRegionSize=3m");
    HoodieValidationException refused = assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(uhdConfig, roundedRegion));
    assertTrue(refused.getMessage().contains("in 6 regions each x 4194304 (G1 region"), refused.getMessage());
  }

  @Test
  void noReserveBelowHalfARegion() {
    assertEquals(0L, VideoFrameHeapCheck.humongousReserveBytes(1920L * 1080, 16 * MIB));
    assertEquals(4 * 24 * MIB, VideoFrameHeapCheck.humongousReserveBytes(3840L * 2160, MIB));
    assertEquals(4 * 32 * MIB, VideoFrameHeapCheck.humongousReserveBytes(3840L * 2160, 32 * MIB));
    // 1080p k = 4 with 16 MiB regions carries no reserve, so it is admitted at the base 516 MiB
    TypedProperties hd = props(1920L * 1080);
    EmbeddingTableConfig hdConfig = EmbeddingTableConfig.from(hd);
    SparkConf sixteenMibRegions = cluster(4, "516m").set("spark.executor.extraJavaOptions", "-XX:G1HeapRegionSize=16m");
    assertDoesNotThrow(() -> VideoFrameHeapCheck.check(hdConfig, sixteenMibRegions));
  }

  /**
   * In local mode the executor is this JVM, so its region size comes from this JVM's launch arguments, never from a Spark key.
   * The fixed term is this JVM's whole heap so that the run is refused and the message shows the region it used.
   */
  @Test
  void localModeReadsThisJvmsArguments() {
    TypedProperties uhd = props(EmbeddingTableConfig.DEFAULT_PREP_VIDEO_MAX_PIXELS);
    uhd.setProperty(EmbeddingTableConfig.PREP_VIDEO_HEAP_FIXED_BYTES.key(), String.valueOf(Runtime.getRuntime().maxMemory()));
    SparkConf local = new SparkConf(false).set("spark.master", "local[2]").set("spark.testing.reservedMemory", "0");
    EmbeddingTableConfig uhdConfig = EmbeddingTableConfig.from(uhd);
    List<String> explicitRegion = Collections.singletonList("-XX:G1HeapRegionSize=32m");
    HoodieValidationException explicit = assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(uhdConfig, local, explicitRegion));
    assertTrue(explicit.getMessage().contains("in 1 regions each x 33554432 (G1 region, -XX:G1HeapRegionSize in this JVM) = 134217728"), explicit.getMessage());
    SparkConf driverOptions = local.clone().set("spark.driver.extraJavaOptions", "-XX:G1HeapRegionSize=32m");
    List<String> noArgs = Collections.emptyList();
    HoodieValidationException ergonomic = assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(uhdConfig, driverOptions, noArgs));
    assertFalse(ergonomic.getMessage().contains("x 33554432"), ergonomic.getMessage());
    assertTrue(ergonomic.getMessage().contains("(G1 region, G1 default for the heap)"), ergonomic.getMessage());
    // nor from spark.executor.extraJavaOptions, which reaches only cluster executors
    SparkConf executorOptions = local.clone().set("spark.executor.extraJavaOptions", "-XX:G1HeapRegionSize=32m");
    HoodieValidationException executorIgnored = assertThrows(HoodieValidationException.class, () -> VideoFrameHeapCheck.check(uhdConfig, executorOptions, noArgs));
    assertFalse(executorIgnored.getMessage().contains("x 33554432"), executorIgnored.getMessage());
    assertTrue(executorIgnored.getMessage().contains("(G1 region, G1 default for the heap)"), executorIgnored.getMessage());
  }

  /** The check runs when an extension the job preps maps to a video preprocessor: every mapped one without an allow-list. */
  @Test
  void jobPrepsVideoWhenAPreppedExtensionMapsToAVideoPreprocessor() {
    assertTrue(prepsVideo(props(1920L * 1080)));
    TypedProperties documents = props(1920L * 1080);
    documents.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "pdf,png");
    assertFalse(prepsVideo(documents));
    TypedProperties withMp4 = props(1920L * 1080);
    withMp4.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "pdf,MP4");
    assertTrue(prepsVideo(withMp4));
    // the preprocessor's kind decides, not the extension's name
    TypedProperties remapped = props(1920L * 1080);
    remapped.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "mp4,pdf");
    remapped.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "mp4" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        DummyPdfPreprocessor.class.getName());
    assertFalse(prepsVideo(remapped));
    remapped.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "pdf" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        TestPreprocessingIteratorSpeech.ScriptedVideo.class.getName());
    assertTrue(prepsVideo(remapped));
  }

  private static boolean prepsVideo(TypedProperties props) {
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    try {
      return VideoFrameHeapCheck.prepsVideo(EmbeddingTableConfig.from(props), registry);
    } finally {
      registry.close();
    }
  }

  /** A job at {@code maxPixels}, with the default read ahead: it never changes admission. */
  private static TypedProperties props(long maxPixels) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), "/embedding");
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key(), String.valueOf(maxPixels));
    return props;
  }

  /** A cluster executor: {@code cores} task slots and {@code memory} of heap, Spark's default 300 MiB reserved. */
  private static SparkConf cluster(int cores, String memory) {
    return new SparkConf(false).set("spark.master", "yarn").set("spark.executor.cores", String.valueOf(cores))
        .set("spark.executor.memory", memory);
  }

  /** {@link #cluster} with Spark's reserved memory set to 0, so the heap is the only term the test moves. */
  private static SparkConf clusterNoReserved(int cores, String memory) {
    return cluster(cores, memory).set("spark.testing.reservedMemory", "0");
  }
}
