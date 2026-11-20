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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The heap options every memory cell's child JVM starts with, and the G1 figures its report adds.
 *
 * <p>Left to its ergonomics, a JVM sizes its initial heap from the memory it sees (1/64 of it), so the same cell at the same
 * {@code -Xmx} starts with a 256 MiB heap on a 16 GiB machine and with its whole heap on a 64 GiB one, and its
 * {@code HEAP_POOL_PEAKS_SUM_BYTES} follows the machine more than the code. {@link #heapOptions} fixes the heap instead:
 * {@code -Xms} equal to {@code -Xmx}, so the heap is the cell's heap from the start on every machine and G1 never resizes it;
 * {@code -XX:MaxRAM}={@value #MAX_RAM}, so whatever else the JVM derives from the machine's memory is the same everywhere too;
 * G1, which the JVM would otherwise choose only on a machine it deems a server; and a G1 heap log next to the child's log.
 *
 * <p>{@link #gcFields} reads two figures back for the cell's {@code MEMORY_CELL} line, both to tell transient garbage from held
 * heap (the sum of the pools' peaks counts both):
 * <ul>
 *   <li>{@code g1_humongous_peak_bytes}: the most humongous regions (objects of half a region or more, allocated straight into
 *   the old generation) the log counts at the start of a collection, times the region size;</li>
 *   <li>{@code heap_after_gc_max_bytes}: the most heap held after a collection ({@link PostGcHeap}'s
 *   {@code POST_GC_HEAP_PEAK_BYTES}).</li>
 * </ul>
 */
final class MemoryCellJvm {

  /** The machine memory every child's JVM sizes its defaults from. */
  static final String MAX_RAM = "32g";

  /** The region size line: {@code [gc,heap] Heap region size: 1M} on Java 11, {@code [gc,init] Heap Region Size: 1M} on 17. */
  private static final Pattern REGION_SIZE = Pattern.compile("Heap region size: (\\d+)([KMG])", Pattern.CASE_INSENSITIVE);
  private static final Pattern HUMONGOUS = Pattern.compile("Humongous regions: (\\d+)->");
  private static final String POST_GC_HEAP_PEAK = "POST_GC_HEAP_PEAK_BYTES ";

  private MemoryCellJvm() {
  }

  /**
   * The child's heap options at {@code heap} (an {@code -Xmx} value such as {@code 576m}), with its G1 heap log at
   * {@link #gcLog}{@code (childLog)}.
   */
  static List<String> heapOptions(String heap, Path childLog) {
    return Arrays.asList("-Xms" + heap, "-Xmx" + heap, "-XX:MaxRAM=" + MAX_RAM, "-XX:+UseG1GC",
        "-Xlog:gc,gc+heap,gc+init:file=" + gcLog(childLog).toAbsolutePath() + ":uptime,level,tags:filecount=0");
  }

  /** The G1 heap log of the child whose log is {@code childLog} ({@code <run>-child.log}): {@code <run>-gc.log} next to it. */
  static Path gcLog(Path childLog) {
    return childLog.resolveSibling(childLog.getFileName().toString().replace("-child.log", "") + "-gc.log");
  }

  /**
   * {@code " g1_humongous_peak_bytes=<n> heap_after_gc_max_bytes=<n>"}, appended to a {@code MEMORY_CELL} line: the first from
   * the G1 heap log of the child whose log is {@code childLog}, the second from the child's {@code output}.
   */
  static String gcFields(Path childLog, List<String> output) throws IOException {
    return " g1_humongous_peak_bytes=" + humongousPeakBytes(gcLog(childLog)) + " heap_after_gc_max_bytes=" + output.stream()
        .filter(line -> line.startsWith(POST_GC_HEAP_PEAK)).map(line -> line.substring(POST_GC_HEAP_PEAK.length()))
        .reduce((first, last) -> last).orElseThrow(() -> new AssertionError("no " + POST_GC_HEAP_PEAK.trim() + " line"));
  }

  /** The most humongous regions {@code gcLog} counts at the start of a collection (0 without one), times the region size. */
  static long humongousPeakBytes(Path gcLog) throws IOException {
    String log = new String(Files.readAllBytes(gcLog), StandardCharsets.UTF_8);
    Matcher size = REGION_SIZE.matcher(log);
    if (!size.find()) {
      throw new AssertionError("no G1 region size in " + gcLog);
    }
    long regionBytes = Long.parseLong(size.group(1)) << ("K".equalsIgnoreCase(size.group(2)) ? 10 : "M".equalsIgnoreCase(size.group(2)) ? 20 : 30);
    long regions = 0;
    Matcher humongous = HUMONGOUS.matcher(log);
    while (humongous.find()) {
      regions = Math.max(regions, Long.parseLong(humongous.group(1)));
    }
    return regions * regionBytes;
  }
}
