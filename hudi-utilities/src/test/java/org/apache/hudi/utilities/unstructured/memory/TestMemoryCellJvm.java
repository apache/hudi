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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MemoryCellJvm}: the heap options make a child's heap the same whatever memory the machine has, and the G1 figures are
 * read from the child's heap log and output.
 */
public class TestMemoryCellJvm {

  private static final String HEAP = "576m";
  private static final long HEAP_BYTES = 576L << 20;

  @TempDir
  Path tempDir;

  /**
   * A JVM sized for a machine of {@code machineRam} (an {@code -XX:MaxRAM} placed before the heap options, as the machine's own
   * memory would be) starts with the cell's whole heap, on a G1 heap log next to the child's log.
   */
  @ParameterizedTest
  @ValueSource(strings = {"4g", "16g", "64g"})
  public void heapOptionsFixTheHeapWhateverTheMachineMemory(String machineRam) throws Exception {
    Path childLog = tempDir.resolve("cell-child.log");
    List<String> command = new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    command.add("-XX:MaxRAM=" + machineRam);
    command.addAll(MemoryCellJvm.heapOptions(HEAP, childLog));
    command.add("-XX:+PrintFlagsFinal");
    command.add("-version");
    Map<String, String> flags = flags(command);
    assertEquals(String.valueOf(HEAP_BYTES), flags.get("InitialHeapSize"), "the initial heap is the cell's heap");
    assertEquals(String.valueOf(HEAP_BYTES), flags.get("MaxHeapSize"), "the max heap is the cell's heap");
    assertEquals(String.valueOf(32L << 30), flags.get("MaxRAM"), "the machine memory the JVM sizes its defaults from is fixed");
    assertEquals("true", flags.get("UseG1GC"), "the child runs G1");
    Path gcLog = tempDir.resolve("cell-gc.log");
    assertEquals(gcLog, MemoryCellJvm.gcLog(childLog));
    assertTrue(new String(Files.readAllBytes(gcLog), StandardCharsets.UTF_8).toLowerCase(Locale.ROOT).contains("heap region size: 1m"),
        "the child's G1 heap log is next to its log");
  }

  /** The humongous peak is the most humongous regions at a collection's start, times the logged region size. */
  @Test
  public void humongousPeakIsTheMostRegionsAtACollectionStart() throws IOException {
    Path gcLog = write("cell-gc.log",
        "[0.004s][info][gc,init] Heap Region Size: 2M",
        "[0.105s][info][gc,heap] GC(0) Eden regions: 24->0(25)",
        "[0.105s][info][gc,heap] GC(0) Humongous regions: 7->3",
        "[0.105s][info][gc     ] GC(0) Pause Young (Normal) (G1 Evacuation Pause) 261M->80M(576M) 6.079ms",
        "[0.205s][info][gc,heap] GC(1) Humongous regions: 12->12",
        "[0.305s][info][gc,heap] GC(2) Humongous regions: 5->0");
    assertEquals(12L * (2L << 20), MemoryCellJvm.humongousPeakBytes(gcLog));
  }

  /** No collection, no humongous peak; a log without a G1 region size is not a G1 child's and fails the cell. */
  @Test
  public void humongousPeakWithoutCollectionsAndWithoutG1() throws IOException {
    assertEquals(0L, MemoryCellJvm.humongousPeakBytes(write("idle-gc.log", "[0.004s][info][gc,heap] Heap region size: 1M")));
    Path serial = write("serial-gc.log", "[0.004s][info][gc] Using Serial");
    assertThrows(AssertionError.class, () -> MemoryCellJvm.humongousPeakBytes(serial));
  }

  /** The two fields appended to a {@code MEMORY_CELL} line, the heap after collections from the child's last such line. */
  @Test
  public void gcFieldsAppendTheHumongousPeakAndTheHeapAfterCollections() throws IOException {
    write("cell-gc.log", "[0.004s][info][gc,heap] Heap region size: 1M", "[0.105s][info][gc,heap] GC(0) Humongous regions: 3->1");
    Path childLog = tempDir.resolve("cell-child.log");
    List<String> output = Arrays.asList("ROUND 1 rows=4", "HEAP_POOL_PEAKS_SUM_BYTES 900", "POST_GC_HEAP_PEAK_BYTES 400",
        "POST_GC_COUNT 2", "RESULT ok");
    assertEquals(" g1_humongous_peak_bytes=" + (3L << 20) + " heap_after_gc_max_bytes=400", MemoryCellJvm.gcFields(childLog, output));
    assertThrows(AssertionError.class, () -> MemoryCellJvm.gcFields(childLog, Collections.singletonList("RESULT ok")));
  }

  private Path write(String name, String... lines) throws IOException {
    return Files.write(tempDir.resolve(name), Arrays.asList(lines), StandardCharsets.UTF_8);
  }

  /** Runs {@code command} (a {@code -XX:+PrintFlagsFinal} JVM) and returns its flags by name. */
  private Map<String, String> flags(List<String> command) throws IOException, InterruptedException {
    Path out = tempDir.resolve("flags.txt");
    Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(out.toFile()).start();
    assertTrue(process.waitFor(1, TimeUnit.MINUTES), "the JVM printed its flags");
    assertEquals(0, process.exitValue(), "the JVM started: " + new String(Files.readAllBytes(out), StandardCharsets.UTF_8));
    Map<String, String> flags = new TreeMap<>();
    for (String line : Files.readAllLines(out, StandardCharsets.UTF_8)) {
      String[] tokens = line.trim().split("\\s+");
      if (tokens.length >= 4 && "=".equals(tokens[2])) {
        flags.put(tokens[1], tokens[3]);
      }
    }
    return flags;
  }
}
