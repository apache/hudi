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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The scan's chosen timestamp schedule has bounded heap use independently of video length. */
class TestFfmpegTimestampSchedule {
  @TempDir
  static Path directory;
  private static Path clip;

  @BeforeAll
  static void clip() throws Exception {
    clip = VideoClips.generate(directory.resolve("clip.mp4"), 1, 1, 1, 64, 64, -1, -1);
  }

  @Test
  void smallTimestampScheduleControl() throws Exception {
    child(1000, true);
  }

  @Test
  void largeTimestampScheduleFitsTheSameHeap() throws Exception {
    child(5_000_000, true);
  }

  @Test
  void earlyCloseRemovesAnyTemporaryTimestampSchedule() throws Exception {
    child(1000, false);
  }

  private static void child(int count, boolean consume) throws Exception {
    Path work = Files.createDirectory(directory.resolve("schedule-" + count + "-" + consume));
    Path log = work.resolve("child.log");
    Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Xms64m", "-Xmx64m", "-XX:+UseG1GC", "-XX:ActiveProcessorCount=2", "-XX:-ExplicitGCInvokesConcurrent",
        "-XX:-HeapDumpOnOutOfMemoryError", "-Djava.io.tmpdir=" + work,
        "-Dorg.bytedeco.javacpp.cachedir=" + System.getProperty("org.bytedeco.javacpp.cachedir", directory.resolve("native-cache").toString()),
        "-cp", System.getProperty("java.class.path"), Child.class.getName(), clip.toString(), Integer.toString(count),
        Boolean.toString(consume)).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    if (!child.waitFor(120, TimeUnit.SECONDS)) {
      child.destroyForcibly();
      child.waitFor();
      throw new AssertionError("timestamp child did not finish: " + Files.readString(log));
    }
    String output = Files.readString(log);
    output.lines().filter(line -> line.startsWith("TIMESTAMP_SCHEDULE")).forEach(System.out::println);
    assertEquals(0, child.exitValue(), output);
    assertTrue(output.contains("TIMESTAMP_SCHEDULE count=" + count + " consume=" + consume), output);
    try (Stream<Path> files = Files.list(work)) {
      assertEquals(Collections.singletonList(log), files.sorted().collect(Collectors.toList()),
          "all temporary timestamp files are removed on exhaustion and early close");
    }
  }

  /** Exercises the real scan accumulation without generating millions of media packets or decoding their frames. */
  public static final class Child {
    public static void main(String[] args) throws Exception {
      long heap = Runtime.getRuntime().maxMemory();
      if (heap != 64L * 1024 * 1024) {
        throw new AssertionError("unexpected heap " + heap);
      }
      System.out.println("TIMESTAMP_SCHEDULE max_heap_bytes=" + heap);
      Path clip = Path.of(args[0]);
      int count = Integer.parseInt(args[1]);
      boolean consume = Boolean.parseBoolean(args[2]);
      try (ClosableIterator<ExtractedFrame> frames = new FfmpegKeyframeVideoFrameSource().frames(VideoClips.supplier(clip),
          new FileMeta("schedule", "mp4", Files.size(clip)), new TypedProperties())) {
        Method choose = frames.getClass().getDeclaredMethod("choose", List.class, long.class, long.class);
        choose.setAccessible(true);
        long baseline = retainedHeap();
        for (int i = 0; i < count; i++) {
          long relative = i * 1000L;
          choose.invoke(frames, Collections.singletonList(relative), relative, relative + 9);
        }
        long retained = retainedHeap() - baseline;
        System.out.println("TIMESTAMP_SCHEDULE retained_schedule_bytes=" + retained);
        if (retained > 4L * 1024 * 1024) {
          throw new AssertionError("timestamp schedule retained " + retained + " bytes above its baseline");
        }
        if (consume) {
          Field scanned = frames.getClass().getDeclaredField("scanned");
          scanned.setAccessible(true);
          scanned.set(frames, true);
          Method next = frames.getClass().getDeclaredMethod("nextChosen");
          next.setAccessible(true);
          for (int i = 0; i < count; i++) {
            long[] pair = (long[]) next.invoke(frames);
            if (pair == null || pair[0] != i * 1000L || pair[1] != i * 1000L + 9) {
              throw new AssertionError("timestamp pair changed at " + i);
            }
          }
          long[] end = (long[]) next.invoke(frames);
          if (end.length != 0) {
            throw new AssertionError("unexpected timestamp after the schedule ended");
          }
        }
      }
      System.out.println("TIMESTAMP_SCHEDULE count=" + count + " consume=" + consume);
    }

    private static long retainedHeap() {
      System.gc();
      return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }
  }
}
