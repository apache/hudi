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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * One video memory cell's embedding rounds, in a child JVM whose heap is the cell's heap: first prints what the video path finds
 * in this JVM, then runs {@link EmbeddingRoundMemoryChild} with the same arguments (its {@code ROUND},
 * {@code HEAP_POOL_PEAKS_SUM_BYTES}, {@link PostGcHeap}'s and {@code RESULT} lines). The lines printed first:
 * <ul>
 *   <li>{@code CHILD_MAX_HEAP_BYTES <n>}: this JVM's max heap, the executor heap the video frame heap start check reads in local
 *   mode;</li>
 *   <li>{@code FFMPEG_ON_CLASS_PATH <true|false>}: whether FFmpeg's JavaCV classes load here;</li>
 *   <li>{@code FFMPEG_PROGRAM_ON_PATH <paths|none>}: every {@code ffmpeg} executable on this JVM's {@code PATH} (the parent removes
 *   their folders, so a round that finishes needed no {@code ffmpeg} program).</li>
 * </ul>
 * At exit it prints {@code VIDEO_DECODES max_in_flight=<n> tasks=<partition>=<decodes>,...} ({@link VideoDecodeConcurrency}): how
 * many decodes of the cell's counting preprocessor ran at once, and in which tasks.
 */
public final class VideoRoundMemoryChild {

  /** Classes of the FFmpeg jars (javacv, ffmpeg). */
  static final List<String> FFMPEG_CLASSES = Arrays.asList("org.bytedeco.javacv.FFmpegFrameGrabber", "org.bytedeco.ffmpeg.global.avcodec");
  /** The name of the FFmpeg command line program. */
  static final String FFMPEG_PROGRAM = "ffmpeg";

  private VideoRoundMemoryChild() {
  }

  public static void main(String[] args) throws Exception {
    System.out.println("CHILD_MAX_HEAP_BYTES " + Runtime.getRuntime().maxMemory());
    System.out.println("FFMPEG_ON_CLASS_PATH " + ffmpegOnClassPath());
    List<String> programs = ffmpegPrograms(System.getenv("PATH"));
    System.out.println("FFMPEG_PROGRAM_ON_PATH " + (programs.isEmpty() ? "none" : String.join(",", programs)));
    // the delegate ends the JVM with System.exit, which runs this hook (an exhausted heap does not, and fails the cell anyway)
    Runtime.getRuntime().addShutdownHook(new Thread(() -> System.out.println(VideoDecodeConcurrency.report())));
    EmbeddingRoundMemoryChild.main(args);
  }

  private static boolean ffmpegOnClassPath() {
    for (String ffmpegClass : FFMPEG_CLASSES) {
      try {
        Class.forName(ffmpegClass, false, VideoRoundMemoryChild.class.getClassLoader());
        return true;
      } catch (ClassNotFoundException e) {
        // not on this class path
      }
    }
    return false;
  }

  /** Every executable {@value #FFMPEG_PROGRAM} in the folders of {@code path} (a {@code PATH} value, may be null). */
  static List<String> ffmpegPrograms(String path) {
    List<String> programs = new ArrayList<>();
    if (path == null) {
      return programs;
    }
    for (String folder : path.split(File.pathSeparator)) {
      if (folder.isEmpty()) {
        continue;
      }
      Path program = Paths.get(folder, FFMPEG_PROGRAM);
      if (Files.isRegularFile(program) && Files.isExecutable(program)) {
        programs.add(program.toString());
      }
    }
    return programs;
  }
}
