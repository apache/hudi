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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The class-path filter of {@link NoFfmpegClassPath}: FFmpeg entries (under org/bytedeco, or a javacv / javacpp / ffmpeg jar) are
 * dropped and every other entry kept in order, and a manifest-only booter jar is read as its manifest's entries.
 */
class TestNoFfmpegClassPath {

  private static final String REPO = "/home/u/.m2/repository/";
  private static final String JAVACV = REPO + "org/bytedeco/javacv/1.5.10/javacv-1.5.10.jar";
  private static final String FFMPEG_NATIVE = REPO + "org/bytedeco/ffmpeg/6.1.1-1.5.10/ffmpeg-6.1.1-1.5.10-linux-x86_64.jar";
  private static final String JAVACPP_ELSEWHERE = "/opt/jars/javacpp-1.5.10.jar";
  private static final String FFMPEG_ELSEWHERE = "/opt/jars/ffmpeg-6.1.1-1.5.10.jar";
  private static final String JCODEC = REPO + "org/jcodec/jcodec/0.2.5/jcodec-0.2.5.jar";
  private static final String TEST_CLASSES = "/work/hudi-utilities/target/test-classes";
  private static final String SPARK = REPO + "org/apache/spark/spark-core_2.12/3.5.3/spark-core_2.12-3.5.3.jar";

  @Test
  void dropsFfmpegEntriesAndKeepsTheRestInOrder() {
    List<String> entries = Arrays.asList(TEST_CLASSES, JAVACV, JCODEC, FFMPEG_NATIVE, JAVACPP_ELSEWHERE, SPARK, FFMPEG_ELSEWHERE);
    assertEquals(Arrays.asList(TEST_CLASSES, JCODEC, SPARK), NoFfmpegClassPath.withoutFfmpeg(entries));
  }

  @Test
  void namesFfmpegEntries() {
    for (String ffmpeg : Arrays.asList(JAVACV, FFMPEG_NATIVE, JAVACPP_ELSEWHERE, FFMPEG_ELSEWHERE, "C:\\m2\\org\\bytedeco\\x\\x.jar")) {
      assertTrue(NoFfmpegClassPath.isFfmpeg(ffmpeg), ffmpeg);
    }
    // a jar that only mentions ffmpeg in its directory, or a directory named like a jar prefix, is not FFmpeg
    for (String other : Arrays.asList(JCODEC, TEST_CLASSES, SPARK, "/opt/ffmpeg-tools/lib/commons-io-2.16.1.jar", "/opt/javacv")) {
      assertFalse(NoFfmpegClassPath.isFfmpeg(other), other);
    }
  }

  @Test
  void readsAManifestOnlyBooterJarAsItsClassPath(@TempDir Path dir) throws IOException {
    Path classes = Files.createDirectories(dir.resolve("test classes"));
    Path javacv = Files.createDirectories(dir.resolve("repo/org/bytedeco/javacv")).resolve("javacv-1.5.10.jar");
    Path jcodec = dir.resolve("jcodec-0.2.5.jar");
    Path booter = dir.resolve("surefirebooter123.jar");
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH,
        classes.toUri() + " " + javacv.toUri() + " " + jcodec.getFileName());
    try (OutputStream out = new JarOutputStream(Files.newOutputStream(booter), manifest)) {
      out.flush();
    }

    List<String> entries = NoFfmpegClassPath.expand(Collections.singletonList(booter.toString()));
    assertEquals(Arrays.asList(classes.toString(), javacv.toString(), jcodec.toString()), entries);
    assertEquals(Arrays.asList(classes.toString(), jcodec.toString()), NoFfmpegClassPath.withoutFfmpeg(entries));
  }

  @Test
  void keepsAClassPathOfSeveralEntriesOrAJarWithoutManifestClassPath(@TempDir Path dir) throws IOException {
    List<String> several = Arrays.asList(TEST_CLASSES, JCODEC);
    assertEquals(several, NoFfmpegClassPath.expand(several));
    Path plain = dir.resolve("plain.jar");
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    try (OutputStream out = new JarOutputStream(Files.newOutputStream(plain), manifest)) {
      out.flush();
    }
    assertEquals(Collections.singletonList(plain.toString()), NoFfmpegClassPath.expand(Collections.singletonList(plain.toString())));
    assertEquals(several, NoFfmpegClassPath.split(TEST_CLASSES + File.pathSeparator + File.pathSeparator + JCODEC));
  }

  @Test
  void thisJvmsClassPathWithoutFfmpegKeepsTheTestClassesAndJcodec() {
    List<String> entries = NoFfmpegClassPath.testClassPathWithoutFfmpeg();
    assertTrue(entries.stream().noneMatch(NoFfmpegClassPath::isFfmpeg), entries.toString());
    assertTrue(entries.stream().anyMatch(entry -> entry.replace('\\', '/').endsWith("target/test-classes")), entries.toString());
    assertTrue(entries.stream().anyMatch(entry -> entry.contains("jcodec")), entries.toString());
  }
}
