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

import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestFfmpegLibraryVersions {
  private static final String JAVACV_CLASS = "org/bytedeco/javacv/FFmpegFrameGrabber.class";
  private static final String FFMPEG_CLASS = "org/bytedeco/ffmpeg/global/avutil.class";

  @TempDir
  Path temp;

  /** The declared versions are the ones this build compiles and tests against (a pom bump without them fails here). */
  @Test
  void declaredVersionsAreTheJarsThisBuildIsTestedWith() {
    ClassLoader tested = getClass().getClassLoader();
    assertEquals(FfmpegLibraryVersions.JAVACV, FfmpegLibraryVersions.loaded(tested, JAVACV_CLASS, FfmpegLibraryVersions.JAVACV));
    assertEquals(FfmpegLibraryVersions.FFMPEG, FfmpegLibraryVersions.loaded(tested, FFMPEG_CLASS, FfmpegLibraryVersions.FFMPEG));
    assertDoesNotThrow(() -> FfmpegLibraryVersions.requireDeclared(tested));
  }

  @Test
  void executorJarsOfTheDeclaredVersionsAreAccepted() throws IOException {
    try (URLClassLoader executor = executor(jar("javacv", JAVACV_CLASS, "org.bytedeco/javacv", "1.5.11"),
        jar("ffmpeg", FFMPEG_CLASS, "org.bytedeco/ffmpeg", "7.1-1.5.11"))) {
      assertDoesNotThrow(() -> FfmpegLibraryVersions.requireDeclared(executor));
    }
  }

  @Test
  void executorJarOfAnotherVersionIsRefusedNamingExpectedAndFound() throws IOException {
    try (URLClassLoader executor = executor(jar("javacv", JAVACV_CLASS, "org.bytedeco/javacv", "1.5.10"),
        jar("ffmpeg", FFMPEG_CLASS, "org.bytedeco/ffmpeg", "7.1-1.5.11"))) {
      PreprocessorConfigurationException refused = assertThrows(PreprocessorConfigurationException.class,
          () -> FfmpegLibraryVersions.requireDeclared(executor));
      assertEquals("The FFmpeg libraries on this executor are not the versions this Hudi build declares and its prep logic"
          + " was verified with (expected org.bytedeco/javacv:1.5.11, found org.bytedeco/javacv:1.5.10); add the declared"
          + " versions' jars to the job; nothing was prepared with FFmpeg", refused.getMessage());
    }
  }

  @Test
  void executorJarWithoutVersionMetadataIsRefused() throws IOException {
    try (URLClassLoader executor = executor(jar("javacv", JAVACV_CLASS, null, null),
        jar("ffmpeg", FFMPEG_CLASS, "org.bytedeco/ffmpeg", "7.1-1.5.11"))) {
      PreprocessorConfigurationException refused = assertThrows(PreprocessorConfigurationException.class,
          () -> FfmpegLibraryVersions.requireDeclared(executor));
      assertTrue(refused.getMessage().contains("(expected org.bytedeco/javacv:1.5.11, found no org.bytedeco/javacv"
          + " Maven metadata in "), refused.getMessage());
    }
  }

  @Test
  void repackagedJarUsesItsImplementationVersion() throws IOException {
    Path jar = Files.createTempFile(temp, "repackaged-javacv-", ".jar");
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VERSION, "1.5.11");
    try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar), manifest)) {
      out.putNextEntry(new JarEntry(JAVACV_CLASS));
      out.closeEntry();
    }
    try (URLClassLoader executor = executor(jar, jar("ffmpeg", FFMPEG_CLASS, "org.bytedeco/ffmpeg", "7.1-1.5.11"))) {
      assertDoesNotThrow(() -> FfmpegLibraryVersions.requireDeclared(executor));
    }
  }

  @Test
  void executorWithoutTheJarsIsRefused() throws IOException {
    try (URLClassLoader executor = executor(jar("ffmpeg", FFMPEG_CLASS, "org.bytedeco/ffmpeg", "7.1-1.5.11"))) {
      PreprocessorConfigurationException refused = assertThrows(PreprocessorConfigurationException.class,
          () -> FfmpegLibraryVersions.requireDeclared(executor));
      assertTrue(refused.getMessage().contains("(expected org.bytedeco/javacv:1.5.11, found absent)"), refused.getMessage());
    }
  }

  /** Only these jars: no parent delegation to the test class path, which carries the real FFmpeg jars. */
  private static URLClassLoader executor(Path... jars) throws IOException {
    URL[] urls = new URL[jars.length];
    for (int i = 0; i < jars.length; i++) {
      urls[i] = jars[i].toUri().toURL();
    }
    return new URLClassLoader(urls, null);
  }

  /** A jar holding {@code resource} and, when {@code artifact} is set, the Maven metadata a released jar carries. */
  private Path jar(String name, String resource, String artifact, String version) throws IOException {
    Path jar = Files.createTempFile(temp, name + "-", ".jar");
    try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
      out.putNextEntry(new JarEntry(resource));
      out.closeEntry();
      if (artifact != null) {
        out.putNextEntry(new JarEntry("META-INF/maven/" + artifact + "/pom.properties"));
        out.write(("version=" + version + "\n").getBytes(StandardCharsets.UTF_8));
        out.closeEntry();
      }
    }
    return jar;
  }
}
