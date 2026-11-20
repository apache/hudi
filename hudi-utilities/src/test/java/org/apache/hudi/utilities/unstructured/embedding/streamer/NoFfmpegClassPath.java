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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

/**
 * The test JVM's class path without FFmpeg (org.bytedeco): what a job that runs without the FFmpeg jars sees. The entries are
 * surefire's test class path when it sets one, else {@code java.class.path}; a class path that is one manifest-only jar (surefire's
 * booter) is that jar's manifest {@code Class-Path}. An FFmpeg entry is one under an {@code org/bytedeco} directory of a Maven
 * repository, or a jar whose name starts with {@code javacv}, {@code javacpp} or {@code ffmpeg}.
 */
public final class NoFfmpegClassPath {

  private static final List<String> FFMPEG_JAR_PREFIXES = Collections.unmodifiableList(Arrays.asList("javacv", "javacpp", "ffmpeg"));

  private NoFfmpegClassPath() {
  }

  /** The running JVM's test class path, every entry an absolute path. */
  public static List<String> testClassPath() {
    String surefire = System.getProperty("surefire.test.class.path");
    String classPath = surefire == null || surefire.isEmpty() ? System.getProperty("java.class.path") : surefire;
    return expand(split(classPath));
  }

  /** The running JVM's test class path without its FFmpeg entries. */
  public static List<String> testClassPathWithoutFfmpeg() {
    return withoutFfmpeg(testClassPath());
  }

  /** {@code entries} without the FFmpeg ones ({@link #isFfmpeg}), in order. */
  public static List<String> withoutFfmpeg(List<String> entries) {
    return entries.stream().filter(entry -> !isFfmpeg(entry)).collect(Collectors.toList());
  }

  /** Whether {@code entry} is an FFmpeg jar or directory: under {@code org/bytedeco}, or named {@code javacv*}, {@code javacpp*} or {@code ffmpeg*}. */
  public static boolean isFfmpeg(String entry) {
    String path = entry.replace('\\', '/');
    if (path.contains("/org/bytedeco/")) {
      return true;
    }
    String name = path.substring(path.lastIndexOf('/') + 1).toLowerCase(Locale.ROOT);
    return name.endsWith(".jar") && FFMPEG_JAR_PREFIXES.stream().anyMatch(name::startsWith);
  }

  /** The non-empty entries of a class path string. */
  public static List<String> split(String classPath) {
    return Arrays.stream(classPath.split(File.pathSeparator)).filter(entry -> !entry.isEmpty()).collect(Collectors.toList());
  }

  /**
   * {@code entries}, or, when they are one jar whose manifest names a {@code Class-Path} (a manifest-only booter jar), the
   * manifest's entries as absolute paths.
   */
  public static List<String> expand(List<String> entries) {
    if (entries.size() != 1 || !entries.get(0).endsWith(".jar")) {
      return entries;
    }
    File jar = new File(entries.get(0));
    List<String> manifestEntries = manifestClassPath(jar);
    return manifestEntries.isEmpty() ? entries : manifestEntries;
  }

  /** The {@code Class-Path} of {@code jar}'s manifest, each entry resolved against the jar's location; empty when it has none. */
  static List<String> manifestClassPath(File jar) {
    String classPath;
    try (JarFile file = new JarFile(jar)) {
      Manifest manifest = file.getManifest();
      classPath = manifest == null ? null : manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH);
    } catch (IOException e) {
      throw new UncheckedIOException("cannot read the manifest of " + jar, e);
    }
    if (classPath == null || classPath.trim().isEmpty()) {
      return Collections.emptyList();
    }
    URI base = jar.getAbsoluteFile().toURI();
    List<String> out = new ArrayList<>();
    for (String url : classPath.trim().split("\\s+")) {
      try {
        out.add(Paths.get(base.resolve(new URI(url))).toString());
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("bad Class-Path entry " + url + " in " + jar, e);
      }
    }
    return out;
  }
}
