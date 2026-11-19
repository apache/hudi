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

import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FFmpeg (org.bytedeco) is not shipped in any Hudi bundle; a job that prepares video adds the FFmpeg jars through
 * {@value VideoFramePreprocessor#FFMPEG_JARS_SETTING}. This test runs {@link FfmpegAbsentProbe} in a class loader built from the
 * test class path WITHOUT any org.bytedeco jar (the shipped bundle's situation, whatever the test class path holds) and checks
 * that, with the default video preprocessor, the driver's config check of a PDF-only job and of a video job loads no FFmpeg
 * class (the loader records every class it is asked for), that a PDF-only round passes, and that an unavailable video is
 * quarantined with a {@link PreprocessingException} naming the FFmpeg jars setting and the video preprocessor key. The mixed
 * round keeps the PDF's rows and records the video's error.
 *
 * <p>The probe is called through {@link Callable}, the one type both loaders share: a separate loader is the only way to take
 * classes away from a JVM that has them.
 */
class TestFfmpegAbsentClassGraph {

  @Test
  void defaultVideoPreprocessorNeedsFfmpegOnlyToOpenAVideo() throws Exception {
    String report;
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    try (RecordingClassLoader withoutFfmpeg = new RecordingClassLoader(classPathWithoutBytedeco())) {
      // Hadoop and Spark resolve classes through the context loader: keep them inside the FFmpeg-less one
      thread.setContextClassLoader(withoutFfmpeg);
      Callable<?> probe = (Callable<?>) withoutFfmpeg.loadClass(FfmpegAbsentProbe.class.getName()).getDeclaredConstructor()
          .newInstance();
      report = (String) probe.call();
    } finally {
      thread.setContextClassLoader(previous);
    }
    assertTrue(report.contains("bytedeco hidden\n"), report);
    assertTrue(report.contains("pdf validateConfig ok\n"), report);
    assertTrue(report.contains("video validateConfig ok\n"), report);
    assertTrue(report.contains("video preprocessor " + VideoFramePreprocessor.class.getName() + "\n"), report);
    assertTrue(report.contains("validateConfig loaded ffmpeg classes []\n"), report);

    assertRefusal(line(report, "video quarantined "), report);
    String pdfRound = line(report, "pdf round rows ");
    assertTrue(pdfRound.endsWith(" side outputs 0") && !pdfRound.startsWith("pdf round rows 0 "), report);
    assertTrue(report.contains("mixed round passed\n"), report);
    assertEquals(pdfRound.replace("pdf round", "mixed round").replace("side outputs 0", "side outputs 1"),
        line(report, "mixed round rows "), "the PDF's rows are kept and the unavailable video is quarantined\n" + report);
    assertRefusal(line(report, "video error "), report);
  }

  private static void assertRefusal(String refused, String report) {
    assertTrue(refused.contains(VideoFramePreprocessor.FFMPEG_JARS_SETTING), report);
    assertTrue(refused.contains(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key()), report);
    assertTrue(refused.contains("org.bytedeco:javacv") && refused.contains("org.bytedeco:ffmpeg")
        && refused.contains("org.bytedeco:javacpp"), refused);
    assertFalse(refused.contains("--jars"), refused);
    assertTrue(refused.contains(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key()), "names the opt-out: " + refused);
  }

  private static String line(String report, String prefix) {
    for (String line : report.split("\n")) {
      if (line.startsWith(prefix)) {
        return line;
      }
    }
    return "";
  }

  /** The test JVM's class path (surefire's, when it forks with a manifest-only jar) minus every org.bytedeco jar. */
  private static URL[] classPathWithoutBytedeco() throws MalformedURLException {
    String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    List<URL> urls = new ArrayList<>();
    for (String entry : classPath.split(File.pathSeparator)) {
      if (entry.isEmpty() || entry.replace('\\', '/').contains("/org/bytedeco/")) {
        continue;
      }
      urls.add(new File(entry).toURI().toURL());
    }
    assertEquals(0, urls.stream().filter(url -> url.getPath().contains("bytedeco")).count());
    return urls.toArray(new URL[0]);
  }

  /**
   * A loader of the given class path over the platform loader (so every application class is its own), recording the name of
   * every class it is asked to find, found or not; the probe reads the record as a {@link Supplier}.
   */
  private static final class RecordingClassLoader extends URLClassLoader implements Supplier<List<String>> {
    private final List<String> asked = Collections.synchronizedList(new ArrayList<>());

    private RecordingClassLoader(URL[] classPath) {
      super(classPath, ClassLoader.getPlatformClassLoader());
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      asked.add(name);
      return super.findClass(name);
    }

    @Override
    public List<String> get() {
      synchronized (asked) {
        return new ArrayList<>(asked);
      }
    }
  }
}
