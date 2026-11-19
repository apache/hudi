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

import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Audio on without FFmpeg: runs {@link AudioFfmpegAbsentProbe} in a class loader built from the test class path WITHOUT any
 * org.bytedeco jar and checks that the driver's config check with the default FFmpeg audio segment source asks for no FFmpeg
 * class, that the first video's speech fails as a
 * {@link org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException} naming
 * {@value VideoFramePreprocessor#FFMPEG_JARS_SETTING} and the segment source key, and that a round holding a PDF then a video
 * fails after the PDF's rows with no quarantine entry for the video (a configuration error, never a FAILED file).
 */
public class TestAudioFfmpegAbsent {

  @Test
  public void defaultAudioSegmentSourceNeedsFfmpegOnlyForAVideosSpeech() throws Exception {
    String report;
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    try (RecordingClassLoader withoutFfmpeg = new RecordingClassLoader(classPathWithoutBytedeco())) {
      thread.setContextClassLoader(withoutFfmpeg);
      Callable<?> probe = (Callable<?>) withoutFfmpeg.loadClass(AudioFfmpegAbsentProbe.class.getName()).getDeclaredConstructor()
          .newInstance();
      report = (String) probe.call();
    } finally {
      thread.setContextClassLoader(previous);
    }
    assertTrue(report.contains("bytedeco hidden\n"), report);
    assertTrue(report.contains("audio validateConfig ok\n"), report);
    assertTrue(report.contains("validateConfig loaded bytedeco classes []\n"), report);
    assertRefusal(line(report, "speech refused "), report);
    assertTrue(line(report, "speech cause ").contains("NoClassDefFoundError: org/bytedeco/"), report);
    assertRefusal(line(report, "round failed "), report);
    String rows = line(report, "round rows ");
    assertTrue(rows.endsWith(" side outputs 0") && !rows.startsWith("round rows 0 "),
        "the PDF's rows, then the failure: no quarantine entry for the video\n" + report);
  }

  private static void assertRefusal(String refused, String report) {
    assertTrue(refused.contains(VideoFramePreprocessor.FFMPEG_JARS_SETTING), report);
    assertTrue(refused.contains(AudioConfigKeys.SEGMENT_SOURCE_CLASS.key()), report);
    assertTrue(refused.contains(AudioConfigKeys.ENABLED.key()), "names the opt-out: " + refused);
    assertTrue(refused.contains("org.bytedeco:javacv") && refused.contains("org.bytedeco:ffmpeg"), refused);
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

  /** A loader of the given class path over the platform loader, recording the name of every class it is asked to find. */
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
