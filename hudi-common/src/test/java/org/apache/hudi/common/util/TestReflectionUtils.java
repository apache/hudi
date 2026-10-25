/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.common.conflict.detection.DirectMarkerBasedDetectionStrategy;
import org.apache.hudi.common.conflict.detection.EarlyConflictDetectionStrategy;
import org.apache.hudi.common.conflict.detection.TimelineServerBasedDetectionStrategy;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.function.Supplier;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ReflectionUtils.getMethod;
import static org.apache.hudi.common.util.ReflectionUtils.getTopLevelClassesInClasspath;
import static org.apache.hudi.common.util.ReflectionUtils.isSubClass;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ReflectionUtils}
 */
public class TestReflectionUtils {
  @Test
  public void testIsSubClass() {
    String subClassName1 = DirectMarkerBasedDetectionStrategy.class.getName();
    String subClassName2 = TimelineServerBasedDetectionStrategy.class.getName();
    assertTrue(isSubClass(subClassName1, EarlyConflictDetectionStrategy.class));
    assertTrue(isSubClass(subClassName2, EarlyConflictDetectionStrategy.class));
    assertTrue(isSubClass(subClassName2, TimelineServerBasedDetectionStrategy.class));
    assertFalse(isSubClass(subClassName2, DirectMarkerBasedDetectionStrategy.class));
  }

  @Test
  void testGetMethod() {
    assertTrue(getMethod(HoodieStorage.class, "getScheme").isPresent());
    assertTrue(getMethod(HoodieStorage.class, "listFiles", StoragePath.class).isPresent());
    assertTrue(getMethod(HoodieStorage.class,
        "listDirectEntries", StoragePath.class, StoragePathFilter.class).isPresent());
    assertFalse(getMethod(HoodieStorage.class,
        "listDirectEntries", StoragePathFilter.class).isPresent());
    assertFalse(getMethod(HoodieStorage.class, "nonExistentMethod").isPresent());
  }

  @ParameterizedTest
  @ValueSource(strings = {"jar:file:/unused.jar!/org/apache/hudi/common/util", "file:/invalid path",
      "http://example.invalid/org/apache/hudi/common/util"})
  void testGetTopLevelClassesInClasspathSkipsInvalidResources(String invalidResource) {
    ClassLoader loader = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        return Collections.enumeration(Arrays.asList(new URL(invalidResource), TestReflectionUtils.class.getResource("")));
      }
    };
    assertTrue(withContextClassLoader(loader, () ->
        ReflectionUtils.getTopLevelClassesInClasspath(TestReflectionUtils.class)
            .anyMatch(TestReflectionUtils.class.getName()::equals)));
  }

  @Test
  void testGetTopLevelClassesInClasspathHandlesIOException() {
    ClassLoader loader = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        throw new IOException("Simulated failure enumerating resources");
      }
    };
    assertFalse(withContextClassLoader(loader, () ->
        ReflectionUtils.getTopLevelClassesInClasspath(TestReflectionUtils.class).findAny().isPresent()));
  }

  /**
   * An exploded directory on the classpath, reached over the "file" protocol. Built explicitly
   * rather than scanning the test classpath, so the expected set is exact and independent of
   * whatever else surefire puts on the classpath.
   */
  @Test
  void testGetTopLevelClassesInClasspathFromDirectory(@TempDir Path tempDir) throws Exception {
    String scanned = ReflectionUtils.class.getPackage().getName();
    Path root = tempDir.resolve("classes");
    Path pkgDir = root.resolve(scanned.replace('.', File.separatorChar));
    Files.createDirectories(pkgDir.resolve("nested"));
    Files.write(pkgDir.resolve("Alpha.class"), new byte[] {1});
    Files.write(pkgDir.resolve("Beta.class"), new byte[] {1});
    Files.write(pkgDir.resolve("nested").resolve("Gamma.class"), new byte[] {1});
    Files.write(pkgDir.resolve("notaclass.txt"), new byte[] {1});

    List<String> classes = withContextClassLoaderOver(() ->
        getTopLevelClassesInClasspath(ReflectionUtils.class).collect(Collectors.toList()), root);

    assertEquals(
        Arrays.asList(scanned + ".Alpha", scanned + ".Beta", scanned + ".nested.Gamma"),
        classes.stream().sorted().collect(Collectors.toList()),
        "a directory entry must yield the classes of the package and its subpackages, and nothing else");
  }

  /**
   * Classes packaged in a jar are reached over the "jar" protocol, whose URLs are not hierarchical
   * and so cannot be turned into a {@link File}. Every caller of this method is a bundle
   * Main class, which is exactly the packaged case.
   * <p>
   * The jar is built under the scanned class's own package, and the loader is given no parent, so
   * the only resource found for that package is the one written here.
   */
  @Test
  void testGetTopLevelClassesInClasspathFromJar(@TempDir Path tempDir) throws Exception {
    String scanned = ReflectionUtils.class.getPackage().getName();
    String dir = scanned.replace('.', '/') + "/";
    Path jar = tempDir.resolve("classes.jar");
    writeJar(jar,
        dir,
        dir + "Alpha.class",
        dir + "Beta.class",
        dir + "nested/",
        dir + "nested/Gamma.class",
        dir + "notaclass.txt",
        "org/example/other/Delta.class");

    List<String> classes = withContextClassLoaderOver(() ->
        getTopLevelClassesInClasspath(ReflectionUtils.class).collect(Collectors.toList()), jar);

    assertEquals(
        Arrays.asList(scanned + ".Alpha", scanned + ".Beta", scanned + ".nested.Gamma"),
        classes.stream().sorted().collect(Collectors.toList()),
        "a jar entry must yield the classes of the package and its subpackages, and nothing else");
  }

  @Test
  void testGetTopLevelClassesInClasspathForClassesWithoutAPackage() {
    // Arrays and primitives have no package, which used to dereference null.
    assertEquals(0, getTopLevelClassesInClasspath(String[].class).count());
    assertEquals(0, getTopLevelClassesInClasspath(int.class).count());
  }

  /**
   * A jar: URL whose stream handler returns a plain URLConnection rather than a JarURLConnection.
   * Without the type check the cast throws ClassCastException, which the IOException catch does not
   * cover, so a single such entry would fail the whole scan.
   */
  @Test
  void testGetTopLevelClassesInClasspathSkipsAJarUrlThatIsNotAJarConnection() throws Exception {
    URLStreamHandler plainHandler = new URLStreamHandler() {
      @Override
      protected URLConnection openConnection(URL url) {
        return new URLConnection(url) {
          @Override
          public void connect() {
            // Never connected: getTopLevelClassesInClasspath only inspects the connection's type.
          }
        };
      }
    };
    URL notAJarConnection =
        new URL(null, "jar:file:/unused.jar!/org/apache/hudi/common/util", plainHandler);
    ClassLoader loader = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
      @Override
      public Enumeration<URL> getResources(String name) {
        return Collections.enumeration(Arrays.asList(notAJarConnection, TestReflectionUtils.class.getResource("")));
      }
    };

    assertTrue(withContextClassLoader(loader, () ->
            ReflectionUtils.getTopLevelClassesInClasspath(TestReflectionUtils.class)
                .anyMatch(TestReflectionUtils.class.getName()::equals)),
        "an entry whose connection is not a JarURLConnection must be skipped, not fail the scan");
  }

  /**
   * The scan must leave a jar it did not open alone. Without setUseCaches(false) it closes the
   * JVM-wide cached JarFile, and the next read by whoever opened it first fails with
   * "IllegalStateException: zip file closed".
   */
  @Test
  void testGetTopLevelClassesInClasspathFromJarLeavesASharedJarFileUsable(@TempDir Path tempDir) throws Exception {
    String scanned = ReflectionUtils.class.getPackage().getName();
    String dir = scanned.replace('.', '/') + "/";
    Path jar = tempDir.resolve("shared.jar");
    writeJar(jar, dir, dir + "Alpha.class");

    // Open it through the shared cache first, the way another reader on the same JVM would.
    URL entry = new URL("jar:" + jar.toUri().toURL() + "!/" + dir);
    JarURLConnection shared = (JarURLConnection) entry.openConnection();
    shared.setUseCaches(true);
    JarFile sharedJar = shared.getJarFile();
    try {
      withContextClassLoaderOver(() -> getTopLevelClassesInClasspath(ReflectionUtils.class).count(), jar);
      assertDoesNotThrow(() -> sharedJar.getEntry(dir + "Alpha.class"),
          "the scan must not close a JarFile held open through the JVM-wide cache");
    } finally {
      sharedJar.close();
    }
  }

  /**
   * A package path that is a regular file rather than a directory. File#listFiles returns null
   * there, which used to escape as a NullPointerException.
   */
  @Test
  void testGetTopLevelClassesInClasspathWhenThePackagePathIsAFile(@TempDir Path tempDir) throws Exception {
    String scanned = ReflectionUtils.class.getPackage().getName();
    Path root = tempDir.resolve("classes");
    Path pkgPath = root.resolve(scanned.replace('.', File.separatorChar));
    Files.createDirectories(pkgPath.getParent());
    Files.write(pkgPath, new byte[] {1});

    long count = withContextClassLoaderOver(() -> getTopLevelClassesInClasspath(ReflectionUtils.class).count(), root);
    assertEquals(0, count, "an unreadable package entry must be skipped, not throw");
  }

  /**
   * The package resolving to more than one classpath entry, which is the shape surefire produces.
   * Every entry has to contribute, whichever protocol it uses.
   */
  @Test
  void testGetTopLevelClassesInClasspathUnionsEveryClasspathEntry(@TempDir Path tempDir) throws Exception {
    String scanned = ReflectionUtils.class.getPackage().getName();
    String dir = scanned.replace('.', '/') + "/";
    Path jar = tempDir.resolve("classes.jar");
    writeJar(jar, dir, dir + "FromJar.class");
    Path root = tempDir.resolve("classes");
    Path pkgDir = root.resolve(scanned.replace('.', File.separatorChar));
    Files.createDirectories(pkgDir);
    Files.write(pkgDir.resolve("FromDirectory.class"), new byte[] {1});

    List<String> classes = withContextClassLoaderOver(() ->
        getTopLevelClassesInClasspath(ReflectionUtils.class).collect(Collectors.toList()), jar, root);

    assertEquals(
        Arrays.asList(scanned + ".FromDirectory", scanned + ".FromJar"),
        classes.stream().sorted().collect(Collectors.toList()),
        "a jar entry and a directory entry for the same package must both contribute");
  }

  /** Writes a jar holding the given entry names; names ending in "/" become directory entries. */
  private static void writeJar(Path jar, String... entryNames) throws IOException {
    try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
      for (String entryName : entryNames) {
        out.putNextEntry(new JarEntry(entryName));
        if (!entryName.endsWith("/")) {
          // Content is irrelevant: the scan reads entry names, never the bytecode.
          out.write(new byte[] {1, 2, 3});
        }
        out.closeEntry();
      }
    }
  }

  /**
   * Runs the supplier with the thread context class loader reading only from the given classpath
   * roots, each of which may be a jar or an exploded directory. The loader is given no parent so
   * the scan sees nothing else.
   */
  private static <T> T withContextClassLoaderOver(Supplier<T> supplier, Path... roots) throws IOException {
    URL[] urls = new URL[roots.length];
    for (int i = 0; i < roots.length; i++) {
      urls[i] = roots[i].toUri().toURL();
    }
    try (URLClassLoader loader = new URLClassLoader(urls, null)) {
      return withContextClassLoader(loader, supplier);
    }
  }

  /** Runs the supplier with the given thread context class loader, restoring the previous one after. */
  private static <T> T withContextClassLoader(ClassLoader loader, Supplier<T> supplier) {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(loader);
      return supplier.get();
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }
}
