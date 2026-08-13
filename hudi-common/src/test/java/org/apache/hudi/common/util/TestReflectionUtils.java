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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ReflectionUtils.getMethod;
import static org.apache.hudi.common.util.ReflectionUtils.getTopLevelClassesInClasspath;
import static org.apache.hudi.common.util.ReflectionUtils.isSubClass;
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

  /**
   * An exploded directory on the classpath, reached over the "file" protocol. Built explicitly
   * rather than relying on the test classpath, because under Maven the modules are jars and this
   * branch would never be entered.
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

    List<String> classes = withContextClassLoaderOver(root, () ->
        getTopLevelClassesInClasspath(ReflectionUtils.class).collect(Collectors.toList()));

    assertEquals(
        Arrays.asList(scanned + ".Alpha", scanned + ".Beta", scanned + ".nested.Gamma"),
        classes.stream().sorted().collect(Collectors.toList()),
        "a directory entry must yield the classes of the package and its subpackages, and nothing else");
  }

  /**
   * The classpath as the JVM actually presents it, whichever protocol that turns out to be: a jar
   * under Maven, an exploded directory in an IDE. Both have to work.
   */
  @Test
  void testGetTopLevelClassesInClasspathOnTheRealClasspath() {
    List<String> classes = getTopLevelClassesInClasspath(ReflectionUtils.class).collect(Collectors.toList());

    assertTrue(classes.contains(ReflectionUtils.class.getName()),
        "the class whose package was scanned must be found, got: " + classes);
    assertTrue(classes.stream().allMatch(name -> name.startsWith(ReflectionUtils.class.getPackage().getName())),
        "every name must be in the scanned package, got: " + classes);
    assertTrue(classes.stream().noneMatch(name -> name.endsWith(".class")),
        "names must be class names rather than file names, got: " + classes);
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

    List<String> classes = withContextClassLoaderOver(jar, () ->
        getTopLevelClassesInClasspath(ReflectionUtils.class).collect(Collectors.toList()));

    assertEquals(
        Arrays.asList(scanned + ".Alpha", scanned + ".Beta", scanned + ".nested.Gamma"),
        classes.stream().sorted().collect(Collectors.toList()),
        "a jar entry must yield the classes of the package and its subpackages, and nothing else");
  }

  @Test
  void testGetTopLevelClassesInClasspathForClassesWithoutAPackage() {
    // Arrays have no package, which used to dereference null.
    assertEquals(0, getTopLevelClassesInClasspath(String[].class).count());
    assertEquals(0, getTopLevelClassesInClasspath(int[].class).count());
  }

  @Test
  void testGetTopLevelClassesInClasspathForPackageNotOnTheClasspath(@TempDir Path tempDir) throws Exception {
    // A loader with no parent and an empty jar finds no resource for the package, so there is
    // nothing to scan and nothing to fail on.
    Path jar = tempDir.resolve("empty.jar");
    writeJar(jar);

    long count = withContextClassLoaderOver(jar, () -> getTopLevelClassesInClasspath(ReflectionUtils.class).count());
    assertEquals(0, count);
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
   * root, which may be a jar or an exploded directory. The loader is given no parent so the scan
   * sees nothing else.
   */
  private static <T> T withContextClassLoaderOver(Path root, Supplier<T> supplier) throws IOException {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader = new URLClassLoader(new URL[] {root.toUri().toURL()}, null)) {
      Thread.currentThread().setContextClassLoader(loader);
      return supplier.get();
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

}
