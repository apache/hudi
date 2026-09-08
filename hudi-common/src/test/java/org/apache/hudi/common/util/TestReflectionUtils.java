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
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ReflectionUtils.getMethod;
import static org.apache.hudi.common.util.ReflectionUtils.isSubClass;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ReflectionUtils}.
 */
public class TestReflectionUtils {

  // Isolate fixture classes from production classes on the real classpath.
  private static final String SCRATCH_PACKAGE = "org.apache.hudi.common.util.scratchfixtures";
  private static final String SCRATCH_PACKAGE_PATH = SCRATCH_PACKAGE.replace('.', '/');

  private static final String CLASS_A = SCRATCH_PACKAGE + ".TopLevelA";
  private static final String CLASS_B = SCRATCH_PACKAGE + ".TopLevelB";
  private static final String CLASS_SUBPACKAGE = SCRATCH_PACKAGE + ".sub.SubLevelC";

  @TempDir
  Path tempDir;

  private Path writeDirectoryFixture() throws IOException {
    Path root = tempDir.resolve("classes");
    for (String className : Arrays.asList(CLASS_A, CLASS_B, CLASS_SUBPACKAGE)) {
      String resourceName = className.replace('.', '/') + ".class";
      Path target = root.resolve(resourceName);
      Files.createDirectories(target.getParent());
      try (InputStream input = TestReflectionUtils.class.getClassLoader().getResourceAsStream(resourceName)) {
        Files.copy(input, target);
      }
    }
    // Negative cases: non-class files and nested classes must be excluded.
    Path pkgDir = root.resolve(SCRATCH_PACKAGE_PATH);
    Files.write(pkgDir.resolve("README.txt"), new byte[0]);
    Files.write(pkgDir.resolve("TopLevelA$Nested.class"), new byte[0]);
    return root;
  }

  private Path writeJarFixture() throws IOException {
    Path root = writeDirectoryFixture();
    Path jarPath = tempDir.resolve("fixture.jar");
    try (JarOutputStream jos = new JarOutputStream(Files.newOutputStream(jarPath));
         Stream<Path> paths = Files.walk(root)) {
      Iterator<Path> entries = paths.filter(path -> !path.equals(root)).iterator();
      while (entries.hasNext()) {
        Path entry = entries.next();
        String name = root.relativize(entry).toString().replace(File.separatorChar, '/');
        // URLClassLoader.getResources(package) requires an explicit directory entry.
        jos.putNextEntry(new JarEntry(Files.isDirectory(entry) ? name + "/" : name));
        if (Files.isRegularFile(entry)) {
          Files.copy(entry, jos);
        }
        jos.closeEntry();
      }
    }
    return jarPath;
  }

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

  @Test
  void testGetTopLevelClassesInClasspathFromDirectory() throws IOException {
    Path root = writeDirectoryFixture();
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader = new URLClassLoader(new URL[]{root.toUri().toURL()}, null)) {
      Thread.currentThread().setContextClassLoader(loader);
      // Anchor on TopLevelA, which is loaded from the fixture and exposes
      // SCRATCH_PACKAGE as its package.
      Class<?> anchor = loader.loadClass(CLASS_A);
      java.util.List<String> scanned = ReflectionUtils.getTopLevelClassesInClasspath(anchor)
          .collect(Collectors.toList());
      // Subpackages are included by documented behaviour; the non-class
      // file must be excluded.
      assertEquals(new HashSet<>(Arrays.asList(CLASS_A, CLASS_B, CLASS_SUBPACKAGE)),
          new HashSet<>(scanned),
          "Expected only the three classes inside " + SCRATCH_PACKAGE
              + ", got " + scanned);
    } catch (ClassNotFoundException e) {
      throw new AssertionError("Fixture class should be loadable", e);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  void testGetTopLevelClassesInClasspathFromJar() throws IOException {
    Path jarPath = writeJarFixture();
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader = new URLClassLoader(new URL[]{jarPath.toUri().toURL()}, null)) {
      Thread.currentThread().setContextClassLoader(loader);
      Class<?> anchor = loader.loadClass(CLASS_A);
      java.util.List<String> scanned = ReflectionUtils.getTopLevelClassesInClasspath(anchor)
          .collect(Collectors.toList());
      assertEquals(new HashSet<>(Arrays.asList(CLASS_A, CLASS_B, CLASS_SUBPACKAGE)),
          new HashSet<>(scanned),
          "Expected only the three classes inside " + SCRATCH_PACKAGE
              + ", got " + scanned);
    } catch (ClassNotFoundException e) {
      throw new AssertionError("Fixture class should be loadable", e);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  void testGetTopLevelClassesInClasspathOnTheRealClasspath() {
    // Discover classes across the test directory and dependency JARs.
    java.util.List<String> scanned = ReflectionUtils.getTopLevelClassesInClasspath(
        TestReflectionUtils.class).collect(Collectors.toList());
    assertTrue(scanned.contains(TestReflectionUtils.class.getName()));
    assertTrue(scanned.contains(ReflectionUtils.class.getName()));
  }

  @Test
  void testGetTopLevelClassesInClasspathForClassesWithoutAPackage() {
    // Arrays and primitives have no Package; the original code dereferenced
    // getPackage() and threw NullPointerException. New contract: empty stream.
    Stream<String> arrayResult = ReflectionUtils.getTopLevelClassesInClasspath(int[].class);
    assertEquals(0L, arrayResult.count(),
        "Expected empty stream for an array class without a package");
    Stream<String> primitiveResult = ReflectionUtils.getTopLevelClassesInClasspath(int.class);
    assertEquals(0L, primitiveResult.count(),
        "Expected empty stream for a primitive class without a package");
  }

  @Test
  void testGetTopLevelClassesInClasspathForPackageNotOnTheClasspath() throws IOException {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader = new URLClassLoader(new URL[0], null)) {
      Thread.currentThread().setContextClassLoader(loader);
      assertEquals(0L, ReflectionUtils.getTopLevelClassesInClasspath(TestReflectionUtils.class).count());
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  void testGetTopLevelClassesInClasspathHandlesIOException() {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    ClassLoader loader = new ClassLoader(original) {
      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        throw new IOException("Simulated failure enumerating resources");
      }
    };
    try {
      Thread.currentThread().setContextClassLoader(loader);
      assertEquals(0L, ReflectionUtils.getTopLevelClassesInClasspath(TestReflectionUtils.class).count());
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }
}
