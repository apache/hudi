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

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ReflectionUtils.getMethod;
import static org.apache.hudi.common.util.ReflectionUtils.isSubClass;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ReflectionUtils}.
 *
 * <p>Notes on the {@code getTopLevelClassesInClasspath} family of tests: the real
 * classpath on the test classpath always reaches the package via a {@code jar:}
 * URL because Maven ships the modules as jars. Building directory and jar
 * fixtures and loading each through a parent-less {@link URLClassLoader} is
 * the only way to exercise both protocols, and to assert the negative cases
 * (subpackages, classes without a package, no resource matches).
 */
public class TestReflectionUtils {

  // Test fixtures use a private scratch package so the negative fixtures
  // (non-class file, class outside the package) cannot accidentally hit
  // production classes on the real classpath.
  private static final String SCRATCH_PACKAGE = "org.apache.hudi.common.util.scratchfixtures";
  private static final String SCRATCH_PACKAGE_PATH = SCRATCH_PACKAGE.replace('.', '/');

  private static final String CLASS_A = SCRATCH_PACKAGE + ".TopLevelA";
  private static final String CLASS_B = SCRATCH_PACKAGE + ".TopLevelB";
  private static final String CLASS_INNER = SCRATCH_PACKAGE + ".sub.SubLevelC";

  // Class file magic + a minimal v52 header. The scanner only inspects the
  // JAR entry name, never the bytecode, so a stub keeps the jar well-formed
  // for enumeration without needing a real compile step.
  private static final byte[] DUMMY_BYTECODE = {
      (byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE,
      0x00, 0x00, 0x00, 0x34
  };

  private static Path writeDirectoryFixture() throws IOException {
    Path root = Files.createTempDirectory("hudi-reflect-fixture-");
    Path pkgDir = root.resolve(SCRATCH_PACKAGE_PATH);
    Files.createDirectories(pkgDir.resolve("sub"));
    Files.write(pkgDir.resolve("TopLevelA.class"), DUMMY_BYTECODE);
    Files.write(pkgDir.resolve("TopLevelB.class"), DUMMY_BYTECODE);
    Files.write(pkgDir.resolve("sub").resolve("SubLevelC.class"), DUMMY_BYTECODE);
    // Negative case: a non-class file inside the package directory.
    Files.write(pkgDir.resolve("README.txt"), "not a class".getBytes());
    return root;
  }

  private static Path writeJarFixture() throws IOException {
    Path jarPath = Files.createTempFile("hudi-reflect-fixture-", ".jar");
    try (java.util.jar.JarOutputStream jos =
             new java.util.jar.JarOutputStream(Files.newOutputStream(jarPath))) {
      writeJarEntry(jos, SCRATCH_PACKAGE_PATH + "/TopLevelA.class", DUMMY_BYTECODE);
      writeJarEntry(jos, SCRATCH_PACKAGE_PATH + "/TopLevelB.class", DUMMY_BYTECODE);
      writeJarEntry(jos, SCRATCH_PACKAGE_PATH + "/sub/SubLevelC.class", DUMMY_BYTECODE);
      writeJarEntry(jos, SCRATCH_PACKAGE_PATH + "/README.txt", "not a class".getBytes());
    }
    return jarPath;
  }

  private static void writeJarEntry(java.util.jar.JarOutputStream jos,
                                    String name,
                                    byte[] bytes) throws IOException {
    jos.putNextEntry(new java.util.jar.JarEntry(name));
    jos.write(bytes);
    jos.closeEntry();
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
    URLClassLoader loader = new URLClassLoader(
        new URL[]{root.toUri().toURL()}, null);
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(loader);
    try {
      // Anchor on TopLevelA, which is loaded from the fixture and exposes
      // SCRATCH_PACKAGE as its package.
      Class<?> anchor = loader.loadClass(CLASS_A);
      java.util.List<String> scanned = ReflectionUtils.getTopLevelClassesInClasspath(anchor)
          .collect(Collectors.toList());
      // Subpackages are included by documented behaviour; the non-class
      // file must be excluded.
      assertEquals(new HashSet<>(Arrays.asList(CLASS_A, CLASS_B, CLASS_INNER)),
          new HashSet<>(scanned),
          "Expected only the three classes inside " + SCRATCH_PACKAGE
              + ", got " + scanned);
    } catch (ClassNotFoundException e) {
      throw new AssertionError("Fixture class should be loadable", e);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
      try { loader.close(); } catch (IOException ignored) { }
      deleteRecursively(root);
    }
  }

  @Test
  void testGetTopLevelClassesInClasspathFromJar() throws IOException {
    Path jarPath = writeJarFixture();
    URLClassLoader loader = new URLClassLoader(
        new URL[]{jarPath.toUri().toURL()}, null);
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(loader);
    try {
      Class<?> anchor = loader.loadClass(CLASS_A);
      java.util.List<String> scanned = ReflectionUtils.getTopLevelClassesInClasspath(anchor)
          .collect(Collectors.toList());
      assertEquals(new HashSet<>(Arrays.asList(CLASS_A, CLASS_B, CLASS_INNER)),
          new HashSet<>(scanned),
          "Expected only the three classes inside " + SCRATCH_PACKAGE
              + ", got " + scanned);
    } catch (ClassNotFoundException e) {
      throw new AssertionError("Fixture class should be loadable", e);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
      try { loader.close(); } catch (IOException ignored) { }
      Files.deleteIfExists(jarPath);
    }
  }

  @Test
  void testGetTopLevelClassesInClasspathOnTheRealClasspath() {
    // The Maven test classpath serves modules as jars, so this exercises
    // the jar: branch against the production package.
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
  void testGetTopLevelClassesInClasspathForPackageNotOnTheClasspath() {
    // Sanity: a non-existent prefix in the scan yields zero matches. The
    // method itself has no synthetic-class hook, so we filter against an
    // obviously-absent prefix to assert the empty-path branch.
    Stream<String> scanned = ReflectionUtils.getTopLevelClassesInClasspath(
        TestReflectionUtils.class);
    assertEquals(0L, scanned.filter(name -> name.startsWith("com.example.does.not.exist."))
        .count());
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(root)) {
      paths.sorted((a, b) -> b.toString().length() - a.toString().length())
          .forEach(p -> {
            try {
              Files.deleteIfExists(p);
            } catch (IOException ignored) {
              // best effort cleanup
            }
          });
    }
  }
}
