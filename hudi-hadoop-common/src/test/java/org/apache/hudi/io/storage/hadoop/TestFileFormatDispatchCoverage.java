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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.core.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Exhaustiveness tests for the per-{@link HoodieFileFormat} dispatch sites on the read path.
 *
 * <p>The per-format {@code switch} statements are duplicated across factory overloads
 * ({@link HoodieFileReaderFactory#getFileReader} has one switch per overload, and
 * {@link HoodieHadoopIOFactory#getFileFormatUtils} has another). When a new
 * {@link HoodieFileFormat} value is added, it is easy to add the new case to one dispatch
 * point but miss another; e.g. the VORTEX case was added to the
 * {@code getFileReader(HoodieConfig, StoragePath, ...)} overload but initially missed in the
 * {@code getFileReader(HoodieConfig, StoragePathInfo, ...)} overload (fixed by
 * apache/hudi#19252). These tests iterate {@code HoodieFileFormat.values()} (never a
 * hardcoded list) and assert that all dispatch points treat every format consistently, so
 * they fail mechanically when a future format value covers some dispatch points but not
 * others.
 */
class TestFileFormatDispatchCoverage {

  private static final StoragePath TEST_PATH = new StoragePath("/partition/path/f1_1-0-1_000.parquet");

  /**
   * Outcome of pushing a format through one {@code getFileReader} dispatch point.
   */
  private enum Dispatch {
    // The switch has a case for the format and routed to the per-format factory method.
    DISPATCHED,
    // The switch fell through to the default case and threw UnsupportedOperationException.
    UNSUPPORTED
  }

  // TODO: include VORTEX here (remove the exclusion) once apache/hudi#19252 is merged; on
  //  current master the StoragePathInfo overload is missing the VORTEX case, which is exactly
  //  the asymmetry this test exists to catch (see testVortexReaderOverloadDispatchConsistency).
  @ParameterizedTest
  @EnumSource(value = HoodieFileFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"VORTEX"})
  public void testStoragePathAndStoragePathInfoOverloadsDispatchConsistently(HoodieFileFormat format)
      throws IOException {
    assertReaderOverloadsDispatchConsistently(format);
  }

  /**
   * Same check as {@link #testStoragePathAndStoragePathInfoOverloadsDispatchConsistently} for
   * VORTEX, asserting the post-fix behavior of apache/hudi#19252 (both overloads dispatch).
   */
  @Disabled("Depends on apache/hudi#19252: on current master getFileReader(HoodieConfig, StoragePathInfo, ...) "
      + "is missing the VORTEX case while the StoragePath overload has it. Enable once #19252 is merged.")
  @Test
  public void testVortexReaderOverloadDispatchConsistency() throws IOException {
    HoodieFileReader marker = mock(HoodieFileReader.class);
    HoodieFileReaderFactory factory = factoryWithMarkerReaders(marker);
    assertEquals(Dispatch.DISPATCHED, dispatchByStoragePath(factory, marker, HoodieFileFormat.VORTEX));
    assertEquals(Dispatch.DISPATCHED, dispatchByStoragePathInfo(factory, marker, HoodieFileFormat.VORTEX));
  }

  @ParameterizedTest
  @EnumSource(HoodieFileFormat.class)
  public void testExtensionDispatchConsistentWithFormatDispatch(HoodieFileFormat format) throws IOException {
    HoodieFileReader marker = mock(HoodieFileReader.class);
    HoodieFileReaderFactory factory = factoryWithMarkerReaders(marker);
    Dispatch byExtension = dispatchByExtension(factory, marker, format);
    Dispatch byFormat = dispatchByStoragePath(factory, marker, format);
    assertEquals(byFormat, byExtension, () -> String.format(
        "Dispatch asymmetry for %s: getFileReader(HoodieConfig, StoragePath, %s) -> %s but the extension-based "
            + "getFileReader(HoodieConfig, StoragePath) -> %s. A format handled by the format switch must also be "
            + "recognized by the extension if-chain in HoodieFileReaderFactory, and vice versa.",
        format, format, byFormat, byExtension));
  }

  @ParameterizedTest
  @EnumSource(HoodieFileFormat.class)
  public void testGetFileFormatUtilsThrowsOnlyUnsupportedOperationException(HoodieFileFormat format)
      throws IOException {
    try (HoodieStorage storage = HoodieTestUtils.getDefaultStorage()) {
      HoodieHadoopIOFactory ioFactory = new HoodieHadoopIOFactory(storage);
      try {
        assertNotNull(ioFactory.getFileFormatUtils(format),
            () -> "getFileFormatUtils(" + format + ") returned null instead of a FileFormatUtils");
      } catch (UnsupportedOperationException e) {
        // Acceptable: the format has no FileFormatUtils implementation yet. Any other exception
        // type (e.g. HoodieException, NPE) propagates and fails the test, flagging a dispatch
        // point that handles a new format inconsistently with the rest.
      }
    }
  }

  @Test
  public void testFileFormatUtilsCoverAllReaderDispatchableFormats() throws IOException {
    HoodieFileReader marker = mock(HoodieFileReader.class);
    HoodieFileReaderFactory factory = factoryWithMarkerReaders(marker);
    Set<HoodieFileFormat> readerDispatchable = EnumSet.noneOf(HoodieFileFormat.class);
    for (HoodieFileFormat format : HoodieFileFormat.values()) {
      if (dispatchByStoragePath(factory, marker, format) == Dispatch.DISPATCHED) {
        readerDispatchable.add(format);
      }
    }

    Set<HoodieFileFormat> utilsSupported = EnumSet.noneOf(HoodieFileFormat.class);
    try (HoodieStorage storage = HoodieTestUtils.getDefaultStorage()) {
      HoodieHadoopIOFactory ioFactory = new HoodieHadoopIOFactory(storage);
      for (HoodieFileFormat format : HoodieFileFormat.values()) {
        try {
          ioFactory.getFileFormatUtils(format);
          utilsSupported.add(format);
        } catch (UnsupportedOperationException e) {
          // format has no FileFormatUtils implementation
        }
      }
    }

    assertTrue(utilsSupported.containsAll(readerDispatchable), () -> String.format(
        "Formats dispatchable by HoodieFileReaderFactory but missing from "
            + "HoodieHadoopIOFactory#getFileFormatUtils: %s. Add the missing case to the "
            + "getFileFormatUtils switch.",
        readerDispatchable.stream()
            .filter(f -> !utilsSupported.contains(f))
            .collect(Collectors.toList())));
  }

  private void assertReaderOverloadsDispatchConsistently(HoodieFileFormat format) throws IOException {
    HoodieFileReader marker = mock(HoodieFileReader.class);
    HoodieFileReaderFactory factory = factoryWithMarkerReaders(marker);
    Dispatch byStoragePath = dispatchByStoragePath(factory, marker, format);
    Dispatch byStoragePathInfo = dispatchByStoragePathInfo(factory, marker, format);
    assertEquals(byStoragePath, byStoragePathInfo, () -> String.format(
        "Dispatch asymmetry for %s: getFileReader(HoodieConfig, StoragePath, ...) -> %s but "
            + "getFileReader(HoodieConfig, StoragePathInfo, ...) -> %s. Every HoodieFileFormat case must be "
            + "present in BOTH switch statements in HoodieFileReaderFactory (this is the bug class fixed by "
            + "apache/hudi#19252 for VORTEX).",
        format, byStoragePath, byStoragePathInfo));
  }

  /**
   * Builds a {@link HoodieFileReaderFactory} whose per-format {@code newXxxFileReader} factory
   * methods all return {@code marker}, while every other method (in particular the
   * {@code getFileReader} overloads containing the per-format switches) runs the real code.
   *
   * <p>The interception is by method-name pattern ({@code new*} returning a
   * {@link HoodieFileReader}), so a per-format factory hook added for a future format is
   * covered automatically without updating this test.
   */
  private HoodieFileReaderFactory factoryWithMarkerReaders(HoodieFileReader marker) {
    return mock(HoodieFileReaderFactory.class, invocation -> {
      Method method = invocation.getMethod();
      // newBootstrapFileReader also matches the name pattern but is a composition helper, not a
      // per-format dispatch hook; exclude it so only format hooks are intercepted.
      if (method.getName().startsWith("new") && !method.getName().equals("newBootstrapFileReader")
          && HoodieFileReader.class.isAssignableFrom(method.getReturnType())) {
        return marker;
      }
      return invocation.callRealMethod();
    });
  }

  private Dispatch dispatchByStoragePath(HoodieFileReaderFactory factory, HoodieFileReader marker,
                                         HoodieFileFormat format) throws IOException {
    try {
      HoodieFileReader reader =
          factory.getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, TEST_PATH, format, Option.empty());
      assertSame(marker, reader, () -> "getFileReader(HoodieConfig, StoragePath, " + format + ") returned a reader "
          + "that did not come from a per-format newXxxFileReader hook; the switch case likely constructs the "
          + "reader inline, which this coverage test cannot track. Route it through a protected factory method.");
      return Dispatch.DISPATCHED;
    } catch (UnsupportedOperationException e) {
      return Dispatch.UNSUPPORTED;
    }
  }

  private Dispatch dispatchByStoragePathInfo(HoodieFileReaderFactory factory, HoodieFileReader marker,
                                             HoodieFileFormat format) throws IOException {
    StoragePathInfo pathInfo = new StoragePathInfo(TEST_PATH, 100, false, (short) 0, 0, 0);
    try {
      HoodieFileReader reader =
          factory.getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, pathInfo, format, Option.empty());
      assertSame(marker, reader, () -> "getFileReader(HoodieConfig, StoragePathInfo, " + format + ") returned a "
          + "reader that did not come from a per-format newXxxFileReader hook; the switch case likely constructs "
          + "the reader inline, which this coverage test cannot track. Route it through a protected factory method.");
      return Dispatch.DISPATCHED;
    } catch (UnsupportedOperationException e) {
      return Dispatch.UNSUPPORTED;
    }
  }

  private Dispatch dispatchByExtension(HoodieFileReaderFactory factory, HoodieFileReader marker,
                                       HoodieFileFormat format) throws IOException {
    StoragePath path = new StoragePath("/partition/path/f1_1-0-1_000" + format.getFileExtension());
    try {
      HoodieFileReader reader = factory.getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, path);
      assertSame(marker, reader, () -> "Extension-based getFileReader for " + format + " returned a reader that "
          + "did not come from a per-format newXxxFileReader hook.");
      return Dispatch.DISPATCHED;
    } catch (UnsupportedOperationException e) {
      return Dispatch.UNSUPPORTED;
    }
  }
}
