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

package org.apache.hudi.io.hadoop;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.HFileUtils;
import org.apache.hudi.common.util.LanceUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrcUtils;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.VortexUtils;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.storage.hadoop.HoodieHadoopIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.function.Supplier;

import static org.apache.hudi.common.testutils.HoodieTestUtils.DEFAULT_URI;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieHadoopIOFactory}
 */
class TestHoodieHadoopIOFactory {
  @Test
  void testGetFileFormatUtils() throws IOException {
    try (HoodieStorage storage = newStorage()) {
      HoodieIOFactory ioFactory = new HoodieHadoopIOFactory(storage);
      assertTrue(ioFactory.getFileFormatUtils(new StoragePath("file:///a/b.parquet")) instanceof ParquetUtils);
      assertTrue(ioFactory.getFileFormatUtils(new StoragePath("file:///a/b.orc")) instanceof OrcUtils);
      assertTrue(ioFactory.getFileFormatUtils(new StoragePath("file:///a/b.hfile")) instanceof HFileUtils);
      assertTrue(ioFactory.getFileFormatUtils(new StoragePath("file:///a/b.lance")) instanceof LanceUtils);
      assertTrue(ioFactory.getFileFormatUtils(new StoragePath("file:///a/b.vortex")) instanceof VortexUtils);
      assertThrows(
          UnsupportedOperationException.class,
          () -> ioFactory.getFileFormatUtils(new StoragePath("file:///a/b.log")));

      assertTrue(ioFactory.getFileFormatUtils(HoodieFileFormat.PARQUET) instanceof ParquetUtils);
      assertTrue(ioFactory.getFileFormatUtils(HoodieFileFormat.ORC) instanceof OrcUtils);
      assertTrue(ioFactory.getFileFormatUtils(HoodieFileFormat.HFILE) instanceof HFileUtils);
      assertTrue(ioFactory.getFileFormatUtils(HoodieFileFormat.LANCE) instanceof LanceUtils);
      assertTrue(ioFactory.getFileFormatUtils(HoodieFileFormat.VORTEX) instanceof VortexUtils);
      assertThrows(
          UnsupportedOperationException.class,
          () -> ioFactory.getFileFormatUtils(HoodieFileFormat.HOODIE_LOG));
    }
  }

  /**
   * {@link HoodieIOFactory#getFileFormatUtils(StoragePath)} maps the file extension to a format
   * through its own if-chain before delegating to the {@link HoodieFileFormat} switch in
   * {@link HoodieHadoopIOFactory#getFileFormatUtils(HoodieFileFormat)}. A format added to one
   * dispatch point but missed in the other only fails at runtime, so this sweeps every enum value
   * (never a hardcoded list) and requires both entry points to agree: the same utils class, or
   * {@link UnsupportedOperationException} from both. Any other exception type propagates.
   */
  @ParameterizedTest
  @EnumSource(HoodieFileFormat.class)
  void testGetFileFormatUtilsEntryPointsAgreeForEveryFormat(HoodieFileFormat format) throws IOException {
    try (HoodieStorage storage = newStorage()) {
      HoodieIOFactory ioFactory = new HoodieHadoopIOFactory(storage);
      StoragePath path = new StoragePath("file:///a/b" + format.getFileExtension());
      Option<Class<?>> byFormat = fileFormatUtilsClass(() -> ioFactory.getFileFormatUtils(format));
      Option<Class<?>> byPath = fileFormatUtilsClass(() -> ioFactory.getFileFormatUtils(path));
      assertEquals(byFormat, byPath, () -> String.format(
          "Dispatch asymmetry for %s: getFileFormatUtils(HoodieFileFormat) -> %s but getFileFormatUtils(%s) -> %s; "
              + "the extension if-chain in HoodieIOFactory and the format switch in HoodieHadoopIOFactory must "
              + "cover the same formats.",
          format, byFormat, path, byPath));
    }
  }

  /**
   * @return the class of the returned utils, or empty when the entry point throws
   * {@link UnsupportedOperationException} for the format.
   */
  private static Option<Class<?>> fileFormatUtilsClass(Supplier<FileFormatUtils> entryPoint) {
    try {
      return Option.of(entryPoint.get().getClass());
    } catch (UnsupportedOperationException e) {
      return Option.empty();
    }
  }

  private static HoodieStorage newStorage() {
    return new HoodieHadoopStorage(HadoopFSUtils.getFs(DEFAULT_URI, getDefaultStorageConf()));
  }
}
