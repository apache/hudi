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
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.HFileUtils;
import org.apache.hudi.common.util.LanceUtils;
import org.apache.hudi.common.util.OrcUtils;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.VortexUtils;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.io.storage.hadoop.HoodieHadoopIOFactory;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieHadoopIOFactory}
 */
class TestHoodieHadoopIOFactory {
  private final HoodieIOFactory ioFactory = new HoodieHadoopIOFactory(HoodieTestUtils.getDefaultStorage());

  @Test
  void testGetFileFormatUtils() {
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

  /**
   * The extension if-chain in {@link HoodieIOFactory#getFileFormatUtils(StoragePath)} and the format switch in
   * {@link HoodieHadoopIOFactory#getFileFormatUtils(HoodieFileFormat)} must both cover every base file format;
   * a case missing from either surfaces here as {@link UnsupportedOperationException}.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieFileFormat.class, mode = EnumSource.Mode.EXCLUDE, names = {"HOODIE_LOG"})
  void testGetFileFormatUtilsEntryPointsCoverEveryBaseFileFormat(HoodieFileFormat format) {
    FileFormatUtils byFormat = ioFactory.getFileFormatUtils(format);
    FileFormatUtils byPath = ioFactory.getFileFormatUtils(new StoragePath("file:///a/b" + format.getFileExtension()));
    assertEquals(byFormat.getClass(), byPath.getClass());
  }
}
