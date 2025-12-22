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
import org.apache.hudi.common.util.HFileUtils;
import org.apache.hudi.common.util.OrcUtils;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.storage.HoodieStorageUtils.DEFAULT_URI;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests {@link HoodieHadoopIOFactory}
 */
public class TestHoodieHadoopIOFactory {
  @Test
  public void testGetFileFormatUtils() throws IOException {
    try (HoodieStorage storage =
             new HoodieHadoopStorage(HadoopFSUtils.getFs(DEFAULT_URI, getDefaultStorageConf()))) {
      HoodieIOFactory ioFactory = new HoodieHadoopIOFactory(storage);
      assertTrue(ioFactory.getFileFormatUtils(new StoragePath("file:///a/b.parquet")) instanceof ParquetUtils);
      assertTrue(ioFactory.getFileFormatUtils(new StoragePath("file:///a/b.orc")) instanceof OrcUtils);
      assertTrue(ioFactory.getFileFormatUtils(new StoragePath("file:///a/b.hfile")) instanceof HFileUtils);
      assertThrows(
          UnsupportedOperationException.class,
          () -> ioFactory.getFileFormatUtils(new StoragePath("file:///a/b.log")));

      assertTrue(ioFactory.getFileFormatUtils(HoodieFileFormat.PARQUET) instanceof ParquetUtils);
      assertTrue(ioFactory.getFileFormatUtils(HoodieFileFormat.ORC) instanceof OrcUtils);
      assertTrue(ioFactory.getFileFormatUtils(HoodieFileFormat.HFILE) instanceof HFileUtils);
      assertThrows(
          UnsupportedOperationException.class,
          () -> ioFactory.getFileFormatUtils(HoodieFileFormat.HOODIE_LOG));
    }
  }
}
