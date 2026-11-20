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

package org.apache.hudi.core.io.storage;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Tests for the format dispatch in {@link HoodieFileReaderFactory}, in particular that the
 * {@link StoragePath} and {@link StoragePathInfo} entry points agree for every
 * {@link HoodieFileFormat} value.
 */
class TestHoodieFileReaderFactory {

  private static final HoodieConfig CONFIG = new HoodieConfig();

  private enum ReaderHook {
    PARQUET_PATH, HFILE_PATH, HFILE_PATH_INFO, ORC_PATH, LANCE_PATH, VORTEX_PATH
  }

  /**
   * Overrides every reader creation hook to record which hook the factory dispatched to.
   */
  private static class TrackingFileReaderFactory extends HoodieFileReaderFactory {
    private ReaderHook lastHook;
    private StoragePath lastPath;

    TrackingFileReaderFactory() {
      super(mock(HoodieStorage.class));
    }

    private HoodieFileReader track(ReaderHook hook, StoragePath path) {
      this.lastHook = hook;
      this.lastPath = path;
      return mock(HoodieFileReader.class);
    }

    @Override
    protected HoodieFileReader newParquetFileReader(StoragePath path) {
      return track(ReaderHook.PARQUET_PATH, path);
    }

    @Override
    protected HoodieFileReader newHFileFileReader(HoodieConfig hoodieConfig, StoragePath path,
                                                  Option<HoodieSchema> schemaOption) {
      return track(ReaderHook.HFILE_PATH, path);
    }

    @Override
    protected HoodieFileReader newHFileFileReader(HoodieConfig hoodieConfig, StoragePathInfo pathInfo,
                                                  Option<HoodieSchema> schemaOption) {
      return track(ReaderHook.HFILE_PATH_INFO, pathInfo.getPath());
    }

    @Override
    protected HoodieFileReader newOrcFileReader(StoragePath path) {
      return track(ReaderHook.ORC_PATH, path);
    }

    @Override
    protected HoodieFileReader newLanceFileReader(HoodieConfig hoodieConfig, StoragePath path) {
      return track(ReaderHook.LANCE_PATH, path);
    }

    @Override
    protected HoodieFileReader newVortexFileReader(HoodieConfig hoodieConfig, StoragePath path) {
      return track(ReaderHook.VORTEX_PATH, path);
    }
  }

  private static ReaderHook expectedPathHook(HoodieFileFormat format) {
    switch (format) {
      case PARQUET:
        return ReaderHook.PARQUET_PATH;
      case HFILE:
        return ReaderHook.HFILE_PATH;
      case ORC:
        return ReaderHook.ORC_PATH;
      case LANCE:
        return ReaderHook.LANCE_PATH;
      case VORTEX:
        return ReaderHook.VORTEX_PATH;
      default:
        throw new IllegalArgumentException("Unexpected format: " + format);
    }
  }

  private static StoragePath filePath(HoodieFileFormat format) {
    return new StoragePath("/partition/path/f1_1-0-1_000" + format.getFileExtension());
  }

  private static StoragePathInfo pathInfo(StoragePath path) {
    return new StoragePathInfo(path, 100, false, (short) 0, 0, 0);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieFileFormat.class, names = {"HOODIE_LOG"}, mode = EnumSource.Mode.EXCLUDE)
  void storagePathAndPathInfoEntryPointsAgree(HoodieFileFormat format) throws IOException {
    StoragePath path = filePath(format);

    TrackingFileReaderFactory pathFactory = new TrackingFileReaderFactory();
    pathFactory.getFileReader(CONFIG, path, format, Option.empty());

    TrackingFileReaderFactory pathInfoFactory = new TrackingFileReaderFactory();
    pathInfoFactory.getFileReader(CONFIG, pathInfo(path), format, Option.empty());

    assertEquals(expectedPathHook(format), pathFactory.lastHook);
    // HFILE dispatches to the StoragePathInfo-specific hook; all other formats
    // must dispatch to the same hook from both entry points.
    ReaderHook expectedPathInfoHook =
        format == HoodieFileFormat.HFILE ? ReaderHook.HFILE_PATH_INFO : expectedPathHook(format);
    assertEquals(expectedPathInfoHook, pathInfoFactory.lastHook);
    assertEquals(path, pathFactory.lastPath);
    assertEquals(path, pathInfoFactory.lastPath);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieFileFormat.class, names = {"HOODIE_LOG"}, mode = EnumSource.Mode.EXCLUDE)
  void extensionEntryPointDispatchesToFormatHook(HoodieFileFormat format) throws IOException {
    TrackingFileReaderFactory factory = new TrackingFileReaderFactory();
    factory.getFileReader(CONFIG, filePath(format));
    assertEquals(expectedPathHook(format), factory.lastHook);
  }

  @Test
  void logFormatThrowsForBothEntryPoints() {
    StoragePath path = filePath(HoodieFileFormat.HOODIE_LOG);
    TrackingFileReaderFactory factory = new TrackingFileReaderFactory();

    Throwable viaPath = assertThrows(UnsupportedOperationException.class,
        () -> factory.getFileReader(CONFIG, path, HoodieFileFormat.HOODIE_LOG, Option.empty()));
    Throwable viaPathInfo = assertThrows(UnsupportedOperationException.class,
        () -> factory.getFileReader(CONFIG, pathInfo(path), HoodieFileFormat.HOODIE_LOG, Option.empty()));
    assertEquals("HOODIE_LOG format not supported yet.", viaPath.getMessage());
    assertEquals(viaPath.getMessage(), viaPathInfo.getMessage());
  }

  @Test
  void unsupportedExtensionThrows() {
    TrackingFileReaderFactory factory = new TrackingFileReaderFactory();

    Throwable unknown = assertThrows(UnsupportedOperationException.class,
        () -> factory.getFileReader(CONFIG, new StoragePath("/partition/path/f1_1-0-1_000.xyz")));
    assertEquals(".xyz format not supported yet.", unknown.getMessage());

    Throwable logExtension = assertThrows(UnsupportedOperationException.class,
        () -> factory.getFileReader(CONFIG, new StoragePath("/partition/path/f1_1-0-1_000.log")));
    assertEquals(".log format not supported yet.", logExtension.getMessage());
  }
}
