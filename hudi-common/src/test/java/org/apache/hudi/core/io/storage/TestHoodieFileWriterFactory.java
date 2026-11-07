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
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Tests for the format dispatch in {@link HoodieFileWriterFactory}, asserting the extension-based
 * entry point dispatches to the writer creation hook of the matching {@link HoodieFileFormat}.
 */
class TestHoodieFileWriterFactory {

  private static final HoodieConfig CONFIG = new HoodieConfig();
  private static final String INSTANT_TIME = "001";

  private enum WriterHook {
    PARQUET, HFILE, ORC, LANCE, VORTEX
  }

  /**
   * Overrides every path-based writer creation hook to record which hook the factory dispatched to.
   */
  private static class TrackingFileWriterFactory extends HoodieFileWriterFactory {
    private WriterHook lastHook;
    private StoragePath lastPath;

    TrackingFileWriterFactory() {
      super(mock(HoodieStorage.class));
    }

    private HoodieFileWriter track(WriterHook hook, StoragePath path) {
      this.lastHook = hook;
      this.lastPath = path;
      return mock(HoodieFileWriter.class);
    }

    @Override
    protected HoodieFileWriter newParquetFileWriter(String instantTime, StoragePath path, HoodieConfig config,
                                                    HoodieSchema schema, TaskContextSupplier taskContextSupplier) {
      return track(WriterHook.PARQUET, path);
    }

    @Override
    protected HoodieFileWriter newHFileFileWriter(String instantTime, StoragePath path, HoodieConfig config,
                                                  HoodieSchema schema, TaskContextSupplier taskContextSupplier) {
      return track(WriterHook.HFILE, path);
    }

    @Override
    protected HoodieFileWriter newOrcFileWriter(String instantTime, StoragePath path, HoodieConfig config,
                                                HoodieSchema schema, TaskContextSupplier taskContextSupplier) {
      return track(WriterHook.ORC, path);
    }

    @Override
    protected HoodieFileWriter newLanceFileWriter(String instantTime, StoragePath path, HoodieConfig config,
                                                  HoodieSchema schema, TaskContextSupplier taskContextSupplier) {
      return track(WriterHook.LANCE, path);
    }

    @Override
    protected HoodieFileWriter newVortexFileWriter(String instantTime, StoragePath path, HoodieConfig config,
                                                   HoodieSchema schema, TaskContextSupplier taskContextSupplier) {
      return track(WriterHook.VORTEX, path);
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieFileFormat.class, names = {"HOODIE_LOG"}, mode = EnumSource.Mode.EXCLUDE)
  void extensionEntryPointDispatchesToFormatHook(HoodieFileFormat format) throws IOException {
    StoragePath path = new StoragePath("/partition/path/f1_1-0-1_000" + format.getFileExtension());
    TrackingFileWriterFactory factory = new TrackingFileWriterFactory();
    factory.getFileWriterByFormat(format.getFileExtension(), INSTANT_TIME, path, CONFIG, null, null);
    assertEquals(WriterHook.valueOf(format.name()), factory.lastHook);
    assertEquals(path, factory.lastPath);
  }

  @Test
  void unsupportedExtensionThrows() {
    TrackingFileWriterFactory factory = new TrackingFileWriterFactory();
    StoragePath path = new StoragePath("/partition/path/f1_1-0-1_000.xyz");

    Throwable unknown = assertThrows(UnsupportedOperationException.class,
        () -> factory.getFileWriterByFormat(".xyz", INSTANT_TIME, path, CONFIG, null, null));
    assertEquals(".xyz format not supported yet.", unknown.getMessage());

    Throwable logExtension = assertThrows(UnsupportedOperationException.class,
        () -> factory.getFileWriterByFormat(".log", INSTANT_TIME, path, CONFIG, null, null));
    assertEquals(".log format not supported yet.", logExtension.getMessage());
  }
}
