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

package org.apache.hudi.io;

import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.parquet.io.HoodieParquetFileBinaryCopier;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.TestBaseHoodieTable;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.io.IOException;
import java.util.Collections;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;

class TestHoodieBinaryCopyHandle extends HoodieCommonTestHarness {
  @Test
  void testWriteFailureClosesCopierAndPreservesOriginalException() throws Exception {
    initPath();
    initMetaClient();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA).build();
    config.setValue(HoodieClusteringConfig.FILE_STITCHING_BINARY_COPY_SCHEMA_EVOLUTION_ENABLE, "true");
    HoodieTable table = new TestBaseHoodieTable(config, getEngineContext(), metaClient);
    IOException failure = new IOException("copy failed");
    IOException closeFailure = new IOException("close failed");
    try (MockedConstruction<HoodieParquetFileBinaryCopier> copiers = mockConstruction(
        HoodieParquetFileBinaryCopier.class, (copier, context) -> {
          doThrow(failure).when(copier).binaryCopy(any(), any(), any(), anyBoolean());
          doThrow(closeFailure).when(copier).close();
        })) {
      HoodieBinaryCopyHandle handle = new HoodieBinaryCopyHandle(config, "100", "partition", "file-1",
          table, new LocalTaskContextSupplier(), Collections.singletonList(new StoragePath(basePath, "input.parquet")));
      assertSame(failure, assertThrows(HoodieIOException.class, handle::write).getCause());
      assertArrayEquals(new Throwable[] {closeFailure}, failure.getSuppressed());
      assertDoesNotThrow(handle::close);
      verify(copiers.constructed().get(0)).close();
    } finally {
      cleanMetaClient();
    }
  }
}
