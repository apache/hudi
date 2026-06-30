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

package org.apache.hudi.io.cdc;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.log.LogFileCreationCallback;
import org.apache.hudi.common.table.log.NativeLogFooterMetadata;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHoodieNativeCDCFileWriter {

  @Test
  public void testAddsLogBlockHeaderToFooterMetadata() throws Exception {
    String instantTime = "100";
    String schemaString = "{\"type\":\"record\",\"name\":\"cdc\",\"fields\":[]}";
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    HoodieSchema cdcSchema = mock(HoodieSchema.class);
    HoodieFileWriter fileWriter = mock(HoodieFileWriter.class);
    StoragePath parentPath = new StoragePath("/tmp/partition");

    when(config.getProps()).thenReturn(new TypedProperties());
    when(cdcSchema.toString()).thenReturn(schemaString);
    when(storage.exists(any(StoragePath.class))).thenReturn(false);

    try (MockedStatic<HoodieFileWriterFactory> writerFactory = mockStatic(HoodieFileWriterFactory.class)) {
      writerFactory.when(() -> HoodieFileWriterFactory.getFileWriter(
              eq(instantTime), any(StoragePath.class), eq(storage), eq(config), eq(cdcSchema),
              any(TaskContextSupplier.class), eq(HoodieRecord.HoodieRecordType.AVRO)))
          .thenReturn(fileWriter);

      HoodieNativeCDCFileWriter<IndexedRecord> writer = new HoodieNativeCDCFileWriter<>(
          instantTime,
          "partition",
          storage,
          config,
          cdcSchema,
          HoodieFileFormat.PARQUET,
          parentPath,
          "file-1",
          "1-0-1",
          new LogFileCreationCallback() {
          },
          mock(TaskContextSupplier.class),
          HoodieRecord.HoodieRecordType.AVRO);

      writer.write("key1", mock(IndexedRecord.class));
    }

    ArgumentCaptor<Map<String, String>> footerCaptor = ArgumentCaptor.forClass(Map.class);
    verify(fileWriter).addFooterMetadata(footerCaptor.capture());
    Map<HeaderMetadataType, String> header =
        NativeLogFooterMetadata.fromFooterMetadata(footerCaptor.getValue());
    assertEquals(instantTime, header.get(HeaderMetadataType.INSTANT_TIME));
    assertEquals(schemaString, header.get(HeaderMetadataType.SCHEMA));
  }
}
