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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.metadata.HoodieTableMetadataUtil.ColumnStatsCollector;

import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestColumnStatsStreamingTracker {
  private static final HoodieSchema SCHEMA = HoodieSchema.parse(
      "{\"type\":\"record\",\"name\":\"stats\",\"fields\":[{\"name\":\"score\",\"type\":\"int\"}]}");

  @TempDir
  Path tempDir;

  private HoodieWriteConfig config(boolean enabled) {
    return HoodieWriteConfig.newBuilder().withPath(tempDir.toString()).withSchema(SCHEMA.toString())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .withMetadataIndexColumnStats(enabled).withColumnStatsIndexForColumns("score").build()).build();
  }

  @ParameterizedTest
  @EnumSource(value = HoodieFileFormat.class, names = {"LANCE", "PARQUET"})
  void testDisabledOrOtherFormatsDoNotInspectRows(HoodieFileFormat format) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(tempDir.toString(), format);
    HoodieFileWriter<?> writer = mock(HoodieFileWriter.class);
    Option<ColumnStatsCollector> collector = ColumnStatsStreamingTracker.createCollector(
        writer, config(format == HoodieFileFormat.PARQUET), metaClient,
        () -> {
          throw new AssertionError("Schema must not be resolved when collection is disabled");
        },
        "file" + format.getFileExtension(), HoodieRecord.HoodieRecordType.AVRO);
    assertFalse(collector.isPresent());
    verify(writer, never()).setColumnStatsCollector(any());
  }

  @Test
  void testSupportedWriterCollectsRowsAndUnsupportedWriterFallsBack() throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(tempDir.toString(), HoodieFileFormat.LANCE);
    HoodieFileWriter<?> writer = mock(HoodieFileWriter.class);
    assertFalse(ColumnStatsStreamingTracker.createCollector(writer, config(true), metaClient,
        () -> SCHEMA, "file.lance", HoodieRecord.HoodieRecordType.AVRO).isPresent());

    when(writer.setColumnStatsCollector(any())).thenReturn(true);
    ColumnStatsCollector collector = ColumnStatsStreamingTracker.createCollector(writer, config(true), metaClient,
        () -> SCHEMA, "file.lance", HoodieRecord.HoodieRecordType.AVRO).get();
    ArgumentCaptor<Consumer<HoodieRecord>> callback = ArgumentCaptor.forClass(Consumer.class);
    verify(writer, org.mockito.Mockito.times(2)).setColumnStatsCollector(callback.capture());
    GenericData.Record row = new GenericData.Record(SCHEMA.toAvroSchema());
    row.put("score", 8);
    callback.getValue().accept(new HoodieAvroIndexedRecord(row));
    row.put("score", -3);
    callback.getValue().accept(new HoodieAvroIndexedRecord(row));
    assertEquals(-3, collector.getColumnStats().get("score").getMinValue());
    assertEquals(8, collector.getColumnStats().get("score").getMaxValue());
    assertEquals(2, collector.getColumnStats().get("score").getValueCount());
    assertTrue(collector.getColumnStats().get("score").getFilePath().endsWith("file.lance"));
  }
}
