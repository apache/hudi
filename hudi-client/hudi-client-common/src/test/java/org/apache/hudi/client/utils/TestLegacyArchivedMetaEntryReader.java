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

package org.apache.hudi.client.utils;

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.MetadataConversionUtils;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for {@link LegacyArchivedMetaEntryReader}.
 */
public class TestLegacyArchivedMetaEntryReader {
  private static final Logger LOG = LoggerFactory.getLogger(TestLegacyArchivedMetaEntryReader.class);

  @TempDir
  File tempFile;

  @Test
  void testReadLegacyArchivedTimeline() throws Exception {
    String tableName = "testTable";
    String tablePath = tempFile.getAbsolutePath() + Path.SEPARATOR + tableName;
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(new Configuration(), tablePath, HoodieTableType.COPY_ON_WRITE, tableName);
    prepareLegacyArchivedTimeline(metaClient);
    LegacyArchivedMetaEntryReader reader = new LegacyArchivedMetaEntryReader(metaClient);
    ClosableIterator<ActiveAction> iterator = reader.getActiveActionsIterator();
    List<ActiveAction> activeActions = new ArrayList<>();
    while (iterator.hasNext()) {
      activeActions.add(iterator.next());
    }
    assertThat(activeActions.stream().map(ActiveAction::getInstantTime).sorted().collect(Collectors.joining(",")),
        is("00000001,00000002,00000003,00000004,00000005,00000006,00000007,00000008,00000009,00000010"));
  }

  private void prepareLegacyArchivedTimeline(HoodieTableMetaClient metaClient) throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    for (int i = 1; i < 11; i++) {
      String instantTime = String.format("%08d", i);
      HoodieCommitMetadata metadata = testTable.createCommitMetadata(instantTime, WriteOperationType.INSERT, Arrays.asList("par1", "par2"), 10, false);
      testTable.addCommit(instantTime, Option.of(metadata));
    }
    List<HoodieInstant> instants = new HoodieActiveTimeline(metaClient, false).getInstantsAsStream().sorted().collect(Collectors.toList());
    // archive 2 times to have 2 log files.
    archive(metaClient, instants.subList(0, instants.size() / 2));
    archive(metaClient, instants.subList(instants.size() / 2, instants.size()));
  }

  private HoodieLogFormat.Writer openWriter(HoodieTableMetaClient metaClient) {
    try {
      return HoodieLogFormat.newWriterBuilder().onParentPath(new Path(metaClient.getArchivePath()))
          .withFileId("commits").withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION)
          .withFs(metaClient.getFs()).withDeltaCommit("").build();
    } catch (IOException e) {
      throw new HoodieException("Unable to initialize HoodieLogFormat writer", e);
    }
  }

  public void archive(HoodieTableMetaClient metaClient, List<HoodieInstant> instants) throws HoodieCommitException {
    try (HoodieLogFormat.Writer writer = openWriter(metaClient)) {
      Schema wrapperSchema = HoodieArchivedMetaEntry.getClassSchema();
      LOG.info("Wrapper schema " + wrapperSchema.toString());
      List<IndexedRecord> records = new ArrayList<>();
      for (HoodieInstant hoodieInstant : instants) {
        try {
          records.add(convertToAvroRecord(hoodieInstant, metaClient));
        } catch (Exception e) {
          LOG.error("Failed to archive commits, .commit file: " + hoodieInstant.getFileName(), e);
          throw e;
        }
      }
      writeToFile(metaClient, wrapperSchema, records, writer);
    } catch (Exception e) {
      throw new HoodieCommitException("Failed to archive commits", e);
    }
  }

  private void writeToFile(HoodieTableMetaClient metaClient, Schema wrapperSchema, List<IndexedRecord> records, HoodieLogFormat.Writer writer) throws Exception {
    if (records.size() > 0) {
      Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, wrapperSchema.toString());
      final String keyField = metaClient.getTableConfig().getRecordKeyFieldProp();
      List<HoodieRecord> indexRecords = records.stream().map(HoodieAvroIndexedRecord::new).collect(Collectors.toList());
      HoodieAvroDataBlock block = new HoodieAvroDataBlock(indexRecords, false, header, keyField);
      writer.appendBlock(block);
      records.clear();
    }
  }

  private IndexedRecord convertToAvroRecord(HoodieInstant hoodieInstant, HoodieTableMetaClient metaClient)
      throws IOException {
    return MetadataConversionUtils.createMetaWrapper(hoodieInstant, metaClient);
  }
}
