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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.action.commit.HoodieMergeHelper;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests {@link HoodieMergeHandle}.
 */
public class TestMergeHandle extends BaseTestHandle {

  @Test
  public void testMergeHandleRLIStats() throws IOException {
    // init config and table
    HoodieWriteConfig config = getConfigBuilder(basePath)
        .withPopulateMetaFields(false)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withEnableRecordIndex(true).withStreamingWriteEnabled(true).build())
        .withKeyGenerator(KeyGeneratorForDataGeneratorRecords.class.getCanonicalName())
        .build();
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, new HoodieLocalEngineContext(storageConf), metaClient);
    // one round per partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    // init some args
    String fileId = UUID.randomUUID().toString();
    String instantTime = "000";

    // Create a parquet file
    config.setSchema(TRIP_EXAMPLE_SCHEMA);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});
    Pair<WriteStatus, List<HoodieRecord>> statusListPair = createParquetFile(config, table, partitionPath, fileId, instantTime, dataGenerator);
    WriteStatus writeStatus = statusListPair.getLeft();
    List<HoodieRecord> records = statusListPair.getRight();
    assertEquals(records.size(), writeStatus.getTotalRecords());
    assertEquals(0, writeStatus.getTotalErrorRecords());

    instantTime = "001";
    List<HoodieRecord> updates = dataGenerator.generateUniqueUpdates(instantTime, 10);
    HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config, instantTime, table, updates.iterator(), partitionPath, fileId, table.getTaskContextSupplier(),
        new HoodieBaseFile(writeStatus.getStat().getPath()), Option.of(new KeyGeneratorForDataGeneratorRecords(config.getProps())));
    HoodieMergeHelper.newInstance().runMerge(table, mergeHandle);
    writeStatus = mergeHandle.writeStatus;
    // verify stats after merge
    assertEquals(records.size(), writeStatus.getStat().getNumWrites());
    assertEquals(10, writeStatus.getStat().getNumUpdateWrites());
  }
}
