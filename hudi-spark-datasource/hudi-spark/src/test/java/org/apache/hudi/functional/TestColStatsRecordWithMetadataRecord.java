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

package org.apache.hudi.functional;

import org.apache.hudi.avro.model.DateWrapper;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.io.storage.HoodieSeekingFileReader;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestColStatsRecordWithMetadataRecord extends HoodieSparkClientTestHarness {

  private static final Random RANDOM = new Random();

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieCreateHandle");
    initPath();
    initHoodieStorage();
    initTestDataGenerator();
    initMetaClient();
    initTimelineService();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testRowCreateHandle() throws Exception {

    // create a data table which will auto create mdt table as well
    HoodieWriteConfig cfg = getConfig();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      writeData(client, InProcessTimeGenerator.createNewInstantTime(), 100, false);
    }

    String fileName = "file.parquet";
    String targetColName = "c1";

    Date minDate = new Date(1000 * 60 * 60 * 10);
    Date maxDate = new Date(1000 * 60 * 60 * 60);

    HoodieColumnRangeMetadata<Comparable> expectedColStats =
        HoodieColumnRangeMetadata.<Comparable>create(fileName, targetColName, minDate, maxDate, 5, 1000, 123456, 123456);

    // let's ensure this record gets serialized as DateWrapper
    assertEquals(DateWrapper.class.getCanonicalName(), ((HoodieMetadataPayload) HoodieMetadataPayload.createColumnStatsRecords("p1", Collections.singletonList(expectedColStats), false)
        .collect(Collectors.toList()).get(0).getData()).getColumnStatMetadata().get().getMinValue().getClass().getCanonicalName());

    // create mdt records
    HoodieRecord<HoodieMetadataPayload> columnStatsRecord =
        HoodieMetadataPayload.createColumnStatsRecords("p1", Collections.singletonList(expectedColStats), false)
            .collect(Collectors.toList()).get(0);

    HoodieWriteConfig mdtWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(cfg, HoodieFailedWritesCleaningPolicy.EAGER);
    HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder().setBasePath(mdtWriteConfig.getBasePath()).setConf(context.getStorageConf().newInstance()).build();

    HoodieTable table = HoodieSparkTable.create(mdtWriteConfig, context, mdtMetaClient);
    String newCommitTime = InProcessTimeGenerator.createNewInstantTime();
    HoodieCreateHandle handle = new HoodieCreateHandle(mdtWriteConfig, newCommitTime, table, COLUMN_STATS.getPartitionPath(), "col-stats-00001-0", new PhoneyTaskContextSupplier());

    // write the record to hfile.
    handle.write(columnStatsRecord, new Schema.Parser().parse(mdtWriteConfig.getSchema()), new TypedProperties());

    WriteStatus writeStatus = (WriteStatus) handle.close().get(0);
    String filePath = writeStatus.getStat().getPath();

    // read the hfile using base file reader.
    StoragePath baseFilePath = new StoragePath(mdtMetaClient.getBasePath() + "/" + filePath);
    HoodieSeekingFileReader baseFileReader = (HoodieSeekingFileReader<?>) HoodieIOFactory.getIOFactory(mdtMetaClient.getStorage())
        .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, baseFilePath);

    ClosableIterator itr = baseFileReader.getRecordIterator();
    List<HoodieRecord<HoodieMetadataPayload>> allRecords = new ArrayList<>();
    while (itr.hasNext()) {
      GenericRecord genericRecord = (GenericRecord) ((HoodieRecord) itr.next()).getData();
      HoodieRecord<HoodieMetadataPayload> mdtRec = SpillableMapUtils.convertToHoodieRecordPayload(genericRecord,
          mdtWriteConfig.getPayloadClass(), mdtWriteConfig.getPreCombineField(),
          Pair.of(mdtMetaClient.getTableConfig().getRecordKeyFieldProp(), mdtMetaClient.getTableConfig().getPartitionFieldProp()),
          false, Option.of(COLUMN_STATS.getPartitionPath()), Option.empty());
      allRecords.add(mdtRec);
    }

    // validate the min and max values.
    HoodieMetadataColumnStats actualColStatsMetadata = allRecords.get(0).getData().getColumnStatMetadata().get();
    HoodieMetadataColumnStats expectedColStatsMetadata = ((HoodieMetadataPayload) HoodieMetadataPayload.createColumnStatsRecords("p1", Collections.singletonList(expectedColStats), false)
        .collect(Collectors.toList()).get(0).getData()).getColumnStatMetadata().get();
    assertEquals(expectedColStatsMetadata.getMinValue().getClass().getCanonicalName(), actualColStatsMetadata.getMinValue().getClass().getCanonicalName());
    assertEquals(expectedColStatsMetadata.getMinValue(), actualColStatsMetadata.getMinValue());
    assertEquals(expectedColStatsMetadata.getMaxValue(), actualColStatsMetadata.getMaxValue());
  }

  private List<WriteStatus> writeData(SparkRDDWriteClient client, String instant, int numRecords, boolean doCommit) {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    JavaRDD records = jsc.parallelize(dataGen.generateInserts(instant, numRecords), 2);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    client.startCommitWithTime(instant);
    List<WriteStatus> writeStatuses = client.upsert(records, instant).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatuses);
    if (doCommit) {
      List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      boolean committed = client.commitStats(instant, writeStats, Option.empty(), metaClient.getCommitActionType());
      Assertions.assertTrue(committed);
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatuses;
  }

  class PhoneyTaskContextSupplier extends TaskContextSupplier {

    @Override
    public Supplier<Integer> getPartitionIdSupplier() {
      return () -> 1;
    }

    @Override
    public Supplier<Integer> getStageIdSupplier() {
      return () -> 1;
    }

    @Override
    public Supplier<Long> getAttemptIdSupplier() {
      return () -> 1L;
    }

    @Override
    public Option<String> getProperty(EngineProperty prop) {
      return Option.empty();
    }
  }
}
