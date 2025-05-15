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

package org.apache.hudi.table.functional;

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.functional.TestHoodieBackedMetadata;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantFileNameGeneratorV1;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieSparkRollback extends SparkClientFunctionalTestHarness {

  private String basePath;

  private void initBasePath() {
    basePath = basePath().substring(7);
  }

  private SparkRDDWriteClient getHoodieWriteClient(Boolean autoCommitEnabled) throws IOException {
    return getHoodieWriteClient(getConfigToTestMDTRollbacks(autoCommitEnabled));
  }

  protected List<HoodieRecord> insertRecords(SparkRDDWriteClient client, HoodieTestDataGenerator dataGen, String commitTime) {
    /*
     * Write 1 (only inserts, written as base file)
     */
    WriteClientTestUtils.startCommitWithTime(client, commitTime);

    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 20);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

    List<WriteStatus> statuses = client.upsert(writeRecords, commitTime).collect();
    client.commit(commitTime, jsc().parallelize(statuses));
    assertNoWriteErrors(statuses);
    return records;
  }

  protected List<WriteStatus> updateRecords(SparkRDDWriteClient client, HoodieTestDataGenerator dataGen, String commitTime,
                                          List<HoodieRecord> records) throws IOException {
    WriteClientTestUtils.startCommitWithTime(client, commitTime);

    records = dataGen.generateUpdates(commitTime, records);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    List<WriteStatus> statuses = client.upsert(writeRecords, commitTime).collect();
    client.commit(commitTime, jsc().parallelize(statuses));
    assertNoWriteErrors(statuses);
    return statuses;
  }

  protected HoodieWriteConfig getConfigToTestMDTRollbacks(Boolean autoCommit) {
    return getConfigToTestMDTRollbacks(autoCommit, true);
  }

  protected HoodieWriteConfig getConfigToTestMDTRollbacks(Boolean autoCommit, Boolean mdtEnable) {
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(getPropertiesForKeyGen(true))
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withDeleteParallelism(2)
        .withEmbeddedTimelineServerEnabled(false).forTable("test-trip-table")
        .withRollbackUsingMarkers(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(mdtEnable).build())
        .build();
    cfg.setValue(HoodieWriteConfig.WRITE_TABLE_VERSION, "6");
    return cfg;
  }

  /**
   * Scenario: data table is updated, no changes to MDT
   */
  protected void testRollbackWithFailurePreMDTTableVersionSix(HoodieTableType tableType) throws Exception {
    initBasePath();
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.VERSION.key(), "6");
    props.setProperty(HoodieTableConfig.TIMELINE_LAYOUT_VERSION.key(), "1");
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, props);
    SparkRDDWriteClient client =  getHoodieWriteClient(true);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    //normal insert
    List<HoodieRecord> records = insertRecords(client, dataGen, "001");
    //update but don't commit
    client = getHoodieWriteClient(false);
    updateRecords(client, dataGen, "002", records);
    //New update will trigger rollback and we will commit this time
    client = getHoodieWriteClient(true);
    updateRecords(client, dataGen, "003", records);
    //validate that metadata table file listing matches reality
    metaClient = HoodieTableMetaClient.reload(metaClient);
    TestHoodieBackedMetadata.validateMetadata(getConfigToTestMDTRollbacks(true), Option.empty(), hoodieStorage(), basePath, metaClient,
        storageConf(), new HoodieSparkEngineContext(jsc()), TestHoodieBackedMetadata.metadata(client, hoodieStorage()), client, HoodieTimer.start());
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion());
  }

  /**
   * Scenario: data table is updated, deltacommit is completed in MDT
   */
  protected void testRollbackWithFailurePostMDTTableVersionSix(HoodieTableType tableType) throws Exception {
    testRollbackWithFailurePostMDTTableVersionSix(tableType, false);
  }

  protected void testRollbackWithFailurePostMDTTableVersionSix(HoodieTableType tableType, Boolean failRollback) throws Exception {
    initBasePath();
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.VERSION.key(), "6");
    props.setProperty(HoodieTableConfig.TIMELINE_LAYOUT_VERSION.key(), "1");
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, props);
    HoodieWriteConfig cfg = getConfigToTestMDTRollbacks(true);
    SparkRDDWriteClient client =  getHoodieWriteClient(cfg);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    //normal insert
    List<HoodieRecord> records = insertRecords(client, dataGen, "001");
    //New update and commit so that the MDT has the update
    List<WriteStatus> statuses = updateRecords(client, dataGen, "002", records);

    //delete commit from timeline
    metaClient = HoodieTableMetaClient.reload(metaClient);
    String filename = new InstantFileNameGeneratorV1().getFileName(metaClient.getActiveTimeline().lastInstant().get());
    File commit = new File(metaClient.getBasePath().toString().substring(5) + "/.hoodie/" + filename);
    assertTrue(commit.delete());
    metaClient.reloadActiveTimeline();

    //Add back the marker files to mimic that we haven't committed yet
    statuses.forEach(s -> {
      try {
        recreateMarkerFile(cfg, "002", s);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    if (failRollback) {
      copyOut(tableType, "002");
      //disable MDT so we don't copy it
      client = getHoodieWriteClient(getConfigToTestMDTRollbacks(true, false));
      assertTrue(client.rollback("002", "003"));
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieInstant lastInstant = metaClient.getActiveTimeline().lastInstant().get();
      assertEquals(HoodieTimeline.ROLLBACK_ACTION, lastInstant.getAction());
      HoodieRollbackMetadata rollbackMetadata = metaClient.getActiveTimeline().readInstantContent(lastInstant, HoodieRollbackMetadata.class);
      copyIn(tableType, "002");
      rollbackMetadata.getPartitionMetadata().forEach((partition, metadata) -> metadata.getRollbackLogFiles().forEach((n, k) -> recreateMarkerFile(cfg, "003", partition, n)));
      rollbackMetadata.getPartitionMetadata().forEach((partition, metadata) -> metadata.getLogFilesFromFailedCommit().forEach((n, k) -> recreateMarkerFile(cfg, "002", partition, n)));
      commit = new File(metaClient.getBasePath().toString().substring(5) + "/.hoodie/" + new InstantFileNameGeneratorV1().getFileName(lastInstant));
      assertTrue(commit.delete());
      metaClient.reloadActiveTimeline();
    }

    //now we are at a state that we would be at if a write failed after writing to MDT but before commit is finished

    //New update will trigger rollback and we will commit this time
    client = getHoodieWriteClient(getConfigToTestMDTRollbacks(true, true));
    updateRecords(client, dataGen, "004", records);
    //validate that metadata table file listing matches reality
    metaClient = HoodieTableMetaClient.reload(metaClient);
    TestHoodieBackedMetadata.validateMetadata(cfg, Option.empty(), hoodieStorage(), basePath, metaClient, storageConf(), new HoodieSparkEngineContext(jsc()),
        TestHoodieBackedMetadata.metadata(client, hoodieStorage()), client, HoodieTimer.start());
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion());
  }

  private void copyOut(HoodieTableType tableType, String commitTime) throws IOException {
    File tmpDir = new File(basePath, ".tmpdir");
    assertTrue(tmpDir.mkdir());
    String commitAction = (tableType.equals(COPY_ON_WRITE) ? ".commit" : ".deltacommit");
    String metaDir = basePath + ".hoodie/";
    String inflight = commitTime + (tableType.equals(COPY_ON_WRITE) ? "" : commitAction) + ".inflight";
    Files.copy(new File(metaDir + inflight).toPath(), tmpDir.toPath().resolve(inflight), StandardCopyOption.REPLACE_EXISTING);
    String requested = commitTime + commitAction + ".requested";
    Files.copy(new File(metaDir + requested).toPath(), tmpDir.toPath().resolve(requested), StandardCopyOption.REPLACE_EXISTING);
  }

  private void copyIn(HoodieTableType tableType, String commitTime) throws IOException {
    Path tmpDir = new File(basePath, ".tmpdir").toPath();
    String commitAction = (tableType.equals(COPY_ON_WRITE) ? ".commit" : ".deltacommit");
    String metaDir = basePath + ".hoodie/";
    String inflight = commitTime + (tableType.equals(COPY_ON_WRITE) ? "" : commitAction) + ".inflight";
    Files.copy(tmpDir.resolve(inflight), new File(metaDir + inflight).toPath(), StandardCopyOption.REPLACE_EXISTING);
    String requested = commitTime + commitAction + ".requested";
    Files.copy(tmpDir.resolve(requested), new File(metaDir + requested).toPath(), StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Scenario: data table is updated, deltacommit of interest is inflight in MDT
   */
  protected void testRollbackWithFailureinMDTTableVersionSix(HoodieTableType tableType) throws Exception {
    initBasePath();
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.VERSION.key(), "6");
    props.setProperty(HoodieTableConfig.TIMELINE_LAYOUT_VERSION.key(), "1");
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, props);
    HoodieWriteConfig cfg = getConfigToTestMDTRollbacks(true);
    cfg.setValue(HoodieWriteConfig.WRITE_TABLE_VERSION, "6");
    SparkRDDWriteClient client =  getHoodieWriteClient(cfg);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    //normal insert
    List<HoodieRecord> records = insertRecords(client, dataGen, "001");
    //New update and commit
    List<WriteStatus> statuses = updateRecords(client, dataGen, "002", records);

    //delete commit from timeline
    metaClient = HoodieTableMetaClient.reload(metaClient);
    String filename = new InstantFileNameGeneratorV1().getFileName(metaClient.getActiveTimeline().lastInstant().get());
    File deltacommit = new File(metaClient.getBasePath().toString().substring(5) + "/.hoodie/" + filename);
    assertTrue(deltacommit.delete());
    metaClient.reloadActiveTimeline();

    //Add back the marker files to mimic that we haven't committed yet
    statuses.forEach(s -> {
      try {
        recreateMarkerFile(cfg, "002", s);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    //Make the MDT appear to fail mid write by deleting the commit in the MDT timline. The MDT does not use markers so we do not need to recreate them
    String metadataBasePath = basePath + "/.hoodie/metadata";
    HoodieTableMetaClient metadataMetaClient =  HoodieTableMetaClient.builder().setConf(storageConf()).setBasePath(metadataBasePath).build();
    HoodieInstant latestCommitInstant = metadataMetaClient.getActiveTimeline().lastInstant().get();
    File metadatadeltacommit = new File(metadataBasePath + "/.hoodie/" + new InstantFileNameGeneratorV1().getFileName(latestCommitInstant));
    assertTrue(metadatadeltacommit.delete());

    //New update will trigger rollback and we will commit this time
    updateRecords(client, dataGen, "003", records);
    //validate that metadata table file listing matches reality
    metaClient = HoodieTableMetaClient.reload(metaClient);
    TestHoodieBackedMetadata.validateMetadata(cfg, Option.empty(), hoodieStorage(), basePath, metaClient,
        storageConf(), new HoodieSparkEngineContext(jsc()), TestHoodieBackedMetadata.metadata(client, hoodieStorage()), client, HoodieTimer.start());
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion());
  }

  /**
   * We are simulating scenarios where commits fail inflight. To mimic this, we need to recreate the marker files for the files that are
   * written in the "failed" commit
   * */
  protected void recreateMarkerFile(HoodieWriteConfig cfg, String commitTime, WriteStatus writeStatus) throws IOException, InterruptedException {
    HoodieWriteStat writeStat = writeStatus.getStat();
    final WriteMarkers writeMarkers = WriteMarkersFactory.get(cfg.getMarkersType(),
        HoodieSparkTable.create(cfg, context()), commitTime);
    if (writeStat instanceof HoodieDeltaWriteStat) {
      ((HoodieDeltaWriteStat) writeStat).getLogFiles().forEach(lf -> writeMarkers.create(writeStat.getPartitionPath(), lf, IOType.APPEND));
    } else {
      writeMarkers.create(writeStat.getPartitionPath(), writeStat.getPath().replace(writeStat.getPartitionPath() + "/",""), IOType.MERGE);
    }
  }

  protected void recreateMarkerFile(HoodieWriteConfig cfg, String commitTime, String partitionPath, String path) {
    final WriteMarkers writeMarkers = WriteMarkersFactory.get(cfg.getMarkersType(),
        HoodieSparkTable.create(cfg, context()), commitTime);
    writeMarkers.create(partitionPath, new File(path).getName(), IOType.APPEND);
  }
}
