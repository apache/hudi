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

package org.apache.hudi.testutils;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StoragePath;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.bootstrap.index.TestBootstrapIndex.generateBootstrapIndex;
import static org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HoodieCleanerTestBase extends HoodieClientTestBase {
  protected static HoodieCommitMetadata generateCommitMetadata(
      String instantTime, Map<String, List<String>> partitionToFilePaths) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestTable.PHONY_TABLE_SCHEMA);
    partitionToFilePaths.forEach((partitionPath, fileList) -> fileList.forEach(f -> {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPartitionPath(partitionPath);
      writeStat.setPath(partitionPath + "/" + f);
      writeStat.setFileId(f);
      writeStat.setTotalWriteBytes(1);
      writeStat.setFileSizeInBytes(1);
      metadata.addWriteStat(partitionPath, writeStat);
    }));
    return metadata;
  }

  /**
   * Helper to run cleaner and collect Clean Stats.
   *
   * @param config HoodieWriteConfig
   */
  protected List<HoodieCleanStat> runCleaner(HoodieWriteConfig config) throws IOException {
    return runCleaner(config, false, false, 1, false);
  }

  protected List<HoodieCleanStat> runCleanerWithInstantFormat(HoodieWriteConfig config, boolean needInstantInHudiFormat) throws IOException {
    return runCleaner(config, false, false, 1, needInstantInHudiFormat);
  }

  protected List<HoodieCleanStat> runCleaner(HoodieWriteConfig config, int firstCommitSequence, boolean needInstantInHudiFormat) throws IOException {
    return runCleaner(config, false, false, firstCommitSequence, needInstantInHudiFormat);
  }

  protected List<HoodieCleanStat> runCleaner(HoodieWriteConfig config, boolean simulateRetryFailure) throws IOException {
    return runCleaner(config, simulateRetryFailure, false, 1, false);
  }

  protected List<HoodieCleanStat> runCleaner(
      HoodieWriteConfig config, boolean simulateRetryFailure, boolean simulateMetadataFailure) throws IOException {
    return runCleaner(config, simulateRetryFailure, simulateMetadataFailure, 1, false);
  }

  /**
   * Helper to run cleaner and collect Clean Stats.
   *
   * @param config HoodieWriteConfig
   */
  protected List<HoodieCleanStat> runCleaner(
      HoodieWriteConfig config, boolean simulateRetryFailure, boolean simulateMetadataFailure,
      Integer firstCommitSequence, boolean needInstantInHudiFormat) throws IOException {
    SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(config);
    String cleanInstantTs = needInstantInHudiFormat ? makeNewCommitTime(firstCommitSequence, "%014d") : makeNewCommitTime(firstCommitSequence, "%09d");
    HoodieCleanMetadata cleanMetadata1 = writeClient.clean(cleanInstantTs);

    if (null == cleanMetadata1) {
      return new ArrayList<>();
    }

    if (simulateRetryFailure) {
      HoodieInstant completedCleanInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLEAN_ACTION, cleanInstantTs);
      HoodieCleanMetadata metadata = CleanerUtils.getCleanerMetadata(metaClient, completedCleanInstant);
      metadata.getPartitionMetadata().values().forEach(p -> {
        String dirPath = metaClient.getBasePath() + "/" + p.getPartitionPath();
        p.getSuccessDeleteFiles().forEach(p2 -> {
          try {
            metaClient.getStorage().create(new StoragePath(dirPath, p2), true).close();
          } catch (IOException e) {
            throw new HoodieIOException(e.getMessage(), e);
          }
        });
      });
      metaClient.reloadActiveTimeline().revertToInflight(completedCleanInstant);

      if (config.isMetadataTableEnabled() && simulateMetadataFailure) {
        // Simulate the failure of corresponding instant in the metadata table
        HoodieTableMetaClient metadataMetaClient = HoodieTestUtils.createMetaClient(
            metaClient.getStorageConf(),
            HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath()), metaClient.getTableConfig().getTableVersion());
        HoodieInstant deltaCommit = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, cleanInstantTs);
        metadataMetaClient.reloadActiveTimeline().revertToInflight(deltaCommit);
      }

      // retry clean operation again
      writeClient.clean();
      final HoodieCleanMetadata retriedCleanMetadata = CleanerUtils.getCleanerMetadata(HoodieTableMetaClient.reload(metaClient), completedCleanInstant);
      cleanMetadata1.getPartitionMetadata().keySet().forEach(k -> {
        HoodieCleanPartitionMetadata p1 = cleanMetadata1.getPartitionMetadata().get(k);
        HoodieCleanPartitionMetadata p2 = retriedCleanMetadata.getPartitionMetadata().get(k);
        assertEquals(p1.getDeletePathPatterns(), p2.getDeletePathPatterns());
        assertEquals(p1.getSuccessDeleteFiles(), p2.getSuccessDeleteFiles());
        assertEquals(p1.getFailedDeleteFiles(), p2.getFailedDeleteFiles());
        assertEquals(p1.getPartitionPath(), p2.getPartitionPath());
        assertEquals(k, p1.getPartitionPath());
      });
    }

    Map<String, HoodieCleanStat> cleanStatMap = cleanMetadata1.getPartitionMetadata().values().stream()
        .map(x -> new HoodieCleanStat.Builder().withPartitionPath(x.getPartitionPath())
            .withFailedDeletes(x.getFailedDeleteFiles()).withSuccessfulDeletes(x.getSuccessDeleteFiles())
            .withPolicy(HoodieCleaningPolicy.valueOf(x.getPolicy())).withDeletePathPattern(x.getDeletePathPatterns())
            .withEarliestCommitRetained(Option.ofNullable(cleanMetadata1.getEarliestCommitToRetain() != null
                ? INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "000")
                : null))
            .build())
        .collect(Collectors.toMap(HoodieCleanStat::getPartitionPath, x -> x));
    cleanMetadata1.getBootstrapPartitionMetadata().values().forEach(x -> {
      HoodieCleanStat s = cleanStatMap.get(x.getPartitionPath());
      cleanStatMap.put(x.getPartitionPath(), new HoodieCleanStat.Builder().withPartitionPath(x.getPartitionPath())
          .withFailedDeletes(s.getFailedDeleteFiles()).withSuccessfulDeletes(s.getSuccessDeleteFiles())
          .withPolicy(HoodieCleaningPolicy.valueOf(x.getPolicy())).withDeletePathPattern(s.getDeletePathPatterns())
          .withEarliestCommitRetained(Option.ofNullable(s.getEarliestCommitToRetain())
              .map(y -> INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, y)))
          .withSuccessfulDeleteBootstrapBaseFiles(x.getSuccessDeleteFiles())
          .withFailedDeleteBootstrapBaseFiles(x.getFailedDeleteFiles())
          .withDeleteBootstrapBasePathPatterns(x.getDeletePathPatterns()).build());
    });
    return new ArrayList<>(cleanStatMap.values());
  }

  public void commitWithMdt(String instantTime, Map<String, List<String>> partToFileId,
                            HoodieTestTable testTable, HoodieWriteConfig config) throws Exception {
    commitWithMdt(instantTime, partToFileId, testTable, config, true, false);
  }

  public void commitWithMdt(String instantTime, Map<String, List<String>> partToFileId,
                            HoodieTestTable testTable, HoodieWriteConfig config, boolean addBaseFiles, boolean addLogFiles) throws Exception {
    testTable.addInflightCommit(instantTime);
    Map<String, List<String>> partToFileIds = new HashMap<>();
    partToFileId.forEach((key, value) -> {
      try {
        List<String> files = new ArrayList<>();
        FileCreateUtilsLegacy.createPartitionMetaFile(basePath, key);
        if (addBaseFiles) {
          files.addAll(testTable.withBaseFilesInPartition(key, value.toArray(new String[0])).getValue());
        }
        if (addLogFiles) {
          value.forEach(logFilePrefix -> {
            try {
              files.addAll(testTable.withLogFile(key, logFilePrefix, 1, 2).getValue());
            } catch (Exception e) {
              e.printStackTrace();
            }
          });
        }
        partToFileIds.put(key, files);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    HoodieCommitMetadata commitMeta = generateCommitMetadata(instantTime, partToFileIds);
    try (HoodieTableMetadataWriter metadataWriter = getMetadataWriter(config)) {
      metadataWriter.performTableServices(Option.of(instantTime), true);
      metadataWriter.update(commitMeta, instantTime);
      metaClient.getActiveTimeline().saveAsComplete(
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime),
          Option.of(commitMeta), HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
      metaClient = HoodieTableMetaClient.reload(metaClient);
    }
  }

  protected HoodieTableMetadataWriter getMetadataWriter(HoodieWriteConfig config) {
    return SparkHoodieBackedTableMetadataWriter.create(storageConf, config, context);
  }

  protected HoodieTestTable tearDownTestTableAndReinit(HoodieTestTable testTable, HoodieWriteConfig config) throws Exception {
    testTable.close();
    return HoodieMetadataTestTable.of(metaClient, getMetadataWriter(config), Option.of(context));
  }

  /**
   * Generate Bootstrap index, bootstrap base file and corresponding metaClient.
   *
   * @return Partition to BootstrapFileMapping Map
   * @throws IOException
   */
  protected Map<String, List<BootstrapFileMapping>> generateBootstrapIndexAndSourceData(String... partitions) throws IOException {
    // create bootstrap source data path
    java.nio.file.Path sourcePath = tempDir.resolve("data");
    java.nio.file.Files.createDirectories(sourcePath);
    assertTrue(new File(sourcePath.toString()).exists());

    // recreate metaClient with Bootstrap base path
    metaClient = HoodieTestUtils.init(basePath, getTableType(), sourcePath.toString(), true);

    // generate bootstrap index
    Map<String, List<BootstrapFileMapping>> bootstrapMapping = generateBootstrapIndex(metaClient, sourcePath.toString(), partitions, 1);

    for (Map.Entry<String, List<BootstrapFileMapping>> entry : bootstrapMapping.entrySet()) {
      new File(sourcePath + "/" + entry.getKey()).mkdirs();
      assertTrue(new File(entry.getValue().get(0).getBootstrapFileStatus().getPath().getUri()).createNewFile());
    }
    return bootstrapMapping;
  }
}
