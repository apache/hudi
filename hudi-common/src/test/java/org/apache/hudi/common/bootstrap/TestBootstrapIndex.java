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

package org.apache.hudi.common.bootstrap;

import org.apache.hudi.avro.model.HoodieFSPermission;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.avro.model.HoodiePath;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex.IndexWriter;
import org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex;
import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit Tests for Bootstrap Index.
 */
public class TestBootstrapIndex extends HoodieCommonTestHarness {

  private static final String[] PARTITIONS = {"2020/03/18", "2020/03/19", "2020/03/20", "2020/03/21"};
  private static final Set<String> PARTITION_SET = Arrays.stream(PARTITIONS).collect(Collectors.toSet());
  private static final String BOOTSTRAP_BASE_PATH = "/tmp/source/data_tables/table1";

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void testBootstrapIndex() throws IOException {
    testBootstrapIndexOneRound(10);
  }

  @Test
  public void testBootstrapIndexRecreateIndex() throws IOException {
    testBootstrapIndexOneRound(10);

    HFileBootstrapIndex index = new HFileBootstrapIndex(metaClient);
    index.dropIndex();

    // Run again this time recreating bootstrap index
    testBootstrapIndexOneRound(5);
  }

  @Test
  public void testNoOpBootstrapIndex() throws IOException {
    Properties props = metaClient.getTableConfig().getProps();
    props.put(HoodieTableConfig.BOOTSTRAP_INDEX_ENABLE.key(), "false");
    Properties properties = new Properties();
    properties.putAll(props);
    HoodieTableConfig.create(metaClient.getFs(), new Path(metaClient.getMetaPath()), properties);

    metaClient = HoodieTableMetaClient.builder().setConf(metaClient.getHadoopConf()).setBasePath(basePath).build();
    BootstrapIndex bootstrapIndex = BootstrapIndex.getBootstrapIndex(metaClient);
    assert (bootstrapIndex instanceof NoOpBootstrapIndex);
  }

  @Test
  public void testBootstrapIndexConcurrent() throws Exception {
    Map<String, List<BootstrapFileMapping>> bootstrapMapping  = generateBootstrapIndex(metaClient, BOOTSTRAP_BASE_PATH, PARTITIONS, 100);
    final int numThreads = 20;
    final int numRequestsPerThread = 50;
    ExecutorService service = Executors.newFixedThreadPool(numThreads);
    try {
      List<Future<Boolean>> futureList = new ArrayList<>();
      for (int i = 0; i < numThreads; i++) {
        Future<Boolean> result = service.submit(() -> {
          for (int j = 0; j < numRequestsPerThread; j++) {
            validateBootstrapIndex(bootstrapMapping);
          }
          return true;
        });
        futureList.add(result);
      }

      for (Future<Boolean> res : futureList) {
        res.get();
      }
    } finally {
      service.shutdownNow();
    }
  }

  private void testBootstrapIndexOneRound(int numEntriesPerPartition) throws IOException {
    Map<String, List<BootstrapFileMapping>> bootstrapMapping = generateBootstrapIndex(metaClient, BOOTSTRAP_BASE_PATH, PARTITIONS, numEntriesPerPartition);
    validateBootstrapIndex(bootstrapMapping);
  }

  public static Map<String, List<BootstrapFileMapping>> generateBootstrapIndex(HoodieTableMetaClient metaClient,
      String sourceBasePath, String[] partitions, int numEntriesPerPartition) {
    Map<String, List<BootstrapFileMapping>> bootstrapMapping = generateBootstrapMapping(sourceBasePath, partitions, numEntriesPerPartition);
    BootstrapIndex index = new HFileBootstrapIndex(metaClient);
    try (IndexWriter writer = index.createWriter(sourceBasePath)) {
      writer.begin();
      bootstrapMapping.entrySet().stream().forEach(e -> writer.appendNextPartition(e.getKey(), e.getValue()));
      writer.finish();
    }
    return bootstrapMapping;
  }

  private void validateBootstrapIndex(Map<String, List<BootstrapFileMapping>> bootstrapMapping) {
    BootstrapIndex index = new HFileBootstrapIndex(metaClient);
    try (BootstrapIndex.IndexReader reader = index.createReader()) {
      List<String> indexedPartitions = reader.getIndexedPartitionPaths();
      assertEquals(bootstrapMapping.size(), indexedPartitions.size());
      indexedPartitions.forEach(partition -> assertTrue(PARTITION_SET.contains(partition)));
      long expNumFileGroupKeys = bootstrapMapping.values().stream().flatMap(Collection::stream).count();
      List<HoodieFileGroupId> fileGroupIds = reader.getIndexedFileGroupIds();
      long gotNumFileGroupKeys = fileGroupIds.size();
      assertEquals(expNumFileGroupKeys, gotNumFileGroupKeys);
      fileGroupIds.forEach(fgId -> assertTrue(PARTITION_SET.contains(fgId.getPartitionPath())));

      bootstrapMapping.entrySet().stream().forEach(e -> {
        List<BootstrapFileMapping> gotMapping = reader.getSourceFileMappingForPartition(e.getKey());
        List<BootstrapFileMapping> expected = new ArrayList<>(e.getValue());
        Collections.sort(gotMapping);
        Collections.sort(expected);
        assertEquals(expected, gotMapping, "Check for bootstrap index entries for partition " + e.getKey());
        List<HoodieFileGroupId> fileIds = e.getValue().stream().map(BootstrapFileMapping::getFileGroupId)
            .collect(Collectors.toList());
        Map<HoodieFileGroupId, BootstrapFileMapping> lookupResult = reader.getSourceFileMappingForFileIds(fileIds);
        assertEquals(fileIds.size(), lookupResult.size());
        e.getValue().forEach(x -> {
          BootstrapFileMapping res = lookupResult.get(x.getFileGroupId());
          assertNotNull(res);
          assertEquals(x.getFileId(), res.getFileId());
          assertEquals(x.getPartitionPath(), res.getPartitionPath());
          assertEquals(BOOTSTRAP_BASE_PATH, res.getBootstrapBasePath());
          assertEquals(x.getBootstrapFileStatus(), res.getBootstrapFileStatus());
          assertEquals(x.getBootstrapPartitionPath(), res.getBootstrapPartitionPath());
        });
      });
    }
  }

  private static Map<String, List<BootstrapFileMapping>> generateBootstrapMapping(String sourceBasePath,
      String[] partitions, int numEntriesPerPartition) {
    return Arrays.stream(partitions).map(partition -> {
      return Pair.of(partition, IntStream.range(0, numEntriesPerPartition).mapToObj(idx -> {
        String hudiFileId = UUID.randomUUID().toString();
        String sourceFileName = idx + HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension();
        HoodieFileStatus sourceFileStatus = HoodieFileStatus.newBuilder()
            .setPath(HoodiePath.newBuilder().setUri(sourceBasePath + "/" + partition + "/" + sourceFileName).build())
            .setLength(256 * 1024 * 1024L)
            .setAccessTime(new Date().getTime())
            .setModificationTime(new Date().getTime() + 99999)
            .setBlockReplication(2)
            .setOwner("hudi")
            .setGroup("hudi")
            .setBlockSize(128 * 1024 * 1024L)
            .setPermission(HoodieFSPermission.newBuilder().setUserAction(FsAction.ALL.name())
                .setGroupAction(FsAction.READ.name()).setOtherAction(FsAction.NONE.name()).setStickyBit(true).build())
            .build();
        return new BootstrapFileMapping(sourceBasePath, partition, partition, sourceFileStatus, hudiFileId);
      }).collect(Collectors.toList()));
    }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }
}
