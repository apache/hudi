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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hudi.avro.model.HoodieFSPermission;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.avro.model.HoodiePath;
import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.model.BootstrapSourceFileMapping;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.bootstrap.BootstrapIndex.BootstrapIndexWriter;
import org.apache.hudi.common.util.collection.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit Tests for Bootstrap Index.
 */
public class TestBootstrapIndex extends HoodieCommonTestHarness {

  private static String[] PARTITIONS = {"2020/03/18", "2020/03/19", "2020/03/20", "2020/03/21"};
  private static String SOURCE_BASE_PATH = "/tmp/source/parquet_tables/table1";

  @Before
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

    BootstrapIndex index = new BootstrapIndex(metaClient);
    index.dropIndex();

    // Run again this time recreating bootstrap index
    testBootstrapIndexOneRound(5);
  }

  @Test
  public void testBootstrapIndexConcurrent() throws Exception {
    Map<String, List<BootstrapSourceFileMapping>> bootstrapMapping  = generateBootstrapIndex(100);
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
    Map<String, List<BootstrapSourceFileMapping>> bootstrapMapping = generateBootstrapIndex(numEntriesPerPartition);
    validateBootstrapIndex(bootstrapMapping);
  }

  private Map<String, List<BootstrapSourceFileMapping>> generateBootstrapIndex(int numEntriesPerPartition)
      throws IOException {
    Map<String, List<BootstrapSourceFileMapping>> bootstrapMapping = generateBootstrapMapping(numEntriesPerPartition);
    try (BootstrapIndexWriter writer = BootstrapIndex.writer(SOURCE_BASE_PATH, metaClient)) {
      List<String> partitions = bootstrapMapping.keySet().stream().sorted().collect(Collectors.toList());
      partitions.forEach(p -> writer.appendNextPartition(p, p, bootstrapMapping.get(p)));
      bootstrapMapping.values().stream().flatMap(Collection::stream)
          .sorted()
          .forEach(writer::appendNextSourceFileMapping);
      writer.commit();
    }
    return bootstrapMapping;
  }

  private void validateBootstrapIndex(Map<String, List<BootstrapSourceFileMapping>> bootstrapMapping) {
    try (BootstrapIndex index = new BootstrapIndex(metaClient)) {
      List<String> partitions = index.getAllBootstrapPartitionKeys();
      Assert.assertEquals(bootstrapMapping.size(), partitions.size());
      long expNumFileGroupKeys = bootstrapMapping.values().stream().flatMap(x -> x.stream()).count();
      long gotNumFileGroupKeys = index.getAllBootstrapFileIdKeys().size();
      Assert.assertEquals(expNumFileGroupKeys, gotNumFileGroupKeys);

      bootstrapMapping.entrySet().stream().forEach(e -> {
        List<BootstrapSourceFileMapping> gotMapping = index.getBootstrapInfoForPartition(e.getKey());
        List<BootstrapSourceFileMapping> expected = new ArrayList<>(e.getValue());
        Collections.sort(gotMapping);
        Collections.sort(expected);
        Assert.assertEquals("Check for bootstrap index entries for partition " + e.getKey(),
            expected, gotMapping);
        List<HoodieFileGroupId> fileIds = e.getValue().stream().map(BootstrapSourceFileMapping::getFileGroupId)
            .collect(Collectors.toList());
        Map<HoodieFileGroupId, BootstrapSourceFileMapping> lookupResult = index.getBootstrapInfoForFileIds(fileIds);
        Assert.assertEquals(fileIds.size(), lookupResult.size());
        e.getValue().forEach(x -> {
          BootstrapSourceFileMapping res = lookupResult.get(x.getFileGroupId());
          Assert.assertNotNull(res);
          Assert.assertEquals(x.getHudiFileId(), res.getHudiFileId());
          Assert.assertEquals(x.getHudiPartitionPath(), res.getHudiPartitionPath());
          Assert.assertEquals(SOURCE_BASE_PATH, res.getSourceBasePath());
          Assert.assertEquals(x.getSourceFileStatus(), res.getSourceFileStatus());
          Assert.assertEquals(x.getSourcePartitionPath(), res.getSourcePartitionPath());
        });
      });
    }
  }

  private Map<String, List<BootstrapSourceFileMapping>> generateBootstrapMapping(int numEntriesPerPartition) {
    return Arrays.stream(PARTITIONS).map(partition -> {
      return Pair.of(partition, IntStream.range(0, numEntriesPerPartition).mapToObj(idx -> {
        String hudiFileId = UUID.randomUUID().toString();
        System.out.println(" hudiFileId :" + hudiFileId + ", partition :" + partition);
        String sourceFileName = idx + ".parquet";
        HoodieFileStatus sourceFileStatus = HoodieFileStatus.newBuilder()
            .setPath(HoodiePath.newBuilder().setUri(SOURCE_BASE_PATH + "/" + partition + "/" + sourceFileName).build())
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
        return new BootstrapSourceFileMapping(SOURCE_BASE_PATH, partition, partition, sourceFileStatus, hudiFileId);
      }).collect(Collectors.toList()));
    }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }
}
