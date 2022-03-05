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

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.config.HoodieIndexConfig.BUCKET_MERGE_THRESHOLD;
import static org.apache.hudi.config.HoodieIndexConfig.BUCKET_SPLIT_THRESHOLD;

public class TestSparkConsistentBucketClusteringPlanStrategy extends HoodieClientTestHarness {

  private final Random random = new Random();

  private void setup() throws IOException {
    initPath();
    initSparkContexts();
    initFileSystem();
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @Test
  public void testBuildSplitClusteringGroup() throws IOException {
    setup();
    int maxFileSize = 5120;
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING)
            .withBucketMaxNum(6)
            .withBucketNum("4").build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .parquetMaxFileSize(maxFileSize).build())
        .build();

    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    SparkConsistentBucketClusteringPlanStrategy planStrategy = new SparkConsistentBucketClusteringPlanStrategy(hoodieTable, context, config);

    HoodieConsistentHashingMetadata metadata = new HoodieConsistentHashingMetadata("partition", config.getBucketIndexNumBuckets());
    ConsistentBucketIdentifier identifier = new ConsistentBucketIdentifier(metadata);

    int[] fsSize = {maxFileSize * 5, (int) (maxFileSize * BUCKET_SPLIT_THRESHOLD.defaultValue() + 1), maxFileSize, maxFileSize * 5};
    List<FileSlice> fileSlices = IntStream.range(0, metadata.getNodes().size()).mapToObj(
        i -> createFileSliceWithSize(metadata.getNodes().get(i).getFileIdPfx(), 1024, fsSize[i] - 1024)
    ).collect(Collectors.toList());

    /**
     * 1. Test split candidate selection based on file size
     * 2. Test the effectiveness of split slot
     */
    Triple res = planStrategy.buildSplitClusteringGroups(identifier, fileSlices, 2);
    Assertions.assertEquals(2, res.getMiddle());
    List<HoodieClusteringGroup> groups = (List<HoodieClusteringGroup>) res.getLeft();
    Assertions.assertEquals(2, groups.size());
    Assertions.assertEquals(fileSlices.get(0).getFileId(), groups.get(0).getSlices().get(0).getFileId());
    Assertions.assertEquals(fileSlices.get(1).getFileId(), groups.get(1).getSlices().get(0).getFileId());
    List<FileSlice> fsUntouched = (List<FileSlice>) res.getRight();
    Assertions.assertEquals(2, fsUntouched.size());
    Assertions.assertEquals(fileSlices.get(2), fsUntouched.get(0));
    Assertions.assertEquals(fileSlices.get(3), fsUntouched.get(1));
  }

  @Test
  public void testBuildMergeClusteringGroup() throws Exception {
    setup();
    int maxFileSize = 5120;
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING)
            .withBucketMinNum(4)
            .withBucketNum("4").build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .parquetMaxFileSize(maxFileSize).build())
        .build();

    HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);
    SparkConsistentBucketClusteringPlanStrategy planStrategy = new SparkConsistentBucketClusteringPlanStrategy(hoodieTable, context, config);

    HoodieConsistentHashingMetadata metadata = new HoodieConsistentHashingMetadata("partition", 8);
    ConsistentBucketIdentifier identifier = new ConsistentBucketIdentifier(metadata);

    int mergeSize = (int) (maxFileSize * BUCKET_MERGE_THRESHOLD.defaultValue());
    int[] fsSize = {0, maxFileSize, mergeSize / 2, mergeSize / 2, mergeSize / 2, maxFileSize, mergeSize / 4, mergeSize / 4};
    List<FileSlice> fileSlices = IntStream.range(0, metadata.getNodes().size()).mapToObj(
        i -> createFileSliceWithSize(metadata.getNodes().get(i).getFileIdPfx(), fsSize[i] / 2, fsSize[i] / 2)
    ).collect(Collectors.toList());

    /**
     * 1. Test merge candidate selection based on file size
     * 2. Test empty file size
     * 3. Test merge slot
     */
    Triple res = planStrategy.buildMergeClusteringGroup(identifier, fileSlices, 4);
    Assertions.assertEquals(3, res.getMiddle());
    List<HoodieClusteringGroup> groups = (List<HoodieClusteringGroup>) res.getLeft();
    Assertions.assertEquals(2, groups.size());

    // Check group 0
    Assertions.assertEquals(fileSlices.get(0).getFileId(), groups.get(0).getSlices().get(2).getFileId());
    Assertions.assertEquals(fileSlices.get(7).getFileId(), groups.get(0).getSlices().get(1).getFileId());
    Assertions.assertEquals(fileSlices.get(6).getFileId(), groups.get(0).getSlices().get(0).getFileId());
    Assertions.assertEquals(3, groups.get(0).getSlices().size());
    List<ConsistentHashingNode> nodes = ConsistentHashingNode.fromJsonString(groups.get(0).getExtraMetadata().get(SparkConsistentBucketClusteringPlanStrategy.METADATA_CHILD_NODE_KEY));
    Assertions.assertEquals(3, nodes.size());
    Assertions.assertEquals(ConsistentHashingNode.NodeTag.DELETE, nodes.get(0).getTag());
    Assertions.assertEquals(ConsistentHashingNode.NodeTag.DELETE, nodes.get(1).getTag());
    Assertions.assertEquals(ConsistentHashingNode.NodeTag.REPLACE, nodes.get(2).getTag());
    Assertions.assertEquals(metadata.getNodes().get(0).getValue(), nodes.get(2).getValue());

    // Check group 1
    Assertions.assertEquals(fileSlices.get(2).getFileId(), groups.get(1).getSlices().get(0).getFileId());
    Assertions.assertEquals(fileSlices.get(3).getFileId(), groups.get(1).getSlices().get(1).getFileId());
    Assertions.assertEquals(2, groups.get(1).getSlices().size());
    nodes = ConsistentHashingNode.fromJsonString(groups.get(1).getExtraMetadata().get(SparkConsistentBucketClusteringPlanStrategy.METADATA_CHILD_NODE_KEY));
    Assertions.assertEquals(2, nodes.size());
    Assertions.assertEquals(ConsistentHashingNode.NodeTag.DELETE, nodes.get(0).getTag());
    Assertions.assertEquals(ConsistentHashingNode.NodeTag.REPLACE, nodes.get(1).getTag());
    Assertions.assertEquals(metadata.getNodes().get(3).getValue(), nodes.get(1).getValue());
  }

  private FileSlice createFileSliceWithSize(String fileIdPfx, long baseFileSize, long totalLogFileSize) {
    String fileId = FSUtils.createNewFileId(fileIdPfx, 0);
    FileSlice fs = new FileSlice("partition", "001", fileId);
    if (baseFileSize > 0) {
      HoodieBaseFile f = new HoodieBaseFile(fileId);
      f.setFileLen(baseFileSize);
      fs.setBaseFile(f);
    }

    int numLogFiles = random.nextInt(10) + 1;
    if (totalLogFileSize < numLogFiles) {
      numLogFiles = (int) totalLogFileSize;
    }
    long logFileSize = (totalLogFileSize + numLogFiles - 1) / Math.max(numLogFiles, 1);
    for (int i = 0; i < numLogFiles; ++i) {
      HoodieLogFile f = new HoodieLogFile(String.format(".%s_%s.log.%d", fileId, "12345678", i));
      f.setFileLen(logFileSize);
      fs.addLogFile(f);
    }

    return fs;
  }

}
