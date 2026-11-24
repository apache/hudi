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

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Partitioned-RLI arbitration must look keys up one RLI file group at a time: the metadata reader
 * resolves a partitioned lookup batch against the file group of its first sorted key only.
 */
class TestVectorIndexRliArbitratorPartitionedRli {

  private static final String P1 = "2024/01";
  private static final String P2 = "2024/02";
  private static final int FILE_GROUPS_PER_PARTITION = 4;
  private static final int KEYS_PER_PARTITION = 40;

  private final Map<String, Map<String, HoodieRecordGlobalLocation>> rli = new HashMap<>();
  private final List<Set<Integer>> fileGroupsPerLookup = new ArrayList<>();
  private HoodieTableMetadata metadataTable;

  @BeforeEach
  void setUp() {
    Map<String, List<FileSlice>> fileGroups = new HashMap<>();
    for (String partition : new String[] {P1, P2}) {
      fileGroups.put(partition, IntStream.range(0, FILE_GROUPS_PER_PARTITION)
          .mapToObj(i -> new FileSlice(partition, "t100", "record-index-" + i))
          .collect(Collectors.toList()));
      Map<String, HoodieRecordGlobalLocation> locations = new HashMap<>();
      keys(partition).forEach(key -> locations.put(key, location(partition)));
      rli.put(partition, locations);
    }
    metadataTable = mock(HoodieTableMetadata.class);
    when(metadataTable.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX))
        .thenReturn(fileGroups);
    when(metadataTable.readRecordIndexLocationsWithKeys(any(), any())).thenAnswer(invocation -> {
      HoodieData<String> keys = invocation.getArgument(0);
      Option<String> partition = invocation.getArgument(1);
      return HoodieListPairData.eager(readLikeHoodieBackedTableMetadata(keys.collectAsList(), partition.get()));
    });
  }

  @Test
  void keysSpanningRliFileGroupsAllResolveForMaterializedFinalists() {
    List<ScoredVectorPostingMatch> finalists = finalists();
    assertTrue(spannedFileGroups(P1) > 1, "test keys must span several RLI file groups");

    VectorIndexMdtSearchUtils.ArbitrationResult result =
        VectorIndexRliArbitrator.arbitrateMaterializedFinalists(metadataTable, finalists, true);

    assertEquals(finalists.size(), result.serve().size());
    assertEquals(0L, result.deletedCount());
    assertEquals(0L, result.staleCount());
    assertEveryLookupHitOneFileGroup();
  }

  @Test
  void keysSpanningRliFileGroupsAllResolveForDistributedFinalists() {
    List<ScoredVectorPostingMatch> finalists = finalists();

    List<ScoredVectorPostingMatch> arbitrated = VectorIndexRliArbitrator
        .arbitrateFinalists(metadataTable, HoodieListData.eager(finalists), true)
        .collectAsList();

    assertEquals(finalists.size(), arbitrated.size());
    assertTrue(arbitrated.stream().allMatch(c -> c.getArbiterDecision() == VectorIndexArbiter.Decision.SERVE),
        "every live key must SERVE, none may be misread as DELETED");
    assertEveryLookupHitOneFileGroup();
  }

  @Test
  void partitionWithoutRliFileGroupsResolvesToDeleted() {
    ScoredVectorPostingMatch orphan = candidate("orphan-key", "2024/99");

    VectorIndexMdtSearchUtils.ArbitrationResult result =
        VectorIndexRliArbitrator.arbitrateMaterializedFinalists(metadataTable, Collections.singletonList(orphan), true);

    assertEquals(1L, result.deletedCount());
    assertTrue(fileGroupsPerLookup.isEmpty(), "no RLI read for a partition without RLI file groups");
  }

  /**
   * Mirrors {@code HoodieBackedTableMetadata#lookupIndexRecords} for a partitioned RLI: the batch is
   * sorted, the file group is chosen from the first key, and only that file group is read.
   */
  private List<Pair<String, HoodieRecordGlobalLocation>> readLikeHoodieBackedTableMetadata(
      List<String> keys, String partition) {
    TreeSet<String> sorted = new TreeSet<>(StringUtils.UTF8_LEXICOGRAPHIC_COMPARATOR);
    sorted.addAll(keys);
    int fileGroup = fileGroup(sorted.first());
    fileGroupsPerLookup.add(keys.stream().map(this::fileGroup).collect(Collectors.toSet()));
    return sorted.stream()
        .filter(key -> fileGroup(key) == fileGroup && rli.get(partition).containsKey(key))
        .map(key -> Pair.of(key, rli.get(partition).get(key)))
        .collect(Collectors.toList());
  }

  private void assertEveryLookupHitOneFileGroup() {
    assertTrue(!fileGroupsPerLookup.isEmpty());
    fileGroupsPerLookup.forEach(groups -> assertEquals(1, groups.size(),
        "each partitioned RLI lookup batch must map to exactly one file group, got " + groups));
  }

  private int fileGroup(String key) {
    return HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(key, FILE_GROUPS_PER_PARTITION);
  }

  private long spannedFileGroups(String partition) {
    return keys(partition).stream().map(this::fileGroup).distinct().count();
  }

  private static List<String> keys(String partition) {
    return IntStream.range(0, KEYS_PER_PARTITION)
        .mapToObj(i -> partition.replace('/', '-') + "-key-" + i)
        .collect(Collectors.toList());
  }

  private static List<ScoredVectorPostingMatch> finalists() {
    List<ScoredVectorPostingMatch> finalists = new ArrayList<>();
    for (String partition : new String[] {P1, P2}) {
      keys(partition).forEach(key -> finalists.add(candidate(key, partition)));
    }
    return finalists;
  }

  private static ScoredVectorPostingMatch candidate(String recordKey, String partition) {
    VectorPostingMatch posting = new VectorPostingMatch(
        recordKey, 1, 0, "fg-" + partition, partition, "t100", 42L, new byte[] {1}, 1.0f);
    return new ScoredVectorPostingMatch(posting, 0.5f, posting.toLocation().get());
  }

  private static HoodieRecordGlobalLocation location(String partition) {
    return new HoodieRecordGlobalLocation(partition, "t100", "fg-" + partition, 42L);
  }
}
