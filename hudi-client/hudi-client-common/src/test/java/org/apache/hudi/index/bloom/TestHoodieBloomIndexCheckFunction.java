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

package org.apache.hudi.index.bloom;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieKeyLookupResult;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestHoodieBloomIndexCheckFunction {

  private static final String BASE_PATH = "/table";

  @Mock
  private HoodieTable<?, ?, ?, ?> hoodieTable;
  @Mock
  private HoodieWriteConfig config;
  @Mock
  private HoodieTableMetaClient metaClient;
  @Mock
  private SyncableFileSystemView fileSystemView;
  @Mock
  private HoodieWrapperFileSystem fileSystem;
  @Mock
  private HoodieRecordMerger recordMerger;
  private HoodieFileReaderFactory mockFactory;

  @BeforeEach
  void setUp() {
    Configuration hadoopConf = new Configuration();
    lenient().when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    lenient().when(metaClient.getFs()).thenReturn(fileSystem);
    lenient().when(hoodieTable.getHadoopConf()).thenReturn(hadoopConf);
    lenient().when(hoodieTable.getBaseFileOnlyView()).thenReturn(fileSystemView);
    lenient().when(config.getBloomIndexUseMetadata()).thenReturn(false);
    lenient().when(config.getRecordMerger()).thenReturn(recordMerger);
    lenient().when(recordMerger.getRecordType()).thenReturn(HoodieRecordType.AVRO);
    mockFactory = mock(HoodieFileReaderFactory.class);
  }

  private static String filePath(String partitionPath, String fileId, String commitTime) {
    return BASE_PATH + "/" + partitionPath + "/" + fileId + "_" + commitTime + ".parquet";
  }

  private static List<Pair<HoodieFileGroupId, String>> inputForFileGroup(
      String partitionPath, String fileId, String... recordKeys) {
    List<Pair<HoodieFileGroupId, String>> input = new ArrayList<>();
    HoodieFileGroupId fileGroupId = new HoodieFileGroupId(partitionPath, fileId);
    for (String key : recordKeys) {
      input.add(Pair.of(fileGroupId, key));
    }
    return input;
  }

  private void mockBaseFile(String partitionPath, String fileId, String commitTime) {
    String path = filePath(partitionPath, fileId, commitTime);
    HoodieBaseFile baseFile = mock(HoodieBaseFile.class);
    when(baseFile.getPath()).thenReturn(path);
    when(baseFile.getCommitTime()).thenReturn(commitTime);
    when(fileSystemView.getLatestBaseFile(partitionPath, fileId)).thenReturn(Option.of(baseFile));
  }

  private static BloomFilter mockBloomFilter(String... keysToMatch) {
    BloomFilter bloomFilter = mock(BloomFilter.class);
    Set<String> matchSet = new HashSet<>(Arrays.asList(keysToMatch));
    lenient().when(bloomFilter.mightContain(anyString()))
        .thenAnswer(invocation -> matchSet.contains(invocation.getArgument(0)));
    return bloomFilter;
  }

  private static HoodieFileReader<?> mockFileReader(BloomFilter bloomFilter, Set<String> matchingKeys) {
    HoodieFileReader<?> fileReader = mock(HoodieFileReader.class);
    when(fileReader.readBloomFilter()).thenReturn(bloomFilter);
    when(fileReader.filterRowKeys(any())).thenReturn(matchingKeys);
    return fileReader;
  }

  private void registerFileReader(String filePath, HoodieFileReader<?> fileReader) throws IOException {
    lenient().when(mockFactory.getFileReader(any(Configuration.class), eq(new Path(filePath))))
        .thenReturn(fileReader);
  }

  private void registerDefaultFileReader(HoodieFileReader<?> fileReader) throws IOException {
    lenient().when(mockFactory.getFileReader(any(Configuration.class), any(Path.class)))
        .thenReturn(fileReader);
  }

  private void withStaticMocks(StaticMockTest test) throws Exception {
    try (MockedStatic<HoodieFileReaderFactory> factoryMock = mockStatic(HoodieFileReaderFactory.class);
         MockedStatic<FSUtils> fsUtilsMock = mockStatic(FSUtils.class)) {
      factoryMock.when(() -> HoodieFileReaderFactory.getReaderFactory(HoodieRecordType.AVRO))
          .thenReturn(mockFactory);
      fsUtilsMock.when(() -> FSUtils.isBaseFile(any(Path.class))).thenReturn(true);
      fsUtilsMock.when(() -> FSUtils.getFileExtension(anyString())).thenReturn(".parquet");
      test.run();
    }
  }

  private HoodieBloomIndexCheckFunction<Pair<HoodieFileGroupId, String>> createCheckFunction() {
    return new HoodieBloomIndexCheckFunction<>(hoodieTable, config, Pair::getLeft, Pair::getRight);
  }

  private static List<HoodieKeyLookupResult> collectResults(Iterator<HoodieKeyLookupResult> iterator) {
    List<HoodieKeyLookupResult> results = new ArrayList<>();
    while (iterator.hasNext()) {
      results.add(iterator.next());
    }
    return results;
  }

  @FunctionalInterface
  interface StaticMockTest {
    void run() throws Exception;
  }

  @Test
  void testEmptyInputIterator() {
    HoodieBloomIndexCheckFunction<Pair<HoodieFileGroupId, String>> checkFunction = createCheckFunction();
    Iterator<HoodieKeyLookupResult> result = checkFunction.apply(Collections.emptyIterator());
    assertFalse(result.hasNext());
  }

  @Test
  void testSingleFileGroupWithSingleKey() throws Exception {
    String partitionPath = "2026-01-31";
    String fileId = "file-001";
    String commitTime = "001";
    String recordKey = "key-001";

    mockBaseFile(partitionPath, fileId, commitTime);
    BloomFilter bloomFilter = mockBloomFilter(recordKey);
    HoodieFileReader<?> fileReader = mockFileReader(bloomFilter, new HashSet<>(Collections.singletonList(recordKey)));
    registerDefaultFileReader(fileReader);

    withStaticMocks(() -> {
      List<Pair<HoodieFileGroupId, String>> input = inputForFileGroup(partitionPath, fileId, recordKey);
      Iterator<HoodieKeyLookupResult> resultIterator = createCheckFunction().apply(input.iterator());

      assertTrue(resultIterator.hasNext());
      HoodieKeyLookupResult result = resultIterator.next();
      assertEquals(fileId, result.getFileId());
      assertEquals(partitionPath, result.getPartitionPath());
      assertEquals(commitTime, result.getBaseInstantTime());
      assertTrue(result.getMatchingRecordKeys().contains(recordKey));
      assertFalse(resultIterator.hasNext());
    });
  }

  @Test
  void testSingleFileGroupWithMultipleKeys() throws Exception {
    String partitionPath = "2026-01-31";
    String fileId = "file-001";
    String commitTime = "001";

    mockBaseFile(partitionPath, fileId, commitTime);
    // Bloom filter matches key-001 and key-003, but not key-002
    BloomFilter bloomFilter = mockBloomFilter("key-001", "key-003", "key-004");
    HoodieFileReader<?> fileReader = mockFileReader(bloomFilter, new HashSet<>(Arrays.asList("key-001", "key-003")));
    registerDefaultFileReader(fileReader);

    withStaticMocks(() -> {
      List<Pair<HoodieFileGroupId, String>> input = inputForFileGroup(partitionPath, fileId,
          "key-001", "key-002", "key-003");
      Iterator<HoodieKeyLookupResult> resultIterator = createCheckFunction().apply(input.iterator());

      assertTrue(resultIterator.hasNext());
      HoodieKeyLookupResult result = resultIterator.next();
      assertEquals(fileId, result.getFileId());
      assertEquals(2, result.getMatchingRecordKeys().size());
      assertFalse(resultIterator.hasNext());
    });
  }

  @Test
  void testMultipleFileGroups() throws Exception {
    String partitionPath1 = "2026-01-30";
    String fileId1 = "file-001";
    String commitTime1 = "001";

    String partitionPath2 = "2026-01-31";
    String fileId2 = "file-002";
    String commitTime2 = "002";

    mockBaseFile(partitionPath1, fileId1, commitTime1);
    mockBaseFile(partitionPath2, fileId2, commitTime2);

    BloomFilter bloomFilter1 = mockBloomFilter("key-001");
    BloomFilter bloomFilter2 = mockBloomFilter("key-002");

    HoodieFileReader<?> fileReader1 = mockFileReader(bloomFilter1, new HashSet<>(Collections.singletonList("key-001")));
    HoodieFileReader<?> fileReader2 = mockFileReader(bloomFilter2, new HashSet<>(Collections.singletonList("key-002")));

    registerFileReader(filePath(partitionPath1, fileId1, commitTime1), fileReader1);
    registerFileReader(filePath(partitionPath2, fileId2, commitTime2), fileReader2);

    withStaticMocks(() -> {
      List<Pair<HoodieFileGroupId, String>> input = new ArrayList<>();
      input.addAll(inputForFileGroup(partitionPath1, fileId1, "key-001"));
      input.addAll(inputForFileGroup(partitionPath2, fileId2, "key-002"));

      List<HoodieKeyLookupResult> results = collectResults(createCheckFunction().apply(input.iterator()));

      assertEquals(2, results.size());
      assertEquals(fileId1, results.get(0).getFileId());
      assertEquals(partitionPath1, results.get(0).getPartitionPath());
      assertEquals(fileId2, results.get(1).getFileId());
      assertEquals(partitionPath2, results.get(1).getPartitionPath());
    });
  }

  @Test
  void testIteratorHasNextIsIdempotent() throws Exception {
    String partitionPath = "2026-01-31";
    String fileId = "file-001";
    String commitTime = "001";
    String recordKey = "key-001";

    mockBaseFile(partitionPath, fileId, commitTime);
    BloomFilter bloomFilter = mockBloomFilter(recordKey);
    HoodieFileReader<?> fileReader = mockFileReader(bloomFilter, new HashSet<>(Collections.singletonList(recordKey)));
    registerDefaultFileReader(fileReader);

    withStaticMocks(() -> {
      List<Pair<HoodieFileGroupId, String>> input = inputForFileGroup(partitionPath, fileId, recordKey);
      Iterator<HoodieKeyLookupResult> resultIterator = createCheckFunction().apply(input.iterator());

      // Multiple hasNext calls should not change state
      assertTrue(resultIterator.hasNext());
      assertTrue(resultIterator.hasNext());
      assertTrue(resultIterator.hasNext());

      HoodieKeyLookupResult result = resultIterator.next();
      assertEquals(fileId, result.getFileId());

      assertFalse(resultIterator.hasNext());
      assertFalse(resultIterator.hasNext());
    });
  }

  @Test
  void testBloomFilterFalsePositive() throws Exception {
    String partitionPath = "2026-01-31";
    String fileId = "file-001";
    String commitTime = "001";

    mockBaseFile(partitionPath, fileId, commitTime);
    // Bloom filter says both keys might be present (false positive for key-002)
    BloomFilter bloomFilter = mockBloomFilter("key-001", "key-002");
    // But actual file only contains key-001
    HoodieFileReader<?> fileReader = mockFileReader(bloomFilter, new HashSet<>(Collections.singletonList("key-001")));
    registerDefaultFileReader(fileReader);

    withStaticMocks(() -> {
      List<Pair<HoodieFileGroupId, String>> input = inputForFileGroup(partitionPath, fileId, "key-001", "key-002");
      Iterator<HoodieKeyLookupResult> resultIterator = createCheckFunction().apply(input.iterator());

      assertTrue(resultIterator.hasNext());
      HoodieKeyLookupResult result = resultIterator.next();
      assertEquals(1, result.getMatchingRecordKeys().size());
      assertTrue(result.getMatchingRecordKeys().contains("key-001"));
      assertFalse(resultIterator.hasNext());
    });
  }

  @Test
  void testSameFileIdDifferentPartitionsTreatedAsDifferentGroups() throws Exception {
    String fileId = "file-001";
    String partitionPath1 = "2026-01-30";
    String partitionPath2 = "2026-01-31";
    String commitTime = "001";

    mockBaseFile(partitionPath1, fileId, commitTime);
    mockBaseFile(partitionPath2, fileId, commitTime);

    BloomFilter bloomFilter1 = mockBloomFilter("key-001");
    BloomFilter bloomFilter2 = mockBloomFilter("key-002");

    HoodieFileReader<?> fileReader1 = mockFileReader(bloomFilter1, new HashSet<>(Collections.singletonList("key-001")));
    HoodieFileReader<?> fileReader2 = mockFileReader(bloomFilter2, new HashSet<>(Collections.singletonList("key-002")));

    registerFileReader(filePath(partitionPath1, fileId, commitTime), fileReader1);
    registerFileReader(filePath(partitionPath2, fileId, commitTime), fileReader2);

    withStaticMocks(() -> {
      List<Pair<HoodieFileGroupId, String>> input = new ArrayList<>();
      input.addAll(inputForFileGroup(partitionPath1, fileId, "key-001"));
      input.addAll(inputForFileGroup(partitionPath2, fileId, "key-002"));

      List<HoodieKeyLookupResult> results = collectResults(createCheckFunction().apply(input.iterator()));

      assertEquals(2, results.size());
      assertEquals(partitionPath1, results.get(0).getPartitionPath());
      assertEquals(partitionPath2, results.get(1).getPartitionPath());
    });
  }
}
