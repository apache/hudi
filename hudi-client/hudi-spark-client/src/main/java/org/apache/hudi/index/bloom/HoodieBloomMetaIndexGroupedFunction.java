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

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.bloom.HoodieDynamicBoundedBloomFilter;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.FileID;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.io.HoodieKeyMetaBloomIndexGroupedLookupHandle.MetaBloomIndexGroupedKeyLookupResult;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Function performing actual checking of RDD partition containing (fileId, hoodieKeys) against the actual files.
 */
public class HoodieBloomMetaIndexGroupedFunction
    implements Function2<Integer, Iterator<Tuple2<Tuple2<String, String>, HoodieKey>>,
    Iterator<List<MetaBloomIndexGroupedKeyLookupResult>>> {

  private static final Logger LOG = LogManager.getLogger(HoodieBloomMetaIndexGroupedFunction.class);
  private final HoodieTable hoodieTable;

  private final HoodieWriteConfig config;

  public HoodieBloomMetaIndexGroupedFunction(HoodieTable hoodieTable, HoodieWriteConfig config) {
    this.hoodieTable = hoodieTable;
    this.config = config;
  }

  @Override
  public Iterator<List<MetaBloomIndexGroupedKeyLookupResult>> call(Integer integer,
                                                                   Iterator<Tuple2<Tuple2<String, String>, HoodieKey>> tupleIterator) throws Exception {

    List<List<MetaBloomIndexGroupedKeyLookupResult>> resultList = new ArrayList<>();

    // <PartitionPath, FileName> => List<HoodieKey>
    Map<Pair<String, String>, List<HoodieKey>> fileToKeysMap = new HashMap<>();

    // TODO: use getBloomFilters instead o getBloomFilter

    while (tupleIterator.hasNext()) {
      Tuple2<Tuple2<String, String>, HoodieKey> entry = tupleIterator.next();
      fileToKeysMap.computeIfAbsent(Pair.of(entry._2.getPartitionPath(), entry._1._1), k -> new ArrayList<>()).add(entry._2);
    }

    List<FileID> fileIDs = fileToKeysMap.keySet().stream().map(partitionFileNamePair -> {
      return new FileID(partitionFileNamePair.getRight());
    }).collect(Collectors.toList());

    Map<String, ByteBuffer> fileIDToBloomFilterByteBufferMap =
        hoodieTable.getMetadataTable().getBloomFilters(fileIDs);

    fileToKeysMap.forEach((partitionPathFileNamePair, hoodieKeyList) -> {
      final String partitionPath = partitionPathFileNamePair.getLeft();
      final String fileName = partitionPathFileNamePair.getRight();

      final String fileId = FSUtils.getFileId(fileName);
      ValidationUtils.checkState(!fileId.isEmpty());

      final String fileIDHash = new FileID(fileName).asBase64EncodedString();
      if (!fileIDToBloomFilterByteBufferMap.containsKey(fileIDHash)) {
        throw new HoodieIndexException("Failed to get the bloom filter for " + partitionPathFileNamePair);
      }
      final ByteBuffer fileBloomFilterByteBuffer = fileIDToBloomFilterByteBufferMap.get(fileIDHash);

      HoodieDynamicBoundedBloomFilter fileBloomFilter =
          new HoodieDynamicBoundedBloomFilter(StandardCharsets.UTF_8.decode(fileBloomFilterByteBuffer).toString(),
              BloomFilterTypeCode.DYNAMIC_V0);

      List<String> candidateRecordKeys = new ArrayList<>();
      hoodieKeyList.forEach(hoodieKey -> {
        if (fileBloomFilter.mightContain(hoodieKey.getRecordKey())) {
          candidateRecordKeys.add(hoodieKey.getRecordKey());
        }
      });

      Option<HoodieBaseFile> dataFile = hoodieTable.getBaseFileOnlyView()
          .getLatestBaseFile(partitionPath, fileId);
      if (!dataFile.isPresent()) {
        throw new HoodieIndexException("Failed to find the base file for partition: " + partitionPath
            + ", fileId: " + fileId);
      }

      List<String> matchingKeys =
          checkCandidatesAgainstFile(candidateRecordKeys, new Path(dataFile.get().getPath()));

      LOG.debug(
          String.format("Total records (%d), bloom filter candidates (%d)/fp(%d), actual matches (%d)",
              hoodieKeyList.size(), candidateRecordKeys.size(),
              candidateRecordKeys.size() - matchingKeys.size(), matchingKeys.size()));

      ArrayList<MetaBloomIndexGroupedKeyLookupResult> subList = new ArrayList<>();
      subList.add(new MetaBloomIndexGroupedKeyLookupResult(fileId, partitionPath, dataFile.get().getCommitTime(),
          matchingKeys));
      resultList.add(subList);
    });

    return resultList.iterator();
  }

  public List<String> checkCandidatesAgainstFile(List<String> candidateRecordKeys,
                                                 Path latestDataFilePath) throws HoodieIndexException {
    List<String> foundRecordKeys = new ArrayList<>();
    try {
      // Load all rowKeys from the file, to double-confirm
      if (!candidateRecordKeys.isEmpty()) {
        HoodieTimer timer = new HoodieTimer().startTimer();

        final HoodieFileReader fileReader = HoodieFileReaderFactory.getFileReader(hoodieTable.getHadoopConf(),
            latestDataFilePath);
        Set<String> fileRowKeys = fileReader.filterRowKeys(new HashSet<>(candidateRecordKeys));
        foundRecordKeys.addAll(fileRowKeys);
        LOG.debug(String.format("Checked keys against file %s, in %d ms. #candidates (%d) #found (%d)",
            latestDataFilePath,
            timer.endTimer(), candidateRecordKeys.size(), foundRecordKeys.size()));
        LOG.debug("Keys matching for file " + latestDataFilePath + " => " + foundRecordKeys);
      }
    } catch (Exception e) {
      throw new HoodieIndexException("Error checking candidate keys against file.", e);
    }
    return foundRecordKeys;
  }
}
