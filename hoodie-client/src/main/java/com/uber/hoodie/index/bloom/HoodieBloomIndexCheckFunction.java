/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.index.bloom;

import com.uber.hoodie.common.BloomFilter;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.HoodieTimer;
import com.uber.hoodie.common.util.ParquetUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIndexException;
import com.uber.hoodie.func.LazyIterableIterator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 * Function performing actual checking of RDD partition containing (fileId, hoodieKeys) against the
 * actual files
 */
public class HoodieBloomIndexCheckFunction implements
    Function2<Integer, Iterator<Tuple2<String, Tuple2<String, HoodieKey>>>,
        Iterator<List<KeyLookupResult>>> {

  private static Logger logger = LogManager.getLogger(HoodieBloomIndexCheckFunction.class);

  private final HoodieWriteConfig config;

  private final HoodieTableMetaClient metaClient;

  public HoodieBloomIndexCheckFunction(HoodieTableMetaClient metaClient, HoodieWriteConfig config) {
    this.metaClient = metaClient;
    this.config = config;
  }

  /**
   * Given a list of row keys and one file, return only row keys existing in that file.
   */
  public static List<String> checkCandidatesAgainstFile(Configuration configuration,
      List<String> candidateRecordKeys, Path filePath) throws HoodieIndexException {
    List<String> foundRecordKeys = new ArrayList<>();
    try {
      // Load all rowKeys from the file, to double-confirm
      if (!candidateRecordKeys.isEmpty()) {
        HoodieTimer timer = new HoodieTimer().startTimer();
        Set<String> fileRowKeys = ParquetUtils.filterParquetRowKeys(configuration, filePath,
            new HashSet<>(candidateRecordKeys));
        foundRecordKeys.addAll(fileRowKeys);
        logger.info(String.format("Checked keys against file %s, in %d ms. #candidates (%d) #found (%d)", filePath,
            timer.endTimer(), candidateRecordKeys.size(), foundRecordKeys.size()));
        if (logger.isDebugEnabled()) {
          logger.debug("Keys matching for file " + filePath + " => " + foundRecordKeys);
        }
      }
    } catch (Exception e) {
      throw new HoodieIndexException("Error checking candidate keys against file.", e);
    }
    return foundRecordKeys;
  }

  @Override
  public Iterator<List<KeyLookupResult>> call(Integer partition,
      Iterator<Tuple2<String, Tuple2<String, HoodieKey>>> fileParitionRecordKeyTripletItr)
      throws Exception {
    return new LazyKeyCheckIterator(fileParitionRecordKeyTripletItr);
  }

  class LazyKeyCheckIterator extends
      LazyIterableIterator<Tuple2<String, Tuple2<String, HoodieKey>>, List<KeyLookupResult>> {

    private List<String> candidateRecordKeys;

    private BloomFilter bloomFilter;

    private String currentFile;

    private String currentPartitionPath;

    private long totalKeysChecked;

    LazyKeyCheckIterator(
        Iterator<Tuple2<String, Tuple2<String, HoodieKey>>> filePartitionRecordKeyTripletItr) {
      super(filePartitionRecordKeyTripletItr);
      currentFile = null;
      candidateRecordKeys = new ArrayList<>();
      bloomFilter = null;
      currentPartitionPath = null;
      totalKeysChecked = 0;
    }

    @Override
    protected void start() {
    }

    private void initState(String fileName, String partitionPath) throws HoodieIndexException {
      try {
        Path filePath = new Path(config.getBasePath() + "/" + partitionPath + "/" + fileName);
        HoodieTimer timer = new HoodieTimer().startTimer();
        bloomFilter = ParquetUtils.readBloomFilterFromParquetMetadata(metaClient.getHadoopConf(), filePath, config
            .getEnableDynamicBloomIndex());
        logger.info(String.format("Read bloom filter from %s/%s in %d ms", partitionPath, fileName, timer.endTimer()));
        candidateRecordKeys = new ArrayList<>();
        currentFile = fileName;
        currentPartitionPath = partitionPath;
        totalKeysChecked = 0;
      } catch (Exception e) {
        throw new HoodieIndexException("Error checking candidate keys against file.", e);
      }
    }

    // check record key against bloom filter of current file & add to possible keys if needed
    private void checkAndAddCandidates(String recordKey) {
      if (bloomFilter.mightContain(recordKey)) {
        if (logger.isDebugEnabled()) {
          logger.debug("Record key " + recordKey + " matches bloom filter in file " + currentPartitionPath
              + "/" + currentFile);
        }
        candidateRecordKeys.add(recordKey);
      }
      totalKeysChecked++;
    }

    private List<String> checkAgainstCurrentFile() {
      Path filePath = new Path(config.getBasePath() + "/" + currentPartitionPath + "/" + currentFile);
      if (logger.isDebugEnabled()) {
        logger.debug("#The candidate row keys for " + filePath + " => " + candidateRecordKeys);
      }
      List<String> matchingKeys = checkCandidatesAgainstFile(metaClient.getHadoopConf(), candidateRecordKeys, filePath);
      logger.info(String.format("Total records (%d), bloom filter candidates (%d)/fp(%d), actual matches (%d)",
          totalKeysChecked, candidateRecordKeys.size(), candidateRecordKeys.size() - matchingKeys.size(),
          matchingKeys.size()));
      return matchingKeys;
    }

    @Override
    protected List<KeyLookupResult> computeNext() {

      List<KeyLookupResult> ret = new ArrayList<>();
      try {
        // process one file in each go.
        while (inputItr.hasNext()) {
          Tuple2<String, Tuple2<String, HoodieKey>> currentTuple = inputItr.next();
          String fileName = currentTuple._2._1;
          String partitionPath = currentTuple._2._2.getPartitionPath();
          String recordKey = currentTuple._2._2.getRecordKey();

          // lazily init state
          if (currentFile == null) {
            initState(fileName, partitionPath);
          }

          // if continue on current file
          if (fileName.equals(currentFile)) {
            checkAndAddCandidates(recordKey);
          } else {
            // do the actual checking of file & break out
            ret.add(new KeyLookupResult(currentFile, checkAgainstCurrentFile()));
            initState(fileName, partitionPath);
            checkAndAddCandidates(recordKey);
            break;
          }
        }

        // handle case, where we ran out of input, close pending work, update return val
        if (!inputItr.hasNext()) {
          ret.add(new KeyLookupResult(currentFile, checkAgainstCurrentFile()));
        }
      } catch (Throwable e) {
        if (e instanceof HoodieException) {
          throw e;
        }
        throw new HoodieIndexException("Error checking bloom filter index. ", e);
      }

      return ret;
    }

    @Override
    protected void end() {
    }
  }
}
