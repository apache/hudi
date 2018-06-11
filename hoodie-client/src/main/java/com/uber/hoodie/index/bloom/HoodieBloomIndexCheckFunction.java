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
import com.uber.hoodie.common.util.ParquetUtils;
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
 * Function performing actual checking of RDD parition containing (fileId, hoodieKeys) against the
 * actual files
 */
public class HoodieBloomIndexCheckFunction implements
    Function2<Integer, Iterator<Tuple2<String, Tuple2<String, HoodieKey>>>,
        Iterator<List<IndexLookupResult>>> {

  private static Logger logger = LogManager.getLogger(HoodieBloomIndexCheckFunction.class);

  private final String basePath;

  private final HoodieTableMetaClient metaClient;

  public HoodieBloomIndexCheckFunction(HoodieTableMetaClient metaClient, String basePath) {
    this.metaClient = metaClient;
    this.basePath = basePath;
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
        Set<String> fileRowKeys = ParquetUtils.filterParquetRowKeys(configuration, filePath,
            new HashSet<>(candidateRecordKeys));
        foundRecordKeys.addAll(fileRowKeys);
        logger.info("After checking with row keys, we have " + foundRecordKeys.size()
            + " results, for file " + filePath + " => " + foundRecordKeys);
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
  public Iterator<List<IndexLookupResult>> call(Integer partition,
      Iterator<Tuple2<String, Tuple2<String, HoodieKey>>> fileParitionRecordKeyTripletItr)
      throws Exception {
    return new LazyKeyCheckIterator(fileParitionRecordKeyTripletItr);
  }

  class LazyKeyCheckIterator extends
      LazyIterableIterator<Tuple2<String, Tuple2<String, HoodieKey>>, List<IndexLookupResult>> {

    private List<String> candidateRecordKeys;

    private BloomFilter bloomFilter;

    private String currentFile;

    private String currentParitionPath;

    LazyKeyCheckIterator(
        Iterator<Tuple2<String, Tuple2<String, HoodieKey>>> fileParitionRecordKeyTripletItr) {
      super(fileParitionRecordKeyTripletItr);
      currentFile = null;
      candidateRecordKeys = new ArrayList<>();
      bloomFilter = null;
      currentParitionPath = null;
    }

    @Override
    protected void start() {
    }

    private void initState(String fileName, String partitionPath) throws HoodieIndexException {
      try {
        Path filePath = new Path(basePath + "/" + partitionPath + "/" + fileName);
        bloomFilter = ParquetUtils
            .readBloomFilterFromParquetMetadata(metaClient.getHadoopConf(), filePath);
        candidateRecordKeys = new ArrayList<>();
        currentFile = fileName;
        currentParitionPath = partitionPath;
      } catch (Exception e) {
        throw new HoodieIndexException("Error checking candidate keys against file.", e);
      }
    }

    @Override
    protected List<IndexLookupResult> computeNext() {

      List<IndexLookupResult> ret = new ArrayList<>();
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

          // if continue on current file)
          if (fileName.equals(currentFile)) {
            // check record key against bloom filter of current file & add to possible keys if
            // needed
            if (bloomFilter.mightContain(recordKey)) {
              if (logger.isDebugEnabled()) {
                logger.debug("#1 Adding " + recordKey + " as candidate for file " + fileName);
              }
              candidateRecordKeys.add(recordKey);
            }
          } else {
            // do the actual checking of file & break out
            Path filePath = new Path(basePath + "/" + currentParitionPath + "/" + currentFile);
            logger.info(
                "#1 After bloom filter, the candidate row keys is reduced to " + candidateRecordKeys
                    .size() + " for " + filePath);
            if (logger.isDebugEnabled()) {
              logger
                  .debug("#The candidate row keys for " + filePath + " => " + candidateRecordKeys);
            }
            ret.add(new IndexLookupResult(currentFile,
                checkCandidatesAgainstFile(metaClient.getHadoopConf(), candidateRecordKeys, filePath)));

            initState(fileName, partitionPath);
            if (bloomFilter.mightContain(recordKey)) {
              if (logger.isDebugEnabled()) {
                logger.debug("#2 Adding " + recordKey + " as candidate for file " + fileName);
              }
              candidateRecordKeys.add(recordKey);
            }
            break;
          }
        }

        // handle case, where we ran out of input, close pending work, update return val
        if (!inputItr.hasNext()) {
          Path filePath = new Path(basePath + "/" + currentParitionPath + "/" + currentFile);
          logger.info(
              "#2 After bloom filter, the candidate row keys is reduced to " + candidateRecordKeys
                  .size() + " for " + filePath);
          if (logger.isDebugEnabled()) {
            logger.debug("#The candidate row keys for " + filePath + " => " + candidateRecordKeys);
          }
          ret.add(new IndexLookupResult(currentFile,
              checkCandidatesAgainstFile(metaClient.getHadoopConf(), candidateRecordKeys, filePath)));
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
