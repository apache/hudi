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

import org.apache.hudi.client.utils.LazyIterableIterator;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.io.HoodieKeyMetaBloomIndexLookupHandle;
import org.apache.hudi.io.HoodieKeyMetaBloomIndexLookupHandle.MetaBloomIndexKeyLookupResult;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Function performing actual checking of RDD partition containing (fileId, hoodieKeys) against the actual files.
 */
public class HoodieBloomMetaIndexCheckFunction
    implements Function2<Integer, Iterator<Tuple2<Tuple2<String, String>, HoodieKey>>,
    Iterator<List<MetaBloomIndexKeyLookupResult>>> {

  private static final Logger LOG = LogManager.getLogger(HoodieBloomMetaIndexCheckFunction.class);
  private final HoodieTable hoodieTable;

  private final HoodieWriteConfig config;

  public HoodieBloomMetaIndexCheckFunction(HoodieTable hoodieTable, HoodieWriteConfig config) {
    this.hoodieTable = hoodieTable;
    this.config = config;
  }

  @Override
  public Iterator<List<MetaBloomIndexKeyLookupResult>> call(Integer integer,
                                                            Iterator<Tuple2<Tuple2<String, String>, HoodieKey>> tupleIterator) throws Exception {
    return new LazyKeyCheckIterator(tupleIterator);
  }

  class LazyKeyCheckIterator extends LazyIterableIterator<Tuple2<Tuple2<String, String>, HoodieKey>,
      List<MetaBloomIndexKeyLookupResult>> {

    private HoodieKeyMetaBloomIndexLookupHandle keyLookupHandle;

    LazyKeyCheckIterator(Iterator<Tuple2<Tuple2<String, String>, HoodieKey>> filePartitionRecordKeyTripletItr) {
      super(filePartitionRecordKeyTripletItr);
    }

    @Override
    protected void start() {
    }

    @Override
    protected List<MetaBloomIndexKeyLookupResult> computeNext() {

      List<MetaBloomIndexKeyLookupResult> ret = new ArrayList<>();
      try {
        // process one file in each go.
        while (inputItr.hasNext()) {
          Tuple2<Tuple2<String, String>, HoodieKey> currentTuple = inputItr.next();
          final String fileName = currentTuple._1._1;
          final String fileId = FSUtils.getFileId(fileName);
          ValidationUtils.checkState(!fileId.isEmpty());
          String partitionPath = currentTuple._2.getPartitionPath();
          String recordKey = currentTuple._2.getRecordKey();
          Pair<String, String> partitionPathFileIdPair = Pair.of(partitionPath, fileId);

          // lazily init state
          if (keyLookupHandle == null) {
            keyLookupHandle = new HoodieKeyMetaBloomIndexLookupHandle(config, hoodieTable, partitionPathFileIdPair,
                fileName);
          }

          // if continue on current file
          if (keyLookupHandle.getPartitionPathFilePair().equals(partitionPathFileIdPair)) {
            keyLookupHandle.addKey(recordKey);
          } else {
            // do the actual checking of file & break out
            ret.add(keyLookupHandle.getLookupResult());
            keyLookupHandle = new HoodieKeyMetaBloomIndexLookupHandle(config, hoodieTable, partitionPathFileIdPair,
                fileName);
            keyLookupHandle.addKey(recordKey);
            break;
          }
        }

        // handle case, where we ran out of input, close pending work, update return val
        if (!inputItr.hasNext()) {
          ret.add(keyLookupHandle.getLookupResult());
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
