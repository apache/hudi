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

package com.uber.hoodie.index.bloom;

import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIndexException;
import com.uber.hoodie.func.LazyIterableIterator;
import com.uber.hoodie.io.HoodieKeyLookupHandle;
import com.uber.hoodie.io.HoodieKeyLookupHandle.KeyLookupResult;
import com.uber.hoodie.table.HoodieTable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 * Function performing actual checking of RDD partition containing (fileId, hoodieKeys) against the
 * actual files
 */
public class HoodieBloomIndexCheckFunction implements
    Function2<Integer, Iterator<Tuple2<String, HoodieKey>>, Iterator<List<KeyLookupResult>>> {

  private final HoodieTable hoodieTable;

  private final HoodieWriteConfig config;

  public HoodieBloomIndexCheckFunction(HoodieTable hoodieTable, HoodieWriteConfig config) {
    this.hoodieTable = hoodieTable;
    this.config = config;
  }

  @Override
  public Iterator<List<KeyLookupResult>> call(Integer partition,
      Iterator<Tuple2<String, HoodieKey>> fileParitionRecordKeyTripletItr) {
    return new LazyKeyCheckIterator(fileParitionRecordKeyTripletItr);
  }

  class LazyKeyCheckIterator extends LazyIterableIterator<Tuple2<String, HoodieKey>, List<KeyLookupResult>> {

    private HoodieKeyLookupHandle keyLookupHandle;

    LazyKeyCheckIterator(
        Iterator<Tuple2<String, HoodieKey>> filePartitionRecordKeyTripletItr) {
      super(filePartitionRecordKeyTripletItr);
    }

    @Override
    protected void start() {
    }

    @Override
    protected List<HoodieKeyLookupHandle.KeyLookupResult> computeNext() {

      List<HoodieKeyLookupHandle.KeyLookupResult> ret = new ArrayList<>();
      try {
        // process one file in each go.
        while (inputItr.hasNext()) {
          Tuple2<String, HoodieKey> currentTuple = inputItr.next();
          String fileId = currentTuple._1;
          String partitionPath = currentTuple._2.getPartitionPath();
          String recordKey = currentTuple._2.getRecordKey();
          Pair<String, String> partitionPathFilePair = Pair.of(partitionPath, fileId);

          // lazily init state
          if (keyLookupHandle == null) {
            keyLookupHandle = new HoodieKeyLookupHandle(config, hoodieTable, partitionPathFilePair);
          }

          // if continue on current file
          if (keyLookupHandle.getPartitionPathFilePair().equals(partitionPathFilePair)) {
            keyLookupHandle.addKey(recordKey);
          } else {
            // do the actual checking of file & break out
            ret.add(keyLookupHandle.getLookupResult());
            keyLookupHandle = new HoodieKeyLookupHandle(config, hoodieTable, partitionPathFilePair);
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
