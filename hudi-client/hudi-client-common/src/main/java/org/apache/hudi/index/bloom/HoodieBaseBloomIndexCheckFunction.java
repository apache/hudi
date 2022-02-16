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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.io.HoodieKeyLookupHandle;
import org.apache.hudi.io.HoodieKeyLookupResult;
import org.apache.hudi.table.HoodieTable;

import java.util.function.Function;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Function performing actual checking of list containing (fileId, hoodieKeys) against the actual files.
 */
public class HoodieBaseBloomIndexCheckFunction
    implements Function<Iterator<Pair<String, HoodieKey>>, Iterator<List<HoodieKeyLookupResult>>> {

  private final HoodieTable hoodieTable;

  private final HoodieWriteConfig config;

  public HoodieBaseBloomIndexCheckFunction(HoodieTable hoodieTable, HoodieWriteConfig config) {
    this.hoodieTable = hoodieTable;
    this.config = config;
  }

  @Override
  public Iterator<List<HoodieKeyLookupResult>> apply(Iterator<Pair<String, HoodieKey>> filePartitionRecordKeyTripletItr) {
    return new LazyKeyCheckIterator(filePartitionRecordKeyTripletItr);
  }

  class LazyKeyCheckIterator extends LazyIterableIterator<Pair<String, HoodieKey>, List<HoodieKeyLookupResult>> {

    private HoodieKeyLookupHandle keyLookupHandle;

    LazyKeyCheckIterator(Iterator<Pair<String, HoodieKey>> filePartitionRecordKeyTripletItr) {
      super(filePartitionRecordKeyTripletItr);
    }

    @Override
    protected void start() {
    }

    @Override
    protected List<HoodieKeyLookupResult> computeNext() {
      List<HoodieKeyLookupResult> ret = new ArrayList<>();
      try {
        // process one file in each go.
        while (inputItr.hasNext()) {
          Pair<String, HoodieKey> currentTuple = inputItr.next();
          String fileId = currentTuple.getLeft();
          String partitionPath = currentTuple.getRight().getPartitionPath();
          String recordKey = currentTuple.getRight().getRecordKey();
          Pair<String, String> partitionPathFilePair = Pair.of(partitionPath, fileId);

          // lazily init state
          if (keyLookupHandle == null) {
            keyLookupHandle = new HoodieKeyLookupHandle(config, hoodieTable, partitionPathFilePair);
          }

          // if continue on current file
          if (keyLookupHandle.getPartitionPathFileIDPair().equals(partitionPathFilePair)) {
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
