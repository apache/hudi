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
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.io.HoodieKeyLookupHandle;
import org.apache.hudi.io.HoodieKeyLookupResult;
import org.apache.hudi.table.HoodieTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Function accepting a tuple of {@link HoodieFileGroupId} and a record key and producing
 * a list of {@link HoodieKeyLookupResult} for every file identified by the file-group ids
 *
 * @param <I> type of the tuple of {@code (HoodieFileGroupId, <record-key>)}. Note that this is
 *           parameterized as generic such that this code could be reused for Spark as well
 */
public class HoodieBloomIndexCheckFunction<I> 
    implements Function<Iterator<I>, Iterator<List<HoodieKeyLookupResult>>>, Serializable {

  private final HoodieTable hoodieTable;
  private final HoodieWriteConfig config;
  private final SerializableFunction<I, HoodieFileGroupId> fileGroupIdExtractor;
  private final SerializableFunction<I, String> recordKeyExtractor;
  private final Option<String> lastInstant;

  public HoodieBloomIndexCheckFunction(HoodieTable hoodieTable,
                                       HoodieWriteConfig config,
                                       SerializableFunction<I, HoodieFileGroupId> fileGroupIdExtractor,
                                       SerializableFunction<I, String> recordKeyExtractor) {
    this.hoodieTable = hoodieTable;
    this.config = config;
    this.fileGroupIdExtractor = fileGroupIdExtractor;
    this.recordKeyExtractor = recordKeyExtractor;
    this.lastInstant = hoodieTable.getMetaClient().getCommitsTimeline().filterCompletedInstants().lastInstant().map(HoodieInstant::requestedTime);
  }

  @Override
  public Iterator<List<HoodieKeyLookupResult>> apply(Iterator<I> fileGroupIdRecordKeyPairIterator) {
    return new LazyKeyCheckIterator(fileGroupIdRecordKeyPairIterator);
  }

  protected class LazyKeyCheckIterator extends LazyIterableIterator<I, List<HoodieKeyLookupResult>> {

    private HoodieKeyLookupHandle keyLookupHandle;

    LazyKeyCheckIterator(Iterator<I> filePartitionRecordKeyTripletItr) {
      super(filePartitionRecordKeyTripletItr);
    }

    @Override
    protected List<HoodieKeyLookupResult> computeNext() {

      List<HoodieKeyLookupResult> ret = new ArrayList<>();
      try {
        // process one file in each go.
        while (inputItr.hasNext()) {
          I tuple = inputItr.next();

          HoodieFileGroupId fileGroupId = fileGroupIdExtractor.apply(tuple);
          String recordKey = recordKeyExtractor.apply(tuple);

          String fileId = fileGroupId.getFileId();
          String partitionPath = fileGroupId.getPartitionPath();

          Pair<String, String> partitionPathFilePair = Pair.of(partitionPath, fileId);

          // lazily init state
          if (keyLookupHandle == null) {
            keyLookupHandle = new HoodieKeyLookupHandle(config, hoodieTable, partitionPathFilePair, lastInstant);
          }

          // if continue on current file
          if (keyLookupHandle.getPartitionPathFileIDPair().equals(partitionPathFilePair)) {
            keyLookupHandle.addKey(recordKey);
          } else {
            // do the actual checking of file & break out
            ret.add(keyLookupHandle.getLookupResult());
            keyLookupHandle = new HoodieKeyLookupHandle(config, hoodieTable, partitionPathFilePair, lastInstant);
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
          throw (HoodieException) e;
        }

        throw new HoodieIndexException("Error checking bloom filter index. ", e);
      }

      return ret;
    }
  }
}
