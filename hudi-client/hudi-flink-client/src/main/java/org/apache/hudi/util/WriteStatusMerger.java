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

package org.apache.hudi.util;

import org.apache.hudi.client.IndexStats;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieWriteStat;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper clazz to merge {@link WriteStatus} that belong to one file group during multiple mini-batches writing.
 *
 * <p>CAUTION: The merge can be buggy, we need to maintain the new variables for the {@link WriteStatus}.
 */
public class WriteStatusMerger {
  public static WriteStatus merge(WriteStatus writeStatus1, WriteStatus writeStatus2) {
    WriteStatus mergedStatus = new WriteStatus(writeStatus1.isTrackingSuccessfulWrites(), writeStatus1.getFailureFraction(), writeStatus1.isMetadataTable());

    mergedStatus.setFileId(writeStatus1.getFileId());
    mergedStatus.setPartitionPath(writeStatus1.getPartitionPath());

    // runtime write metrics
    mergedStatus.getErrors().putAll(writeStatus1.getErrors());
    mergedStatus.getErrors().putAll(writeStatus2.getErrors());
    mergedStatus.getFailedRecords().addAll(writeStatus1.getFailedRecords());
    mergedStatus.getFailedRecords().addAll(writeStatus2.getFailedRecords());
    if (writeStatus1.hasGlobalError()) {
      mergedStatus.setGlobalError(writeStatus1.getGlobalError());
    }
    if (writeStatus2.hasGlobalError()) {
      mergedStatus.setGlobalError(writeStatus2.getGlobalError());
    }
    mergedStatus.setTotalRecords(writeStatus1.getTotalRecords() + writeStatus2.getTotalRecords());
    mergedStatus.setTotalErrorRecords(writeStatus1.getTotalErrorRecords() + writeStatus2.getTotalErrorRecords());

    // write statistics
    HoodieWriteStat mergedStat = WriteStatMerger.merge(writeStatus1.getStat(), writeStatus2.getStat());
    mergedStatus.setStat(mergedStat);

    // index statistics
    IndexStats mergedIndexStats = mergedStatus.getIndexStats();
    mergedIndexStats.getSecondaryIndexStats().putAll(writeStatus1.getIndexStats().getSecondaryIndexStats());
    writeStatus2.getIndexStats().getSecondaryIndexStats().forEach(
        (k,v) -> mergedIndexStats.getSecondaryIndexStats().merge(k, v,
            (list1, list2) -> Stream.concat(list1.stream(), list2.stream()).collect(Collectors.toList())));
    mergedIndexStats.getWrittenRecordDelegates().addAll(writeStatus1.getIndexStats().getWrittenRecordDelegates());
    mergedIndexStats.getWrittenRecordDelegates().addAll(writeStatus2.getIndexStats().getWrittenRecordDelegates());
    return mergedStatus;
  }
}
