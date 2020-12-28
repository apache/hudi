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

package org.apache.hudi.metadata;

import org.apache.avro.Schema;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Provides functionality to convert timeline instants to table metadata records and then merge by key. Specify
 *  a filter to limit keys that are merged and stored in memory.
 */
public class HoodieMetadataMergedInstantRecordScanner {

  private static final Logger LOG = LogManager.getLogger(HoodieMetadataMergedInstantRecordScanner.class);

  HoodieTableMetaClient metaClient;
  private List<HoodieInstant> instants;
  private Option<String> lastSyncTs;
  private Set<String> mergeKeyFilter;
  protected final ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records;

  public HoodieMetadataMergedInstantRecordScanner(HoodieTableMetaClient metaClient, List<HoodieInstant> instants,
                                                  Option<String> lastSyncTs, Schema readerSchema, Long maxMemorySizeInBytes,
                                                  String spillableMapBasePath, Set<String> mergeKeyFilter) throws IOException {
    this.metaClient = metaClient;
    this.instants = instants;
    this.lastSyncTs = lastSyncTs;
    this.mergeKeyFilter = mergeKeyFilter != null ? mergeKeyFilter : Collections.emptySet();
    this.records = new ExternalSpillableMap<>(maxMemorySizeInBytes, spillableMapBasePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(readerSchema));

    scan();
  }

  /**
   * Converts instants in scanner to metadata table records and processes each record.
   *
   * @param
   * @throws IOException
   */
  private void scan() {
    for (HoodieInstant instant : instants) {
      try {
        Option<List<HoodieRecord>> records = HoodieTableMetadataUtil.convertInstantToMetaRecords(metaClient, instant, lastSyncTs);
        if (records.isPresent()) {
          records.get().forEach(record -> processNextRecord(record));
        }
      } catch (Exception e) {
        LOG.error(String.format("Got exception when processing timeline instant %s", instant.getTimestamp()), e);
        throw new HoodieException(String.format("Got exception when processing timeline instant %s", instant.getTimestamp()), e);
      }
    }
  }

  /**
   * Process metadata table record by merging with existing record if it is a part of the key filter.
   *
   * @param hoodieRecord
   */
  private void processNextRecord(HoodieRecord<? extends HoodieRecordPayload> hoodieRecord) {
    String key = hoodieRecord.getRecordKey();
    if (mergeKeyFilter.isEmpty() || mergeKeyFilter.contains(key)) {
      if (records.containsKey(key)) {
        // Merge and store the merged record
        HoodieRecordPayload combinedValue = hoodieRecord.getData().preCombine(records.get(key).getData());
        records.put(key, new HoodieRecord<>(new HoodieKey(key, hoodieRecord.getPartitionPath()), combinedValue));
      } else {
        // Put the record as is
        records.put(key, hoodieRecord);
      }
    }
  }

  /**
   * Retrieve merged hoodie record for given key.
   *
   * @param key of the record to retrieve
   * @return {@code HoodieRecord} if key was found else {@code Option.empty()}
   */
  public Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKey(String key) {
    return Option.ofNullable((HoodieRecord) records.get(key));
  }
}
