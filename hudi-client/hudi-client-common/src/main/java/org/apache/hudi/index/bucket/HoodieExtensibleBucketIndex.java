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

package org.apache.hudi.index.bucket;

import org.apache.hudi.client.utils.LazyIterableIterator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// TODO: Implement the HoodieExtensibleBucketIndex class for extensible bucket
public class HoodieExtensibleBucketIndex extends HoodieBucketIndex {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieExtensibleBucketIndex.class);

  public HoodieExtensibleBucketIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(HoodieData<HoodieRecord<R>> records, HoodieEngineContext context, HoodieTable hoodieTable) throws HoodieIndexException {
    // Get bucket location mapper for the given partitions
    List<String> partitions = records.map(HoodieRecord::getPartitionPath).distinct().collectAsList();
    LOG.info("Get ExtensibleBucketMapper for partitions: " + partitions);
    ExtensibleBucketMapper bucketMapper = new ExtensibleBucketMapper(hoodieTable, partitions);
    return records.mapPartitions(iter -> new LazyIterableIterator<HoodieRecord<R>, HoodieRecord<R>>(iter) {
      @Override
      protected HoodieRecord<R> computeNext() {
        HoodieRecord<R> record = inputItr.next();
        Option<HoodieRecordLocation> location = bucketMapper.getRecordLocation(record.getKey());
        return HoodieIndexUtils.tagAsNewRecordIfNeeded(record, location);
      }
    }, false);
  }

  public class ExtensibleBucketMapper implements Serializable {
    /**
     * Mapping from partitionPath -> bucket identifier.
     */
    private final Map<String, ExtensibleBucketIdentifier> partitionToIdentifier;

    public ExtensibleBucketMapper(HoodieTable table, List<String> partitions) {
      partitionToIdentifier = partitions.stream().collect(Collectors.toMap(p -> p, p -> {
        // Get the bucket identifier for the partition
        return ExtensibleBucketIndexUtils.loadExtensibleBucketIdentifierWithExistLocation(table, p);
      }));
    }

    public Option<HoodieRecordLocation> getRecordLocation(HoodieKey key) {
      String partitionPath = key.getPartitionPath();
      Option<HoodieRecordLocation> locationOption = partitionToIdentifier.get(partitionPath).getRecordLocation(key, indexKeyFields);
      if (locationOption.isEmpty()) {
        // locationOption is empty, means that the record belongs to a new bucket which not exists in the fs
        // extensible-bucket index will tag these records in new bucket with calculated location by index num
        locationOption = Option.of(partitionToIdentifier.get(partitionPath).getLogicalRecordLocationByIndex(key, indexKeyFields));
      }
      return locationOption;
    }

  }
}
