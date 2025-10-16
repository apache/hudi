/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.client.SecondaryIndexStats;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.reduceByKeys;

public class SecondaryIndexMapper extends MetadataIndexMapper {
  public SecondaryIndexMapper(HoodieWriteConfig dataWriteConfig) {
    super(dataWriteConfig);
  }

  @Override
  protected List<HoodieRecord> generateRecords(WriteStatus writeStatus) {
    List<HoodieRecord> secondaryIndexRecords = new ArrayList<>(writeStatus.getIndexStats().getSecondaryIndexStats().size());
    for (Map.Entry<String, List<SecondaryIndexStats>> entry : writeStatus.getIndexStats().getSecondaryIndexStats().entrySet()) {
      String indexPartitionName = entry.getKey();
      List<SecondaryIndexStats> secondaryIndexStats = entry.getValue();
      for (SecondaryIndexStats stat : secondaryIndexStats) {
        secondaryIndexRecords.add(HoodieMetadataPayload.createSecondaryIndexRecord(stat.getRecordKey(), stat.getSecondaryKeyValue(), indexPartitionName, stat.isDeleted()));
      }
    }
    return secondaryIndexRecords;
  }

  /**
   * Post-processes secondary index records by deduplicating records with the same metadata record key.
   * This handles partition path updates where the same record generates both DELETE and INSERT.
   * Note that unlike record index, the record may have both partition path update and secondary key
   * update; in this case, the delete record to the secondary index has to be honored. This is
   * guaranteed as part of the reduceByKeys operation as in such a case, the DELETE and INSERT
   * to the secondary index have different metadata record key.
   */
  @Override
  public HoodieData<HoodieRecord> postProcess(HoodieData<HoodieRecord> records) {
    // Deduplicate by metadata record key (secondaryKey$recordKey)
    // usePartitionInKey = false because SI keys are globally unique
    int parallelism = Math.max(
        records.getNumPartitions(), dataWriteConfig.getMetadataConfig().getSecondaryIndexParallelism());
    return reduceByKeys(records, parallelism, false);
  }
}
