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

import org.apache.hudi.client.SecondaryIndexStats;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.SECONDARY_INDEX;

/**
 * For now this is a placeholder to generate all MDT records in one place.
 * Once <a href="https://github.com/apache/hudi/pull/13226">Refactor MDT update logic with Indexer</a> is landed,
 * we will leverage the new abstraction to generate MDT records.
 */
public class MetadataIndexGenerator implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataIndexGenerator.class);

  /**
   * Abstract base class for metadata index mappers. Each index type can extend this to implement
   * its own record generation and post-processing logic.
   */
  public abstract static class MetadataIndexMapper implements SerializableFunction<WriteStatus, Iterator<HoodieRecord>>, Serializable {
    protected final HoodieWriteConfig dataWriteConfig;

    public MetadataIndexMapper(HoodieWriteConfig dataWriteConfig) {
      this.dataWriteConfig = dataWriteConfig;
    }

    /**
     * Generates metadata index records from a WriteStatus.
     *
     * @param writeStatus the write status to process
     * @return list of metadata records
     */
    protected abstract List<HoodieRecord> generateRecords(WriteStatus writeStatus);

    /**
     * Post-processes the generated records. Default implementation returns records as-is.
     * Subclasses can override to add deduplication, validation, or other transformations.
     *
     * @param records the generated records
     * @return post-processed records
     */
    public HoodieData<HoodieRecord> postProcess(HoodieData<HoodieRecord> records) {
      return records;
    }

    @Override
    public Iterator<HoodieRecord> apply(WriteStatus writeStatus) throws Exception {
      return generateRecords(writeStatus).iterator();
    }
  }

  /**
   * Mapper for Record Level Index (RLI).
   */
  static class RecordIndexMapper extends MetadataIndexMapper {
    public RecordIndexMapper(HoodieWriteConfig dataWriteConfig) {
      super(dataWriteConfig);
    }

    @Override
    protected List<HoodieRecord> generateRecords(WriteStatus writeStatus) {
      return processWriteStatusForRLI(writeStatus, dataWriteConfig);
    }

    // RLI doesn't need post-processing, uses default implementation
  }

  /**
   * Mapper for Secondary Index.
   */
  static class SecondaryIndexMapper extends MetadataIndexMapper {
    public SecondaryIndexMapper(HoodieWriteConfig dataWriteConfig) {
      super(dataWriteConfig);
    }

    @Override
    protected List<HoodieRecord> generateRecords(WriteStatus writeStatus) {
      return processWriteStatusForSecondaryIndex(writeStatus);
    }

    /**
     * Post-processes secondary index records by deduplicating records with the same metadata record key.
     * This handles partition path updates where the same record generates both DELETE and INSERT.
     */
    @Override
    public HoodieData<HoodieRecord> postProcess(HoodieData<HoodieRecord> records) {
      // Deduplicate by metadata record key (secondaryKey$recordKey)
      // usePartitionInKey = false because SI keys are globally unique
      return HoodieTableMetadataUtil.reduceByKeys(records,
          dataWriteConfig.getMetadataConfig().getSecondaryIndexParallelism(), false);
    }
  }

  /**
   * Composite mapper that delegates to multiple index-specific mappers.
   */
  static class CompositeMetadataIndexMapper implements SerializableFunction<WriteStatus, Iterator<HoodieRecord>> {
    private final List<MetadataIndexMapper> mappers;

    public CompositeMetadataIndexMapper(List<MetadataPartitionType> enabledPartitionTypes, HoodieWriteConfig dataWriteConfig) {
      this.mappers = new ArrayList<>();
      if (enabledPartitionTypes.contains(RECORD_INDEX)) {
        mappers.add(new RecordIndexMapper(dataWriteConfig));
      }
      if (enabledPartitionTypes.contains(SECONDARY_INDEX)) {
        mappers.add(new SecondaryIndexMapper(dataWriteConfig));
      }
      // yet to add support for more partitions.
      // bloom filter
      // expression index.
    }

    @Override
    public Iterator<HoodieRecord> apply(WriteStatus writeStatus) throws Exception {
      List<HoodieRecord> allRecords = new ArrayList<>();
      for (MetadataIndexMapper mapper : mappers) {
        allRecords.addAll(mapper.generateRecords(writeStatus));
      }
      return allRecords.iterator();
    }

    public List<MetadataIndexMapper> getMappers() {
      return mappers;
    }
  }

  protected static List<HoodieRecord> processWriteStatusForRLI(WriteStatus writeStatus, HoodieWriteConfig dataWriteConfig) {
    List<HoodieRecord> allRecords = new ArrayList<>();
    for (HoodieRecordDelegate recordDelegate : writeStatus.getIndexStats().getWrittenRecordDelegates()) {
      if (!writeStatus.isErrored(recordDelegate.getHoodieKey())) {
        if (recordDelegate.getIgnoreIndexUpdate()) {
          continue;
        }
        HoodieRecord hoodieRecord;
        Option<HoodieRecordLocation> newLocation = recordDelegate.getNewLocation();
        if (newLocation.isPresent()) {
          if (recordDelegate.getCurrentLocation().isPresent()) {
            // This is an update, no need to update index if the location has not changed
            // newLocation should have the same fileID as currentLocation. The instantTimes differ as newLocation's
            // instantTime refers to the current commit which was completed.
            if (!recordDelegate.getCurrentLocation().get().getFileId().equals(newLocation.get().getFileId())) {
              final String msg = String.format("Detected update in location of record with key %s from %s to %s. The fileID should not change.",
                  recordDelegate, recordDelegate.getCurrentLocation().get(), newLocation.get());
              LOG.error(msg);
              throw new HoodieMetadataException(msg);
            }
            // for updates, we can skip updating RLI partition in MDT
          } else {
            // Insert new record case
            hoodieRecord = HoodieMetadataPayload.createRecordIndexUpdate(
                recordDelegate.getRecordKey(), recordDelegate.getPartitionPath(),
                newLocation.get().getFileId(), newLocation.get().getInstantTime(), dataWriteConfig.getWritesFileIdEncoding());
            allRecords.add(hoodieRecord);
          }
        } else {
          // Delete existing index for a deleted record
          hoodieRecord = HoodieMetadataPayload.createRecordIndexDelete(recordDelegate.getRecordKey(), recordDelegate.getPartitionPath(), dataWriteConfig.isPartitionedRecordIndexEnabled());
          allRecords.add(hoodieRecord);
        }
      }
    }
    return allRecords;
  }

  protected static List<HoodieRecord> processWriteStatusForSecondaryIndex(WriteStatus writeStatus) {
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
}
