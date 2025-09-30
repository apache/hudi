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

package org.apache.hudi.client;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Class to assist with streaming writes to metadata table.
 */
public class StreamingMetadataWriteHandler {

  // Mappings of {instant -> metadata writer option} for each action in data table.
  // This will be cleaned up when action is completed or when write client is closed.
  private final Map<String, Option<HoodieTableMetadataWriter>> metadataWriterMap = new HashMap<>();

  /**
   * Called by data table write client and table service client to perform streaming writes to metadata table.
   *
   * @param table                  The {@link HoodieTable} instance for data table of interest.
   * @param dataTableWriteStatuses The {@link WriteStatus} from data table writes.
   * @param instantTime            The instant time of interest.
   * @param enforceCoalesceWithRepartition true when repartition has to be added to dag to coalesce data table write statuses to 1. false otherwise.
   * @param coalesceDivisorForDataTableWrites assist with determining the coalesce parallelism for data table write statuses. N data table write status
   *                                          spark partitions will be divied by this value to find the coalesce parallelism.
   * @return {@link HoodieData} of {@link WriteStatus} referring to both data table writes and partial metadata table writes.
   */
  public HoodieData<WriteStatus> streamWriteToMetadataTable(HoodieTable table, HoodieData<WriteStatus> dataTableWriteStatuses, String instantTime,
                                                           boolean enforceCoalesceWithRepartition, int coalesceDivisorForDataTableWrites) {
    Option<HoodieTableMetadataWriter> metadataWriterOpt = getMetadataWriter(instantTime, table);
    ValidationUtils.checkState(metadataWriterOpt.isPresent(),
        "Cannot instantiate metadata writer for the table of interest " + table.getMetaClient().getBasePath());
    return streamWriteToMetadataTable(dataTableWriteStatuses, metadataWriterOpt.get(), table, instantTime, enforceCoalesceWithRepartition,
        coalesceDivisorForDataTableWrites);
  }

  /**
   * To be invoked by write client or table service client to complete the write to metadata table.
   *
   * <p>When streaming writes is enabled, writes to left over metadata partitions
   * which is not covered in {@link #streamWriteToMetadataTable(HoodieTable, HoodieData, String, boolean)},
   * otherwise writes to metadata table in legacy way(batch update without partial updates).
   *
   * @param table       The {@link HoodieTable} instance for data table of interest.
   * @param instantTime The instant time of interest.
   * @param metadata    The {@link HoodieCommitMetadata} of interest.
   * @param partialMetadataWriteStats List of {@link HoodieWriteStat}s referring to partial writes completed in metadata table with streaming writes.
   */
  public void commitToMetadataTable(HoodieTable table,
                                    String instantTime,
                                    HoodieCommitMetadata metadata,
                                    List<HoodieWriteStat> partialMetadataWriteStats) {
    Option<HoodieTableMetadataWriter> metadataWriterOpt = getMetadataWriter(instantTime, table);
    ValidationUtils.checkState(metadataWriterOpt.isPresent(), "Should not be reachable. Metadata Writer should have been instantiated by now");
    try (HoodieTableMetadataWriter metadataWriter = metadataWriterOpt.get()) {
      metadataWriter.completeStreamingCommit(instantTime, table.getContext(), partialMetadataWriteStats, metadata);
    } catch (Exception e) {
      throw new HoodieException("Error while completing streaming commit to metadata with instant " + instantTime, e);
    } finally {
      metadataWriterMap.remove(instantTime);
    }
  }

  private HoodieData<WriteStatus> streamWriteToMetadataTable(HoodieData<WriteStatus> dataTableWriteStatuses,
                                                             HoodieTableMetadataWriter metadataWriter,
                                                             HoodieTable table,
                                                             String instantTime,
                                                             boolean enforceCoalesceWithRepartition,
                                                             int coalesceDivisorForDataTableWrites) {
    HoodieData<WriteStatus> mdtWriteStatuses = metadataWriter.streamWriteToMetadataPartitions(dataTableWriteStatuses, instantTime);
    mdtWriteStatuses.persist("MEMORY_AND_DISK_SER", table.getContext(), HoodieData.HoodieDataCacheKey.of(table.getMetaClient().getBasePath().toString(), instantTime));
    HoodieData<WriteStatus> coalescedDataWriteStatuses;
    int coalesceParallelism = Math.max(1, dataTableWriteStatuses.getNumPartitions() / coalesceDivisorForDataTableWrites);
    if (enforceCoalesceWithRepartition) {
      // with bulk insert and NONE sort mode, simple coalesce on datatable write statuses also impact record key generation stages.
      // and hence we are adding a partitioner to cut the chain so that coalesce(1) here does not impact record key generation stages.
      coalescedDataWriteStatuses = HoodieJavaRDD.of(HoodieJavaRDD.getJavaRDD(dataTableWriteStatuses)
          .mapToPair((PairFunction<WriteStatus, Boolean, WriteStatus>) writeStatus -> new Tuple2(true, writeStatus))
          .partitionBy(new CoalescingPartitioner(coalesceParallelism))
          .map((Function<Tuple2<Boolean, WriteStatus>, WriteStatus>) booleanWriteStatusTuple2 -> booleanWriteStatusTuple2._2));
    } else {
      coalescedDataWriteStatuses = dataTableWriteStatuses.coalesce(coalesceParallelism);
    }
    return coalescedDataWriteStatuses.union(mdtWriteStatuses);
  }

  /**
   * Returns the table metadata writer option with given instant time {@code triggeringInstant}.
   *
   * @param triggeringInstant The instant that triggers the metadata writes.
   * @param table             The hoodie table
   *
   * @return The metadata writer option.
   */
  @VisibleForTesting
  synchronized Option<HoodieTableMetadataWriter> getMetadataWriter(String triggeringInstant, HoodieTable table) {

    if (!table.getMetaClient().getTableConfig().getTableVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      return Option.empty();
    }

    if (this.metadataWriterMap.containsKey(triggeringInstant)) {
      return this.metadataWriterMap.get(triggeringInstant);
    }

    Option<HoodieTableMetadataWriter> metadataWriterOpt = table.getMetadataWriter(triggeringInstant, true);
    metadataWriterMap.put(triggeringInstant, metadataWriterOpt); // populate this for every new instant time.
    // if metadata table does not exist, the map will contain an entry, with value Option.empty.
    // if not, it will contain the metadata writer instance.

    // start the commit in metadata table.
    metadataWriterOpt.ifPresent(metadataWriter -> metadataWriter.startCommit(triggeringInstant));

    return metadataWriterMap.get(triggeringInstant);
  }
}
