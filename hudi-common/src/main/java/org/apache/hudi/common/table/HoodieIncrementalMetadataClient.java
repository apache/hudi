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

package org.apache.hudi.common.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <code>HoodieIncrementalMetadataClient</code> allows to fetch details about incremental updates
 * to the table using just the metadata.
 *
 */
public class HoodieIncrementalMetadataClient implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieTableMetaClient.class);
  private HoodieTableMetaClient metaClient;

  /**
   * Creates an HoodieIncrementalMetadataClient object.
   *
   * @param conf Hoodie Configuration
   * @param basePath Base path of the Table
   */
  public HoodieIncrementalMetadataClient(Configuration conf, String basePath) {
    this(HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build());
  }

  /**
   * Create HoodieIncrementalMetadataClient from HoodieTableMetaClient.
   */
  public HoodieIncrementalMetadataClient(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
  }

  /**
   * Get the underlying meta client object.
   *
   * @return Meta client
   */
  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  /**
   * Relods the underlying meta client.
   */
  public void reload() {
    this.metaClient.reloadActiveTimeline();
  }

  /**
   * Gets the partitions modified from a hoodie instant.
   *
   * @param timeline Hoodie Timeline
   * @param hoodieInstant Hoodie instant
   * @return Pairs of Partition and the Filenames
   * @throws HoodieIOException
   */
  private Stream<String> getPartitionNameFromInstant(
      HoodieTimeline timeline,
      HoodieInstant hoodieInstant) throws HoodieIOException {
    HoodieCommitMetadata commitMetadata;
    switch (hoodieInstant.getAction()) {
      case HoodieTimeline.COMMIT_ACTION:
      case HoodieTimeline.DELTA_COMMIT_ACTION:
        try {
          commitMetadata = HoodieCommitMetadata
              .fromBytes(timeline.getInstantDetails(hoodieInstant).get(),
                  HoodieCommitMetadata.class);
        } catch (IOException e) {
          throw new HoodieIOException("Unable to deserialize instant from avro", e);
        }
        return commitMetadata.getPartitionToWriteStats().keySet().stream();
      case HoodieTimeline.CLEAN_ACTION:
        try {
          HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils.deserializeHoodieCleanMetadata(timeline.getInstantDetails(hoodieInstant).get());
          return cleanMetadata.getPartitionMetadata().keySet().stream();
        } catch (IOException e) {
          throw new HoodieIOException("unable to deserialize clean plan from avro", e);
        }
      default:
        return Stream.empty();
    }
  }

  /**
   * Filters the instances from the timeline and returns the resulting partitions.
   *
   * @param timeline Hoodie timeline
   * @param beginTs  Start commit timestamp
   * @param endTs    End commit timestamp
   * @return
   */
  private Stream<String> getPartitionNames(HoodieTimeline timeline, String beginTs, String endTs) {
    return timeline.getInstants()
        .filter(instant -> (HoodieTimeline.compareTimestamps(instant.getTimestamp(), HoodieTimeline.GREATER_THAN, beginTs) 
            && (endTs == null || HoodieTimeline.compareTimestamps(instant.getTimestamp(), HoodieTimeline.LESSER_THAN_OR_EQUALS, endTs
        ))))
        .flatMap(i -> getPartitionNameFromInstant(timeline, i)).filter(s -> !s.isEmpty()).distinct();
  }

  /**
   * Gets the list of partitions written since a commit timestamp.
   * The filter will exclude begin timestamp.
   *
   * @param beginInstantTs Start commit timestamp
   * @return List of partition paths
   */
  public List<String> getPartitionsMutatedSince(String beginInstantTs) {
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedInstants();
    return getPartitionNames(timeline, beginInstantTs, null).collect(Collectors.toList());
  }

  /**
   * Gets the list of partitions written to between two timestamps.
   * The filter will exclude start timestamp and include end timestamp in the result.
   *
   * @param beginInstantTs Start commit timestamp
   * @param endInstantTs   End commit timestamp
   * @return List of partition paths
   */
  public List<String> getPartitionsMutatedBetween(String beginInstantTs, String endInstantTs) {
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedInstants();
    return getPartitionNames(timeline, beginInstantTs, endInstantTs).collect(Collectors.toList());
  }
}
