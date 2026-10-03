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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.generic.IndexedRecord;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineV2.ACTION_ARCHIVED_META_FIELD;
import static org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineV2.METADATA_ARCHIVED_META_FIELD;
import static org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineV2.PLAN_ARCHIVED_META_FIELD;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/** Utilities for reading archived cleaner plans and metadata in either timeline layout. */
public class ArchivedCleanTimelineUtils {

  private ArchivedCleanTimelineUtils() {
  }

  /**
   * Creates a timeline for the selected completed cleans, supplied in descending requested-time order.
   * Payloads are loaded lazily in windows of at most {@code batchSize} selected instants; only the
   * current window is retained. This lets callers stop after enough partition rows, including when
   * some cleans have empty partition metadata, without loading the entire archived history.
   */
  public static HoodieTimeline getTimeline(HoodieTableMetaClient metaClient, List<HoodieInstant> selectedInstants,
                                          boolean loadPlans, int batchSize) {
    checkArgument(batchSize > 0, "Batch size must be positive");
    List<HoodieInstant> instants = new ArrayList<>(selectedInstants);
    TimelineFactory factory = metaClient.getTableFormat().getTimelineFactory();
    Map<String, Integer> positions = new HashMap<>();
    for (int i = 0; i < instants.size(); i++) {
      positions.put(instants.get(i).requestedTime(), i);
    }
    boolean legacy = metaClient.getTimelineLayoutVersion().getVersion() < TimelineLayoutVersion.VERSION_2;
    String actionField = legacy ? "actionType" : ACTION_ARCHIVED_META_FIELD;
    String contentField = legacy ? (loadPlans ? "hoodieCleanerPlan" : "hoodieCleanMetadata")
        : (loadPlans ? PLAN_ARCHIVED_META_FIELD : METADATA_ARCHIVED_META_FIELD);
    HoodieArchivedTimeline.LoadMode loadMode = loadPlans ? HoodieArchivedTimeline.LoadMode.PLAN : HoodieArchivedTimeline.LoadMode.METADATA;
    HoodieInstantReader reader = new HoodieInstantReader() {
      private final Map<String, byte[]> contents = new ConcurrentHashMap<>();

      @Override
      public synchronized InputStream getContentStream(HoodieInstant instant) {
        String instantTime = instant.requestedTime();
        if (!contents.containsKey(instantTime)) {
          Integer position = positions.get(instantTime);
          checkArgument(position != null, "Clean instant was not selected: " + instantTime);
          List<HoodieInstant> batch = instants.subList(position, position + Math.min(batchSize, instants.size() - position));
          Set<String> batchTimes = new HashSet<>(batch.stream().map(HoodieInstant::requestedTime).collect(Collectors.toList()));
          HoodieArchivedTimeline.TimeRangeFilter range = new HoodieArchivedTimeline.ClosedClosedTimeRangeFilter(
              batch.get(batch.size() - 1).requestedTime(), batch.get(0).requestedTime());
          contents.clear();
          factory.createArchivedTimelineLoader().loadInstants(metaClient, legacy ? null : range, loadMode,
              record -> HoodieTimeline.CLEAN_ACTION.equals(record.get(actionField).toString()),
              (time, record) -> {
                if (!range.isInRange(time) || !batchTimes.contains(time)) {
                  return;
                }
                Object content = record.get(contentField);
                if (content != null) {
                  byte[] bytes;
                  if (legacy) {
                    bytes = HoodieAvroUtils.avroToFileBytes((IndexedRecord) content);
                  } else {
                    ByteBuffer buffer = ((ByteBuffer) content).duplicate();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                  }
                  contents.put(time, bytes);
                }
              });
        }
        byte[] bytes = contents.get(instantTime);
        if (bytes == null) {
          throw new HoodieIOException("Missing archived clean " + (loadPlans ? "plan" : "metadata") + " for " + instantTime);
        }
        return new ByteArrayInputStream(bytes);
      }
    };
    return factory.createDefaultTimeline(instants.stream().sorted(
        TimelineLayout.fromVersion(metaClient.getTimelineLayoutVersion()).getInstantComparator().requestedTimeOrderedComparator()), reader);
  }
}
