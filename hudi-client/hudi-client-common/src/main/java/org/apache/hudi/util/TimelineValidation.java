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

package org.apache.hudi.util;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;

public class TimelineValidation {
  private static final Logger LOG = LogManager.getLogger(TimelineValidation.class);

  private final String basePath;

  public TimelineValidation(String basePath) {
    this.basePath = basePath;
  }

  public void run() {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(new Configuration())
        .setBasePath(basePath).setLoadActiveTimelineOnLoad(true)
        .build();
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();

    // Partition -> File group ID -> List of pairs of (version (instant time), list of files)
    Map<String, Map<String, List<Pair<String, List<String>>>>> partitionFileGroupInfo = new HashMap<>();

    for (HoodieInstant instant : timeline.getInstants()) {
      LOG.warn("------ parsing " + instant);
      switch (instant.getAction()) {
        case COMMIT_ACTION:
          try {
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
            HoodieInstant compactionInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, COMPACTION_ACTION, instant.getTimestamp());
            HoodieCompactionPlan compactionPlan = TimelineMetadataUtils.deserializeCompactionPlan(timeline.readCompactionPlanAsBytes(compactionInstant).get());
            compactionPlan.getOperations();
          } catch (IOException e) {
            e.printStackTrace();
          }
          break;
        case DELTA_COMMIT_ACTION:
          try {
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
            commitMetadata.getPartitionToWriteStats().entrySet().forEach(entry -> {
              String partition = entry.getKey();
              List<HoodieWriteStat> writeStats = entry.getValue();
              Map<String, List<Pair<String, List<String>>>> fileGroupInfo =
                  partitionFileGroupInfo.computeIfAbsent(partition, k -> new HashMap<>());
              for (HoodieWriteStat writeStat : writeStats) {
                List<Pair<String, List<String>>> pairList = fileGroupInfo.computeIfAbsent(writeStat.getFileId(), k -> new ArrayList<>());
                String path = writeStat.getPath();
                String commitTime = FSUtils.getCommitTime(new Path(path).getName());

                if (pairList.isEmpty()) {
                  List<String> list = new ArrayList<>();
                  list.add(path);
                  pairList.add(Pair.of(commitTime, list));
                } else if (pairList.get(0).getLeft().equals(commitTime)) {
                  pairList.get(0).getRight().add(path);
                } else if (pairList.get(0).getLeft().compareTo(commitTime) < 0) {
                  Pair<String, List<String>> fileSliceInfo = pairList.get(0);
                  pairList.clear();
                  List<String> list = new ArrayList<>();
                  list.add(path);
                  pairList.add(Pair.of(commitTime, list));
                  pairList.add(fileSliceInfo);
                } else {
                  if (pairList.size() < 2) {
                    LOG.error("Could not find previous file slice: fileId=" + writeStat.getFileId() + " commitTime=" + commitTime + " path=" + path);
                    LOG.error(pairList);
                  } else if (pairList.get(1).getLeft().equals(commitTime)) {
                    LOG.error("File added to old file slice: fileId=" + writeStat.getFileId() + " commitTime=" + commitTime + " path=" + path);
                    pairList.get(1).getRight().add("**" + path);
                    LOG.error(pairList);
                  } else {
                    LOG.error("Does not match latest or second file slices: fileId=" + writeStat.getFileId() + " commitTime=" + commitTime + " path=" + path);
                    LOG.error(pairList);
                  }
                }
              }
            });
          } catch (IOException e) {
            throw new HoodieIOException("Failed to parse " + instant, e);
          }
          break;
        default:
          LOG.warn("Ignoring instant " + instant);
      }
    }
  }
}
