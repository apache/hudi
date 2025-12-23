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

package org.apache.hudi.integ.testsuite.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.config.DFSPathSelectorConfig;
import org.apache.hudi.utilities.sources.helpers.DFSPathSelector;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * A custom dfs path selector used only for the hudi test suite. To be used only if workload is not run inline.
 */
@Slf4j
public class DFSTestSuitePathSelector extends DFSPathSelector {

  public DFSTestSuitePathSelector(TypedProperties props, Configuration hadoopConf) {
    super(props, hadoopConf);
  }

  @Override
  public Pair<Option<String>, Checkpoint> getNextFilePathsAndMaxModificationTime(
      Option<Checkpoint> lastCheckpoint, long sourceLimit) {

    Integer lastBatchId;
    Integer nextBatchId;
    try {
      if (lastCheckpoint.isPresent()) {
        lastBatchId = Integer.parseInt(lastCheckpoint.get().getCheckpointKey());
        nextBatchId = lastBatchId + 1;
      } else {
        lastBatchId = 0;
        nextBatchId = 1;
      }

      // obtain all eligible files for the batch
      List<FileStatus> eligibleFiles = new ArrayList<>();
      FileStatus[] fileStatuses = fs.globStatus(
          new Path(getStringWithAltKeys(props, DFSPathSelectorConfig.ROOT_INPUT_PATH), "*"));
      // Say input data is as follow input/1, input/2, input/5 since 3,4 was rolled back and 5 is new generated data
      // checkpoint from the latest commit metadata will be 2 since 3,4 has been rolled back. We need to set the
      // next batch id correctly as 5 instead of 3
      Option<String> correctBatchIdDueToRollback = Option.fromJavaOptional(Arrays.stream(fileStatuses)
          .map(f -> f.getPath().toString().split("/")[f.getPath().toString().split("/").length - 1])
          .filter(bid1 -> Integer.parseInt(bid1) > lastBatchId)
          .min((bid1, bid2) -> Integer.min(Integer.parseInt(bid1), Integer.parseInt(bid2))));
      if (correctBatchIdDueToRollback.isPresent() && Integer.parseInt(correctBatchIdDueToRollback.get()) > nextBatchId) {
        nextBatchId = Integer.parseInt(correctBatchIdDueToRollback.get());
      }
      log.info("Using DFSTestSuitePathSelector, checkpoint: {} sourceLimit: {} lastBatchId: {} nextBatchId: {}", lastCheckpoint, sourceLimit, lastBatchId, nextBatchId);
      for (FileStatus fileStatus : fileStatuses) {
        if (!fileStatus.isDirectory() || IGNORE_FILEPREFIX_LIST.stream()
            .anyMatch(pfx -> fileStatus.getPath().getName().startsWith(pfx))) {
          continue;
        } else if (Integer.parseInt(fileStatus.getPath().getName()) > lastBatchId && Integer.parseInt(fileStatus.getPath()
            .getName()) <= nextBatchId) {
          RemoteIterator<LocatedFileStatus> files = fs.listFiles(fileStatus.getPath(), true);
          while (files.hasNext()) {
            eligibleFiles.add(files.next());
          }
        }
      }

      // no data to readAvro
      if (eligibleFiles.size() == 0) {
        return new ImmutablePair<>(Option.empty(),
            lastCheckpoint.orElseGet(() -> new StreamerCheckpointV2(String.valueOf(Long.MIN_VALUE))));
      }
      // readAvro the files out.
      String pathStr = eligibleFiles.stream().map(f -> f.getPath().toString())
          .collect(Collectors.joining(","));

      return new ImmutablePair<>(Option.ofNullable(pathStr),
          new StreamerCheckpointV2(String.valueOf(nextBatchId)));
    } catch (IOException ioe) {
      throw new HoodieIOException(
          "Unable to readAvro from source from checkpoint: " + lastCheckpoint, ioe);
    }
  }

}
