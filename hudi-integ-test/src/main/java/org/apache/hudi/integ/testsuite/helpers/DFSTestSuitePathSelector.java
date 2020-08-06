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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.sources.helpers.DFSPathSelector;

/**
 * A custom dfs path selector used only for the hudi test suite. To be used only if workload is not run inline.
 */
public class DFSTestSuitePathSelector extends DFSPathSelector {

  public DFSTestSuitePathSelector(TypedProperties props, Configuration hadoopConf) {
    super(props, hadoopConf);
  }

  @Override
  public Pair<Option<String>, String> getNextFilePathsAndMaxModificationTime(
      Option<String> lastCheckpointStr, long sourceLimit) {

    Integer lastBatchId;
    Integer nextBatchId;
    try {
      if (lastCheckpointStr.isPresent()) {
        lastBatchId = Integer.parseInt(lastCheckpointStr.get());
        nextBatchId = lastBatchId + 1;
      } else {
        lastBatchId = -1;
        nextBatchId = 0;
      }
      // obtain all eligible files for the batch
      List<FileStatus> eligibleFiles = new ArrayList<>();
      FileStatus[] fileStatuses = fs.globStatus(
          new Path(props.getString(Config.ROOT_INPUT_PATH_PROP), "*"));
      for (FileStatus fileStatus : fileStatuses) {
        if (!fileStatus.isDirectory() || IGNORE_FILEPREFIX_LIST.stream()
            .anyMatch(pfx -> fileStatus.getPath().getName().startsWith(pfx))) {
          continue;
        } else if (fileStatus.getPath().getName().compareTo(lastBatchId.toString()) > 0 && fileStatus.getPath()
            .getName().compareTo(nextBatchId.toString()) <= 0) {
          RemoteIterator<LocatedFileStatus> files = fs.listFiles(fileStatus.getPath(), true);
          while (files.hasNext()) {
            eligibleFiles.add(files.next());
          }
        }
      }
      // no data to readAvro
      if (eligibleFiles.size() == 0) {
        return new ImmutablePair<>(Option.empty(),
            lastCheckpointStr.orElseGet(() -> String.valueOf(Long.MIN_VALUE)));
      }
      // readAvro the files out.
      String pathStr = eligibleFiles.stream().map(f -> f.getPath().toString())
          .collect(Collectors.joining(","));

      return new ImmutablePair<>(Option.ofNullable(pathStr), String.valueOf(nextBatchId));
    } catch (IOException ioe) {
      throw new HoodieIOException(
          "Unable to readAvro from source from checkpoint: " + lastCheckpointStr, ioe);
    }
  }

}
