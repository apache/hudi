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

package com.uber.hoodie.utilities.sources.helpers;

import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.common.util.collection.ImmutablePair;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class DFSPathSelector {

  /**
   * Configs supported
   */
  static class Config {

    private static final String ROOT_INPUT_PATH_PROP = "hoodie.deltastreamer.source.dfs.root";
  }

  private static final List<String> IGNORE_FILEPREFIX_LIST = Arrays.asList(".", "_");

  private final transient FileSystem fs;
  private final TypedProperties props;

  public DFSPathSelector(TypedProperties props, Configuration hadoopConf) {
    DataSourceUtils.checkRequiredProperties(props, Arrays.asList(Config.ROOT_INPUT_PATH_PROP));
    this.props = props;
    this.fs = FSUtils.getFs(props.getString(Config.ROOT_INPUT_PATH_PROP), hadoopConf);
  }

  public Pair<Optional<String>, String> getNextFilePathsAndMaxModificationTime(
      Optional<String> lastCheckpointStr, long sourceLimit) {

    try {
      // obtain all eligible files under root folder.
      List<FileStatus> eligibleFiles = new ArrayList<>();
      RemoteIterator<LocatedFileStatus> fitr = fs.listFiles(
          new Path(props.getString(Config.ROOT_INPUT_PATH_PROP)), true);
      while (fitr.hasNext()) {
        LocatedFileStatus fileStatus = fitr.next();
        if (fileStatus.isDirectory() || IGNORE_FILEPREFIX_LIST.stream()
                .anyMatch(pfx -> fileStatus.getPath().getName().startsWith(pfx))) {
          continue;
        }
        eligibleFiles.add(fileStatus);
      }
      // sort them by modification time.
      eligibleFiles.sort(Comparator.comparingLong(FileStatus::getModificationTime));

      // Filter based on checkpoint & input size, if needed
      long currentBytes = 0;
      long maxModificationTime = Long.MIN_VALUE;
      List<FileStatus> filteredFiles = new ArrayList<>();
      for (FileStatus f : eligibleFiles) {
        if (lastCheckpointStr.isPresent() && f.getModificationTime() <= Long.valueOf(
            lastCheckpointStr.get())) {
          // skip processed files
          continue;
        }

        if (currentBytes + f.getLen() >= sourceLimit) {
          // we have enough data, we are done
          break;
        }

        maxModificationTime = f.getModificationTime();
        currentBytes += f.getLen();
        filteredFiles.add(f);
      }

      // no data to read
      if (filteredFiles.size() == 0) {
        return new ImmutablePair<>(Optional.empty(),
                lastCheckpointStr.orElseGet(() -> String.valueOf(Long.MIN_VALUE)));
      }

      // read the files out.
      String pathStr = filteredFiles.stream().map(f -> f.getPath().toString())
          .collect(Collectors.joining(","));

      return new ImmutablePair<>(
          Optional.ofNullable(pathStr),
          String.valueOf(maxModificationTime));
    } catch (IOException ioe) {
      throw new HoodieIOException(
          "Unable to read from source from checkpoint: " + lastCheckpointStr, ioe);
    }
  }
}
