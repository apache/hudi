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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class DFSPathSelector {

  protected static volatile Logger log = LogManager.getLogger(DFSPathSelector.class);

  /**
   * Configs supported.
   */
  public static class Config {

    public static final String ROOT_INPUT_PATH_PROP = "hoodie.deltastreamer.source.dfs.root";
  }

  protected static final List<String> IGNORE_FILEPREFIX_LIST = Arrays.asList(".", "_");

  protected final transient FileSystem fs;
  protected final TypedProperties props;

  public DFSPathSelector(TypedProperties props, Configuration hadoopConf) {
    DataSourceUtils.checkRequiredProperties(props, Arrays.asList(Config.ROOT_INPUT_PATH_PROP));
    this.props = props;
    this.fs = FSUtils.getFs(props.getString(Config.ROOT_INPUT_PATH_PROP), hadoopConf);
  }

  public Pair<Option<String>, String> getNextFilePathsAndMaxModificationTime(Option<String> lastCheckpointStr,
      long sourceLimit) {

    try {
      // obtain all eligible files under root folder.
      log.info("Root path => " + props.getString(Config.ROOT_INPUT_PATH_PROP) + " source limit => " + sourceLimit);
      List<FileStatus> eligibleFiles = new ArrayList<>();
      RemoteIterator<LocatedFileStatus> fitr =
          fs.listFiles(new Path(props.getString(Config.ROOT_INPUT_PATH_PROP)), true);
      while (fitr.hasNext()) {
        LocatedFileStatus fileStatus = fitr.next();
        if (fileStatus.isDirectory()
            || IGNORE_FILEPREFIX_LIST.stream().anyMatch(pfx -> fileStatus.getPath().getName().startsWith(pfx))) {
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
        if (lastCheckpointStr.isPresent() && f.getModificationTime() <= Long.valueOf(lastCheckpointStr.get()).longValue()) {
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
        return new ImmutablePair<>(Option.empty(), lastCheckpointStr.orElseGet(() -> String.valueOf(Long.MIN_VALUE)));
      }

      // read the files out.
      String pathStr = filteredFiles.stream().map(f -> f.getPath().toString()).collect(Collectors.joining(","));

      return new ImmutablePair<>(Option.ofNullable(pathStr), String.valueOf(maxModificationTime));
    } catch (IOException ioe) {
      throw new HoodieIOException("Unable to read from source from checkpoint: " + lastCheckpointStr, ioe);
    }
  }
}
