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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.utilities.config.DFSPathSelectorConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.io.storage.HoodieIOFactory.getIOFactory;

public class DFSPathSelector implements Serializable {

  protected static volatile Logger log = LoggerFactory.getLogger(DFSPathSelector.class);

  /**
   * Configs supported.
   */
  public static class Config {

    @Deprecated
    public static final String ROOT_INPUT_PATH_PROP = DFSPathSelectorConfig.ROOT_INPUT_PATH.key();
    @Deprecated
    public static final String SOURCE_INPUT_SELECTOR = DFSPathSelectorConfig.SOURCE_INPUT_SELECTOR.key();
  }

  protected static final List<String> IGNORE_FILEPREFIX_LIST = Arrays.asList(".", "_");

  protected final transient HoodieStorage storage;
  protected final TypedProperties props;

  public DFSPathSelector(TypedProperties props, Configuration hadoopConf) {
    checkRequiredConfigProperties(
        props, Collections.singletonList(DFSPathSelectorConfig.ROOT_INPUT_PATH));
    this.props = props;
    this.storage = getIOFactory(HadoopFSUtils.getStorageConf(hadoopConf)).getStorage(
        getStringWithAltKeys(props, DFSPathSelectorConfig.ROOT_INPUT_PATH));
  }

  /**
   * Factory method for creating custom DFSPathSelector. Default selector
   * to use is {@link DFSPathSelector}
   */
  public static DFSPathSelector createSourceSelector(TypedProperties props,
                                                     Configuration conf) {
    String sourceSelectorClass = getStringWithAltKeys(
        props, DFSPathSelectorConfig.SOURCE_INPUT_SELECTOR, DFSPathSelector.class.getName());
    try {
      DFSPathSelector selector = (DFSPathSelector) ReflectionUtils.loadClass(sourceSelectorClass,
          new Class<?>[] {TypedProperties.class, Configuration.class},
          props, conf);

      log.info("Using path selector " + selector.getClass().getName());
      return selector;
    } catch (Exception e) {
      throw new HoodieException("Could not load source selector class " + sourceSelectorClass, e);
    }
  }

  /**
   * Get the list of files changed since last checkpoint.
   *
   * @param sparkContext JavaSparkContext to help parallelize certain operations
   * @param lastCheckpointStr the last checkpoint time string, empty if first run
   * @param sourceLimit       max bytes to read each time
   * @return the list of files concatenated and their latest modified time
   */
  public Pair<Option<String>, String> getNextFilePathsAndMaxModificationTime(JavaSparkContext sparkContext, Option<String> lastCheckpointStr,
                                                                             long sourceLimit) {
    return getNextFilePathsAndMaxModificationTime(lastCheckpointStr, sourceLimit);
  }

  /**
   * Get the list of files changed since last checkpoint.
   *
   * @param lastCheckpointStr the last checkpoint time string, empty if first run
   * @param sourceLimit       max bytes to read each time
   * @return the list of files concatenated and their latest modified time
   */
  @Deprecated
  public Pair<Option<String>, String> getNextFilePathsAndMaxModificationTime(Option<String> lastCheckpointStr,
                                                                             long sourceLimit) {
    try {
      // obtain all eligible files under root folder.
      log.info("Root path => " + getStringWithAltKeys(props, DFSPathSelectorConfig.ROOT_INPUT_PATH)
          + " source limit => " + sourceLimit);
      long lastCheckpointTime = lastCheckpointStr.map(Long::parseLong).orElse(Long.MIN_VALUE);
      List<StoragePathInfo> eligibleFiles = listEligibleFiles(
          storage, new StoragePath(getStringWithAltKeys(props,
              DFSPathSelectorConfig.ROOT_INPUT_PATH)),
          lastCheckpointTime);
      // sort them by modification time.
      eligibleFiles.sort(Comparator.comparingLong(StoragePathInfo::getModificationTime));
      // Filter based on checkpoint & input size, if needed
      long currentBytes = 0;
      long newCheckpointTime = lastCheckpointTime;
      List<StoragePathInfo> filteredFiles = new ArrayList<>();
      for (StoragePathInfo f : eligibleFiles) {
        if (currentBytes + f.getLength() >= sourceLimit
            && f.getModificationTime() > newCheckpointTime) {
          // we have enough data, we are done
          // Also, we've read up to a file with a newer modification time
          // so that some files with the same modification time won't be skipped in next read
          break;
        }

        newCheckpointTime = f.getModificationTime();
        currentBytes += f.getLength();
        filteredFiles.add(f);
      }

      // no data to read
      if (filteredFiles.isEmpty()) {
        return new ImmutablePair<>(Option.empty(), String.valueOf(newCheckpointTime));
      }

      // read the files out.
      String pathStr =
          filteredFiles.stream().map(f -> f.getPath().toString())
              .collect(Collectors.joining(","));

      return new ImmutablePair<>(Option.ofNullable(pathStr), String.valueOf(newCheckpointTime));
    } catch (IOException ioe) {
      throw new HoodieIOException("Unable to read from source from checkpoint: " + lastCheckpointStr, ioe);
    }
  }

  /**
   * List files recursively, filter out illegible files/directories while doing so.
   */
  protected List<StoragePathInfo> listEligibleFiles(HoodieStorage storage, StoragePath path,
                                                    long lastCheckpointTime) throws IOException {
    // skip files/dirs whose names start with (_, ., etc)
    List<StoragePathInfo> pathInfoList = storage.listDirectEntries(path, file ->
        IGNORE_FILEPREFIX_LIST.stream().noneMatch(pfx -> file.getName().startsWith(pfx)));
    List<StoragePathInfo> res = new ArrayList<>();
    for (StoragePathInfo pathInfo : pathInfoList) {
      if (pathInfo.isDirectory()) {
        res.addAll(listEligibleFiles(storage, pathInfo.getPath(), lastCheckpointTime));
      } else if (pathInfo.getModificationTime() > lastCheckpointTime && pathInfo.getLength() > 0) {
        res.add(pathInfo);
      }
    }
    return res;
  }
}
