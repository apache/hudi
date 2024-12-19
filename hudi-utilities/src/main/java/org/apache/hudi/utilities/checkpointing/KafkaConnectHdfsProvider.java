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

package org.apache.hudi.utilities.checkpointing;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generate checkpoint from Kafka-Connect-HDFS managed data set.
 * Documentation: https://docs.confluent.io/current/connect/kafka-connect-hdfs/index.html
 */
public class KafkaConnectHdfsProvider extends InitialCheckPointProvider {
  private static final String FILENAME_SEPARATOR = "[\\+\\.]";

  public KafkaConnectHdfsProvider(TypedProperties props) {
    super(props);
  }

  @Override
  public void init(Configuration config) throws HoodieException {
    try {
      this.fs = FileSystem.get(config);
    } catch (IOException e) {
      throw new HoodieException("KafkaConnectHdfsProvider initialization failed");
    }
  }

  /**
   * PathFilter for Kafka-Connect-HDFS.
   * Directory format: /partition1=xxx/partition2=xxx
   * File format: topic+partition+lowerOffset+upperOffset.file
   */
  public static class KafkaConnectPathFilter implements PathFilter {
    private static final Pattern DIRECTORY_PATTERN = Pattern.compile(".*=.*");
    private static final Pattern PATTERN =
        Pattern.compile("[a-zA-Z0-9\\._\\-]+\\+\\d+\\+\\d+\\+\\d+(.\\w+)?");

    @Override
    public boolean accept(final Path path) {
      final String filename = path.getName();
      final Matcher matcher = PATTERN.matcher(filename);
      return matcher.matches();
    }

    public boolean acceptDir(final Path path) {
      final String dirName = path.getName();
      final Matcher matcher = DIRECTORY_PATTERN.matcher(dirName);
      return matcher.matches();
    }
  }

  /**
   * Convert map contains max offset of each partition to string.
   *
   * @param topic      Topic name
   * @param checkpoint Map with partition as key and max offset as value
   * @return Checkpoint string
   */
  private static String buildCheckpointStr(final String topic,
                                           final HashMap<Integer, Integer> checkpoint) {
    final StringBuilder checkpointStr = new StringBuilder();
    checkpointStr.append(topic);
    for (int i = 0; i < checkpoint.size(); ++i) {
      checkpointStr.append(",").append(i).append(":").append(checkpoint.get(i));
    }
    return checkpointStr.toString();
  }

  /**
   * List file status recursively.
   *
   * @param curPath Current Path
   * @param filter  PathFilter
   * @return All file status match kafka connect naming convention
   * @throws IOException
   */
  private ArrayList<FileStatus> listAllFileStatus(Path curPath,
                                                  KafkaConnectPathFilter filter) throws IOException {
    ArrayList<FileStatus> allFileStatus = new ArrayList<>();
    FileStatus[] fileStatus = this.fs.listStatus(curPath);
    for (FileStatus status : fileStatus) {
      if (status.isDirectory() && filter.acceptDir(status.getPath())) {
        allFileStatus.addAll(listAllFileStatus(status.getPath(), filter));
      } else {
        if (filter.accept(status.getPath())) {
          allFileStatus.add(status);
        }
      }
    }
    return allFileStatus;
  }

  @Override
  public String getCheckpoint() throws HoodieException {
    final KafkaConnectPathFilter filter = new KafkaConnectPathFilter();
    ArrayList<FileStatus> fileStatus;
    try {
      fileStatus = listAllFileStatus(this.path, filter);
    } catch (IOException e) {
      throw new HoodieException(e.toString());
    }
    if (fileStatus.size() == 0) {
      throw new HoodieException("No valid Kafka Connect Hdfs file found under:" + this.path.getName());
    }
    final String topic = fileStatus.get(0).getPath().getName().split(FILENAME_SEPARATOR)[0];
    int maxPartition = -1;
    final HashMap<Integer, Integer> checkpointMap = new HashMap<>();
    for (final FileStatus status : fileStatus) {
      final String filename = status.getPath().getName();
      final String[] groups = filename.split(FILENAME_SEPARATOR);
      final int partition = Integer.parseInt(groups[1]);
      final int offsetUpper = Integer.parseInt(groups[3]);
      maxPartition = Math.max(maxPartition, partition);
      if (checkpointMap.containsKey(partition)) {
        checkpointMap.put(partition, Math.max(checkpointMap.get(partition), offsetUpper));
      } else {
        checkpointMap.put(partition, offsetUpper);
      }
    }
    if (checkpointMap.size() != maxPartition + 1) {
      throw new HoodieException("Missing partition from the file scan, "
          + "max partition found(start from 0): "
          + maxPartition
          + " total partitions number appear in "
          + this.path.getName()
          + " is: "
          + checkpointMap.size()
          + " total partitions number expected: "
          + (maxPartition + 1));
    }
    return buildCheckpointStr(topic, checkpointMap);
  }
}
