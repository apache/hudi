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

package org.apache.hudi.sync.adb;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.PartitionValueExtractor;
import org.apache.hudi.hive.SchemaDifference;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractAdbSyncHoodieClient extends AbstractSyncHoodieClient {
  protected AdbSyncConfig adbSyncConfig;
  protected PartitionValueExtractor partitionValueExtractor;
  protected HoodieTimeline activeTimeline;

  public AbstractAdbSyncHoodieClient(AdbSyncConfig syncConfig, FileSystem fs) {
    super(syncConfig.basePath, syncConfig.assumeDatePartitioning,
        syncConfig.useFileListingFromMetadata, false, fs);
    this.adbSyncConfig = syncConfig;
    final String clazz = adbSyncConfig.partitionValueExtractorClass;
    try {
      this.partitionValueExtractor = (PartitionValueExtractor) Class.forName(clazz).newInstance();
    } catch (Exception e) {
      throw new HoodieException("Fail to init PartitionValueExtractor class " + clazz, e);
    }

    activeTimeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
  }

  public List<PartitionEvent> getPartitionEvents(Map<List<String>, String> tablePartitions,
                                                 List<String> partitionStoragePartitions) {
    Map<String, String> paths = new HashMap<>();

    for (Map.Entry<List<String>, String> entry : tablePartitions.entrySet()) {
      List<String> partitionValues = entry.getKey();
      String fullTablePartitionPath = entry.getValue();
      paths.put(String.join(", ", partitionValues), fullTablePartitionPath);
    }
    List<PartitionEvent> events = new ArrayList<>();
    for (String storagePartition : partitionStoragePartitions) {
      Path storagePartitionPath = FSUtils.getPartitionPath(adbSyncConfig.basePath, storagePartition);
      String fullStoragePartitionPath = Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      // Check if the partition values or if hdfs path is the same
      List<String> storagePartitionValues = partitionValueExtractor.extractPartitionValuesInPath(storagePartition);
      if (adbSyncConfig.useHiveStylePartitioning) {
        String partition = String.join("/", storagePartitionValues);
        storagePartitionPath = FSUtils.getPartitionPath(adbSyncConfig.basePath, partition);
        fullStoragePartitionPath = Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      }
      if (!storagePartitionValues.isEmpty()) {
        String storageValue = String.join(", ", storagePartitionValues);
        if (!paths.containsKey(storageValue)) {
          events.add(PartitionEvent.newPartitionAddEvent(storagePartition));
        } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
          events.add(PartitionEvent.newPartitionUpdateEvent(storagePartition));
        }
      }
    }
    return events;
  }

  public void close() {

  }

  public abstract Map<List<String>, String> scanTablePartitions(String tableName) throws Exception;

  public abstract void updateTableDefinition(String tableName, SchemaDifference schemaDiff) throws Exception;

  public abstract boolean databaseExists(String databaseName) throws Exception;

  public abstract void createDatabase(String databaseName) throws Exception;

  public abstract void dropTable(String tableName);

  protected String getDatabasePath() {
    String dbLocation = adbSyncConfig.dbLocation;
    Path dbLocationPath;
    if (StringUtils.isNullOrEmpty(dbLocation)) {
      if (new Path(adbSyncConfig.basePath).isRoot()) {
        dbLocationPath = new Path(adbSyncConfig.basePath);
      } else {
        dbLocationPath = new Path(adbSyncConfig.basePath).getParent();
      }
    } else {
      dbLocationPath = new Path(dbLocation);
    }
    return generateAbsolutePathStr(dbLocationPath);
  }

  protected String generateAbsolutePathStr(Path path) {
    String absolutePathStr = path.toString();
    if (path.toUri().getScheme() == null) {
      absolutePathStr = getDefaultFs() + absolutePathStr;
    }
    return absolutePathStr.endsWith("/") ? absolutePathStr : absolutePathStr + "/";
  }

  protected String getDefaultFs() {
    return fs.getConf().get("fs.defaultFS");
  }
}
