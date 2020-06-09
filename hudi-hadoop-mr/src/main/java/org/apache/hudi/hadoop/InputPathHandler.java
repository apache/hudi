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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.InvalidTableException;
import org.apache.hudi.exception.TableNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getTableMetaClientForBasePath;

/**
 * InputPathHandler takes in a set of input paths and incremental tables list. Then, classifies the
 * input paths to incremental, snapshot paths and non-hoodie paths. This is then accessed later to
 * mutate the JobConf before processing incremental mode queries and snapshot queries.
 */
public class InputPathHandler {

  public static final Logger LOG = LogManager.getLogger(InputPathHandler.class);

  private final Configuration conf;
  // tablename to metadata mapping for all Hoodie tables(both incremental & snapshot)
  private final Map<String, HoodieTableMetaClient> tableMetaClientMap;
  private final Map<HoodieTableMetaClient, List<Path>> groupedIncrementalPaths;
  private final List<Path> snapshotPaths;
  private final List<Path> nonHoodieInputPaths;

  InputPathHandler(Configuration conf, Path[] inputPaths, List<String> incrementalTables) throws IOException {
    this.conf = conf;
    tableMetaClientMap = new HashMap<>();
    snapshotPaths = new ArrayList<>();
    nonHoodieInputPaths = new ArrayList<>();
    groupedIncrementalPaths = new HashMap<>();
    parseInputPaths(inputPaths, incrementalTables);
  }

  /**
   * Takes in the original InputPaths and classifies each of them into incremental, snapshot and
   * non-hoodie InputPaths. The logic is as follows:
   *
   * 1. Check if an inputPath starts with the same basepath as any of the metadata basepaths we know
   *    1a. If yes, this belongs to a Hoodie table that we already know about. Simply classify this
   *        as incremental or snapshot - We can get the table name of this inputPath from the
   *        metadata. Then based on the list of incrementalTables, we can classify this inputPath.
   *    1b. If no, this could be a new Hoodie Table we haven't seen yet or a non-Hoodie Input Path.
   *        Try creating the HoodieTableMetadataClient.
   *            - If it succeeds, further classify as incremental on snapshot as described in step
   *              1a above.
   *            - If DatasetNotFoundException/InvalidDatasetException is caught, this is a
   *              non-Hoodie inputPath
   * @param inputPaths - InputPaths from the original jobConf that was passed to HoodieInputFormat
   * @param incrementalTables - List of all incremental tables extracted from the config
   * `hoodie.&lt;table-name&gt;.consume.mode=INCREMENTAL`
   * @throws IOException
   */
  private void parseInputPaths(Path[] inputPaths, List<String> incrementalTables)
      throws IOException {
    for (Path inputPath : inputPaths) {
      boolean basePathKnown = false;
      for (HoodieTableMetaClient metaClient : tableMetaClientMap.values()) {
        if (inputPath.toString().contains(metaClient.getBasePath())) {
          // We already know the base path for this inputPath.
          basePathKnown = true;
          // Check if this is for a snapshot query
          String tableName = metaClient.getTableConfig().getTableName();
          tagAsIncrementalOrSnapshot(inputPath, tableName, metaClient, incrementalTables);
          break;
        }
      }
      if (!basePathKnown) {
        // This path is for a table that we dont know about yet.
        HoodieTableMetaClient metaClient;
        try {
          metaClient = getTableMetaClientForBasePath(inputPath.getFileSystem(conf), inputPath);
          String tableName = metaClient.getTableConfig().getTableName();
          tableMetaClientMap.put(tableName, metaClient);
          tagAsIncrementalOrSnapshot(inputPath, tableName, metaClient, incrementalTables);
        } catch (TableNotFoundException | InvalidTableException e) {
          // This is a non Hoodie inputPath
          LOG.info("Handling a non-hoodie path " + inputPath);
          nonHoodieInputPaths.add(inputPath);
        }
      }
    }
  }

  private void tagAsIncrementalOrSnapshot(Path inputPath, String tableName,
      HoodieTableMetaClient metaClient, List<String> incrementalTables) {
    if (!incrementalTables.contains(tableName)) {
      snapshotPaths.add(inputPath);
    } else {
      // Group incremental Paths belonging to same table.
      if (!groupedIncrementalPaths.containsKey(metaClient)) {
        groupedIncrementalPaths.put(metaClient, new ArrayList<>());
      }
      groupedIncrementalPaths.get(metaClient).add(inputPath);
    }
  }

  public Map<HoodieTableMetaClient, List<Path>> getGroupedIncrementalPaths() {
    return groupedIncrementalPaths;
  }

  public Map<String, HoodieTableMetaClient> getTableMetaClientMap() {
    return tableMetaClientMap;
  }

  public List<Path> getSnapshotPaths() {
    return snapshotPaths;
  }

  public List<Path> getNonHoodieInputPaths() {
    return nonHoodieInputPaths;
  }
}
