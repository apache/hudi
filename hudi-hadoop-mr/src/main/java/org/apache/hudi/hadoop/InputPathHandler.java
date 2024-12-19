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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.InvalidTableException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getTableMetaClientForBasePathUnchecked;

/**
 * InputPathHandler takes in a set of input paths and incremental tables list. Then, classifies the
 * input paths to incremental, snapshot paths and non-hoodie paths. This is then accessed later to
 * mutate the JobConf before processing incremental mode queries and snapshot queries.
 *
 * Note: We are adding jobConf of a mapreduce or spark job. The properties in the jobConf are two
 * type: session properties and table properties from metastore. While session property is common
 * for all the tables in a query the table properties are unique per table so there is no need to
 * check if it belongs to the table for which the path handler is now instantiated. The jobConf has
 * all table properties such as name, last modification time and so on which are unique to a table.
 * This class is written in such a way that it can handle multiple tables and properties unique to
 * a table but for table level property such check is not required.
 */
public class InputPathHandler {

  public static final Logger LOG = LoggerFactory.getLogger(InputPathHandler.class);

  private final Configuration conf;
  // tableName to metadata mapping for all Hoodie tables(both incremental & snapshot)
  private final Map<String, HoodieTableMetaClient> tableMetaClientMap;
  private final Map<HoodieTableMetaClient, List<Path>> groupedIncrementalPaths;
  private final List<Path> snapshotPaths;
  private final List<Path> nonHoodieInputPaths;
  private final boolean isIncrementalUseDatabase;

  public InputPathHandler(Configuration conf, Path[] inputPaths, List<String> incrementalTables) throws IOException {
    this.conf = conf;
    tableMetaClientMap = new HashMap<>();
    snapshotPaths = new ArrayList<>();
    nonHoodieInputPaths = new ArrayList<>();
    groupedIncrementalPaths = new HashMap<>();
    this.isIncrementalUseDatabase = HoodieHiveUtils.isIncrementalUseDatabase(conf);
    parseInputPaths(inputPaths, incrementalTables);
  }

  /**
   * Takes in the original InputPaths and classifies each of them into incremental, snapshot and
   * non-hoodie InputPaths. The logic is as follows:
   * 1. Check if an inputPath starts with the same basePath as any of the metadata basePaths we know
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
      String inputPathStr = inputPath.toString();
      for (HoodieTableMetaClient metaClient : tableMetaClientMap.values()) {
        String basePathStr = metaClient.getBasePath().toString();
        if (inputPathStr.equals(basePathStr) || inputPathStr.startsWith(basePathStr + "/")) {
          // We already know the base path for this inputPath.
          basePathKnown = true;
          // Check if this is for a snapshot query
          tagAsIncrementalOrSnapshot(inputPath, metaClient, incrementalTables);
          break;
        }
      }
      if (!basePathKnown) {
        // This path is for a table that we don't know about yet.
        HoodieTableMetaClient metaClient;
        try {
          metaClient = getTableMetaClientForBasePathUnchecked(conf, inputPath);
          tableMetaClientMap.put(getIncrementalTable(metaClient), metaClient);
          tagAsIncrementalOrSnapshot(inputPath, metaClient, incrementalTables);
        } catch (TableNotFoundException | InvalidTableException e) {
          // This is a non Hoodie inputPath
          LOG.info("Handling a non-hoodie path " + inputPath);
          nonHoodieInputPaths.add(inputPath);
        }
      }
    }
  }

  private void tagAsIncrementalOrSnapshot(Path inputPath, HoodieTableMetaClient metaClient, List<String> incrementalTables) {
    if (!incrementalTables.contains(getIncrementalTable(metaClient))) {
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

  private String getIncrementalTable(HoodieTableMetaClient metaClient) {
    String databaseName = metaClient.getTableConfig().getDatabaseName();
    String tableName = metaClient.getTableConfig().getTableName();
    return isIncrementalUseDatabase && !StringUtils.isNullOrEmpty(databaseName)
            ? databaseName + "." + tableName : tableName;
  }
}
