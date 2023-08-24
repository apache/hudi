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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.sources.helpers.QueryInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Abstract splitter responsible for managing the snapshot load query operations.
 */
public abstract class SnapshotLoadQuerySplitter {

  /**
   * Configuration properties for the splitter.
   */
  protected final TypedProperties properties;

  /**
   * Configurations for the SnapshotLoadQuerySplitter.
   */
  public static class Config {
    /**
     * Property for the snapshot load query splitter class name.
     */
    public static final String SNAPSHOT_LOAD_QUERY_SPLITTER_CLASS_NAME = "hoodie.deltastreamer.snapshotload.query.splitter.class.name";
  }

  /**
   * Constructor initializing the properties.
   *
   * @param properties Configuration properties for the splitter.
   */
  public SnapshotLoadQuerySplitter(TypedProperties properties) {
    this.properties = properties;
  }

  /**
   * Abstract method to retrieve the next checkpoint.
   *
   * @param df The dataset to process.
   * @param beginCheckpointStr The starting checkpoint string.
   * @return The next checkpoint as an Option.
   */
  public abstract Option<String> getNextCheckpoint(Dataset<Row> df, String beginCheckpointStr);

  /**
   * Retrieves the next checkpoint based on query information.
   *
   * @param df The dataset to process.
   * @param queryInfo The query information object.
   * @return Updated query information with the next checkpoint, in case of empty checkpoint,
   * returning endPoint same as queryInfo.getEndInstant().
   */
  public QueryInfo getNextCheckpoint(Dataset<Row> df, QueryInfo queryInfo) {
    return getNextCheckpoint(df, queryInfo.getStartInstant())
        .map(checkpoint -> queryInfo.withUpdatedEndInstant(checkpoint))
        .orElse(queryInfo);
  }
}
