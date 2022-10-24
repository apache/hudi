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

package org.apache.hudi.utilities.sources.helpers.gcs;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.hudi.DataSourceReadOptions.BEGIN_INSTANTTIME;
import static org.apache.hudi.DataSourceReadOptions.END_INSTANTTIME;
import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE;
import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL;
import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL;

/**
 * Uses the start and end instants of a DeltaStreamer Source to help construct the right kind
 * of query for subsequent requests.
 */
public class QueryInfo {

  private final String queryType;
  private final String startInstant;
  private final String endInstant;

  private static final Logger LOG = LogManager.getLogger(QueryInfo.class);

  public QueryInfo(String queryType, String startInstant, String endInstant) {
    this.queryType = queryType;
    this.startInstant = startInstant;
    this.endInstant = endInstant;
  }

  public Dataset<Row> initializeSourceForFilenames(String srcPath, SparkSession sparkSession) {
    if (isIncremental()) {
      return incrementalQuery(sparkSession).load(srcPath);
    }

    // Issue a snapshot query.
    return snapshotQuery(sparkSession).load(srcPath)
            .filter(String.format("%s > '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, getStartInstant()));
  }

  public boolean areStartAndEndInstantsEqual() {
    return getStartInstant().equals(getEndInstant());
  }

  private DataFrameReader snapshotQuery(SparkSession sparkSession) {
    return sparkSession.read().format("org.apache.hudi")
            .option(QUERY_TYPE().key(), QUERY_TYPE_SNAPSHOT_OPT_VAL());
  }

  private DataFrameReader incrementalQuery(SparkSession sparkSession) {
    return sparkSession.read().format("org.apache.hudi")
            .option(QUERY_TYPE().key(), QUERY_TYPE_INCREMENTAL_OPT_VAL())
            .option(BEGIN_INSTANTTIME().key(), getStartInstant())
            .option(END_INSTANTTIME().key(), getEndInstant());
  }

  public boolean isIncremental() {
    return QUERY_TYPE_INCREMENTAL_OPT_VAL().equals(queryType);
  }

  public String getStartInstant() {
    return startInstant;
  }

  public String getEndInstant() {
    return endInstant;
  }

  public void logDetails() {
    LOG.debug("queryType: " + queryType + ", startInstant: " + startInstant + ", endInstant: " + endInstant);
  }

}
