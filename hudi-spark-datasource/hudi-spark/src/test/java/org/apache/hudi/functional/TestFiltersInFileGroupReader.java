/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional;

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;


/**
 * Ensure that parquet filters are not being pushed down when they shouldn't be
 */
@Tag("functional")
//TODO: Since we got rid of non-fg reader, we might need to increase the validation
public class TestFiltersInFileGroupReader extends TestBootstrapReadBase {

  @Test
  public void testFiltersInFileFormat() {
    this.bootstrapType = "mixed";
    this.dashPartitions = true;
    this.tableType = HoodieTableType.MERGE_ON_READ;
    this.nPartitions = 2;
    this.nInserts = 100;
    this.nUpdates = 20;
    sparkSession.conf().set(SQLConf.PARQUET_RECORD_FILTER_ENABLED().key(), "true");
    setupDirs();

    //do bootstrap
    Map<String, String> options = setBootstrapOptions();
    Dataset<Row> bootstrapDf = sparkSession.emptyDataFrame();
    bootstrapDf.write().format("hudi")
        .options(options)
        .mode(SaveMode.Overwrite)
        .save(bootstrapTargetPath);
    runComparison();


    options = basicOptions();
    options.put(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key(), "true");
    options.put(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), "true");

    doUpdate(options, "001");
    runComparison();

    doInsert(options, "002");
    runComparison();

    doDelete(options, "003");
    runComparison();
  }

  protected void runComparison() {
    compareDf(createDf(hudiBasePath, true), createDf(hudiBasePath, false));
    compareDf(createDf(bootstrapTargetPath, true), createDf(bootstrapTargetPath, false));
    compareDf(createDf2(hudiBasePath, true), createDf2(hudiBasePath, false));
    compareDf(createDf2(bootstrapTargetPath, true), createDf2(bootstrapTargetPath, false));
  }

  protected Dataset<Row> createDf(String tableBasePath, Boolean mergeUseRecordPositions) {
    //The chances of a uuid containing 00 with the 8-4-4-4-12 format is around 90%
    //for bootstrap, _hoodie_record_key is in the skeleton while begin_lat is in the data
    //We have a record key filter so that tests MORs filter pushdown with position based merging
    return sparkSession.read().format("hudi")
        .option(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key(), mergeUseRecordPositions)
        .load(tableBasePath)
        .drop("city_to_state")
        .where("begin_lat > 0.5 and _hoodie_record_key LIKE '%00%'");
  }

  protected Dataset<Row> createDf2(String tableBasePath, Boolean mergeUseRecordPositions) {
    //The chances of a uuid containing 00 with the 8-4-4-4-12 format is around 90%
    //for bootstrap, _hoodie_record_key is in the skeleton while begin_lat is in the data
    //We have a record key filter so that tests MORs filter pushdown with position based merging
    return sparkSession.read().format("hudi")
        .option(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key(), mergeUseRecordPositions)
        .load(tableBasePath)
        .drop("city_to_state")
        .where("begin_lat > 0.5 or _hoodie_record_key LIKE '%00%'");
  }
}
