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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;


/**
 * Ensure that parquet filters are not being pushed down when they shouldn't be
 */
@Tag("functional")
public class TestFiltersInFileGroupReader extends TestBootstrapReadBase {

  @Disabled("issues/14222")
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testFiltersInFileFormat(boolean mergeUseRecordPositions) {
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
    runComparison(mergeUseRecordPositions);


    options = basicOptions();
    options.put(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key(), String.valueOf(mergeUseRecordPositions));
    options.put(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), String.valueOf(mergeUseRecordPositions));

    doUpdate(options, "001");
    runComparison(mergeUseRecordPositions);

    doInsert(options, "002");
    runComparison(mergeUseRecordPositions);

    doDelete(options, "003");
    runComparison(mergeUseRecordPositions);
  }

  protected void runComparison(boolean mergeUseRecordPositions) {
    compareDf(createDf(hudiBasePath, true, mergeUseRecordPositions), createDf(hudiBasePath, false, false));
    compareDf(createDf(bootstrapTargetPath, true, mergeUseRecordPositions), createDf(bootstrapTargetPath, false, false));
    compareDf(createDf2(hudiBasePath, true, mergeUseRecordPositions), createDf2(hudiBasePath, false, false));
    compareDf(createDf2(bootstrapTargetPath, true, mergeUseRecordPositions), createDf2(bootstrapTargetPath, false, false));
  }

  protected Dataset<Row> createDf(String tableBasePath, Boolean fgReaderEnabled, Boolean mergeUseRecordPositions) {
    //The chances of a uuid containing 00 with the 8-4-4-4-12 format is around 90%
    //for bootstrap, _hoodie_record_key is in the skeleton while begin_lat is in the data
    //We have a record key filter so that tests MORs filter pushdown with position based merging
    return sparkSession.read().format("hudi")
        .option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), fgReaderEnabled)
        .option(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key(), mergeUseRecordPositions)
        .load(tableBasePath)
        .drop("city_to_state")
        .where("begin_lat > 0.5 and _hoodie_record_key LIKE '%00%'");
  }

  protected Dataset<Row> createDf2(String tableBasePath, Boolean fgReaderEnabled, Boolean mergeUseRecordPositions) {
    //The chances of a uuid containing 00 with the 8-4-4-4-12 format is around 90%
    //for bootstrap, _hoodie_record_key is in the skeleton while begin_lat is in the data
    //We have a record key filter so that tests MORs filter pushdown with position based merging
    return sparkSession.read().format("hudi")
        .option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), fgReaderEnabled)
        .option(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key(), mergeUseRecordPositions)
        .load(tableBasePath)
        .drop("city_to_state")
        .where("begin_lat > 0.5 or _hoodie_record_key LIKE '%00%'");
  }
}
