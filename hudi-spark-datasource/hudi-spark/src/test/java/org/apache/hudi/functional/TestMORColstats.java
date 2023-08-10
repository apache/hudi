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

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.RawTripTestPayload.recordToString;
import static org.apache.spark.sql.SaveMode.Append;
import static org.apache.spark.sql.SaveMode.Overwrite;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMORColstats extends HoodieSparkClientTestBase {

  @TempDir
  public java.nio.file.Path basePath;

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
  }

  @Test
  public void testBaseFileOnlyExclusion() throws IOException {
    Map<String, String> options = new HashMap<>();
    options.put(HoodieMetadataConfig.ENABLE.key(), "true");
    options.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    options.put(HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key(), "begin_lat,end_lat,begin_lon,end_lon");
    options.put(DataSourceReadOptions.ENABLE_DATA_SKIPPING().key(), "true");
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL());
    options.put(HoodieWriteConfig.TBL_NAME.key(), "testTable");
    options.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator");
    dataGen = new HoodieTestDataGenerator();
    Dataset<Row> inserts = makeInsertDf("000", 100);
    Dataset<Row> insertsBatch1 = inserts.where("begin_lat < 0.9");
    Dataset<Row> insertsBatch2 = inserts.where("begin_lat >= 0.9");
    insertsBatch1.write().format("hudi").options(options).mode(Overwrite).save(basePath.toString());
    Properties props = new Properties();
    props.putAll(options);
    metaClient = HoodieTestUtils.init(hadoopConf, basePath.toString(), HoodieTableType.MERGE_ON_READ, props);
    String firstTimestamp = metaClient.getActiveTimeline().lastInstant().get().getTimestamp();
    insertsBatch2.write().format("hudi").options(options).mode(Append).save(basePath.toString());
    corruptFilesNotTimestamp(firstTimestamp);
    assertEquals(0, sparkSession.read().format("hudi").options(options)
        .load(basePath.toString()).where("begin_lat < 0.9")
        .drop("city_to_state", "_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name")
        .except(insertsBatch1.drop("city_to_state")).count());
  }

  protected void corruptFilesNotTimestamp(String timestamp) throws IOException {
    try (Stream<Path> stream = Files.list(basePath)) {
      stream
          .filter(file -> !Files.isDirectory(file))
          .filter(file -> file.toString().contains(".parquet"))
          .filter(file -> !file.toString().contains(timestamp))
          .forEach(file -> corruptFile(file));
    }
  }


  protected static void corruptFile(Path path) {
    File fileToCorrupt = path.toFile();
    fileToCorrupt.delete();
    try {
      fileToCorrupt.createNewFile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }



  protected Dataset<Row> makeInsertDf(String instantTime, Integer n) {
    List<String> records = dataGen.generateInserts(instantTime, n).stream()
        .map(r -> recordToString(r).get()).collect(Collectors.toList());
    JavaRDD<String> rdd = jsc.parallelize(records);
    return sparkSession.read().json(rdd);
  }
}
