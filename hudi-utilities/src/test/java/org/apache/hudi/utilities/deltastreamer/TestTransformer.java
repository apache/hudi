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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestTransformer extends HoodieDeltaStreamerTestBase {

  @Test
  public void testMultipleTransformersWithIdentifiers() throws Exception {
    // Configure 3 transformers of same type. 2nd transformer has no suffix
    String[] arr = new String [] {
        "1:" + TimestampTransformer.class.getName(),
        "2:" + TimestampTransformer.class.getName(),
        "3:" + TimestampTransformer.class.getName()};
    List<String> transformerClassNames = Arrays.asList(arr);

    // Create source using TRIP_SCHEMA
    boolean useSchemaProvider = true;
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesDfs" + testNum;
    int parquetRecordsCount = 10;
    prepareParquetDFSFiles(parquetRecordsCount, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null);
    prepareParquetDFSSource(useSchemaProvider, true, "source.avsc", "source.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "");
    String tableBasePath = basePath + "/testMultipleTransformersWithIdentifiers" + testNum;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(
        HoodieDeltaStreamerTestBase.TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, ParquetDFSSource.class.getName(),
            transformerClassNames, PROPS_FILENAME_TEST_PARQUET, false,
            useSchemaProvider, 100000, false, null, null, "timestamp", null), jsc);

    // Set properties for multi transformer
    // timestamp.transformer.increment is a common config and varies between the transformers
    // timestamp.transformer.multiplier is also a common config but doesn't change between transformers
    Properties properties = ((HoodieStreamer.StreamSyncService) deltaStreamer.getIngestionService()).getProps();
    // timestamp value initially is set to 0
    // timestamp = 0 * 2 + 10; (transformation 1)
    // timestamp = 10 * 2 + 20 = 40 (transformation 2)
    // timestamp = 40 * 2 + 30 = 110 (transformation 3)
    properties.setProperty("timestamp.transformer.increment.1", "10");
    properties.setProperty("timestamp.transformer.increment.3", "30");
    properties.setProperty("timestamp.transformer.increment", "20");
    properties.setProperty("timestamp.transformer.multiplier", "2");
    properties.setProperty("transformer.suffix", ".1,.2,.3");
    deltaStreamer.sync();

    assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);
    assertEquals(0, sqlContext.read().format("org.apache.hudi").load(tableBasePath).where("timestamp != 110").count());
    testNum++;
    deltaStreamer.shutdownGracefully();
  }

  /**
   * Performs transformation on `timestamp` field.
   */
  public static class TimestampTransformer implements Transformer {

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                              TypedProperties properties) {
      String[] suffixes = ((String) properties.get("transformer.suffix")).split(",");
      for (String suffix : suffixes) {
        // verify no configs with suffix are in properties
        properties.keySet().forEach(k -> assertFalse(((String) k).endsWith(suffix)));
      }
      int multiplier = Integer.parseInt((String) properties.get("timestamp.transformer.multiplier"));
      int increment = Integer.parseInt((String) properties.get("timestamp.transformer.increment"));
      return rowDataset.withColumn("timestamp", functions.col("timestamp").multiply(multiplier).plus(increment));
    }
  }
}
