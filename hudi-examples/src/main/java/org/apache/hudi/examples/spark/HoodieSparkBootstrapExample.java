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

package org.apache.hudi.examples.spark;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.examples.common.HoodieExampleSparkUtils;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;



public class HoodieSparkBootstrapExample {

  private static String tableType = HoodieTableType.MERGE_ON_READ.name();


  public static void main(String[] args) throws Exception {
    if (args.length < 5) {
      System.err.println("Usage: HoodieWriteClientExample <tablePath> <tableName>");
      System.exit(1);
    }
    String recordKey = args[0];
    String tableName = args[1];
    String partitionPath = args[2];
    String preCombineField = args[3];
    String basePath = args[4];

    SparkConf sparkConf = HoodieExampleSparkUtils.defaultSparkConf("hoodie-client-example");

    SparkSession spark = SparkSession
            .builder()
            .appName("Java Spark SQL basic example")
            .config("spark.some.config.option", "some-value")
            .enableHiveSupport()
            .getOrCreate();

    Dataset df =  spark.emptyDataFrame();

    df.write().format("hudi").option(HoodieWriteConfig.TBL_NAME.key(), tableName)
            .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL())
            .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), recordKey)
            .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), partitionPath)
            .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), preCombineField)
            .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieFileFormat.ORC.name())
            .option(HoodieBootstrapConfig.BASE_PATH.key(), basePath)
            .option(HoodieBootstrapConfig.KEYGEN_CLASS_NAME.key(), NonpartitionedKeyGenerator.class.getCanonicalName())
            .mode(SaveMode.Overwrite).save("/hudi/"+tableName);

    df.count();
  }
}
