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

package org.apache.hudi.integ.testsuite.utils;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.utilities.schema.RowBasedSchemaProvider;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class TestUtils {

  /**
   * Create a RDD of generic records for testing purposes.
   */
  public static JavaRDD<GenericRecord> makeRDD(JavaSparkContext jsc, int numRecords) {
    return jsc.parallelize(generateGenericRecords(numRecords));
  }

  /**
   * Generate generic records.
   */
  public static List<GenericRecord> generateGenericRecords(int numRecords) {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    return dataGenerator.generateGenericRecords(numRecords);
  }

  public static void createAvroFiles(JavaSparkContext jsc, SparkSession sparkSession, String basePath, int numFiles,
      int numRecordsPerFile) {
    Schema schema = HoodieTestDataGenerator.AVRO_SCHEMA;
    for (int i = 0; i < numFiles; i++) {
      JavaRDD<GenericRecord> rdd = makeRDD(jsc, numRecordsPerFile);
      AvroConversionUtils.createDataFrame(rdd.rdd(), schema.toString(), sparkSession).write()
          .format("avro").option("recordName", RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME)
          .option("recordNamespace", RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE).save(basePath + "/" + i);
    }
  }

  public static Schema getSchema() {
    return HoodieTestDataGenerator.AVRO_SCHEMA;
  }

}
