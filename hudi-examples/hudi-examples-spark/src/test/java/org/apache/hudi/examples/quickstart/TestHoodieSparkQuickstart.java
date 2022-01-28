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

package org.apache.hudi.examples.quickstart;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.examples.common.HoodieExampleDataGenerator;
import org.apache.hudi.testutils.providers.SparkProvider;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Paths;

@Disabled
public class TestHoodieSparkQuickstart implements SparkProvider {
  protected static transient HoodieSparkEngineContext context;

  private static transient SparkSession spark;
  private static transient SQLContext sqlContext;
  private static transient JavaSparkContext jsc;

  /**
   * An indicator of the initialization status.
   */
  protected boolean initialized = false;
  @TempDir
  protected java.nio.file.Path tempDir;

  private static final HoodieExampleDataGenerator<HoodieAvroPayload> DATA_GEN = new HoodieExampleDataGenerator<>();

  @Override
  public SparkSession spark() {
    return spark;
  }

  @Override
  public SQLContext sqlContext() {
    return sqlContext;
  }

  @Override
  public JavaSparkContext jsc() {
    return jsc;
  }

  @Override
  public HoodieSparkEngineContext context() {
    return context;
  }

  public String basePath() {
    return tempDir.toAbsolutePath().toString();
  }

  public String tablePath(String tableName) {
    return Paths.get(basePath(), tableName).toString();
  }

  @BeforeEach
  public synchronized void runBeforeEach() {
    initialized = spark != null;
    if (!initialized) {
      SparkConf sparkConf = conf();
      SparkRDDWriteClient.registerClasses(sparkConf);
      HoodieReadClient.addHoodieSupport(sparkConf);
      spark = SparkSession.builder().config(sparkConf).getOrCreate();
      sqlContext = spark.sqlContext();
      jsc = new JavaSparkContext(spark.sparkContext());
      context = new HoodieSparkEngineContext(jsc);
    }
  }

  @Test
  public void testHoodieSparkQuickstart() {
    String tableName = "spark_quick_start";
    String tablePath = tablePath(tableName);

    try {
      HoodieSparkQuickstart.insertData(spark, jsc, tablePath, tableName, DATA_GEN);
      HoodieSparkQuickstart.updateData(spark, jsc, tablePath, tableName, DATA_GEN);

      HoodieSparkQuickstart.queryData(spark, jsc, tablePath, tableName, DATA_GEN);
      HoodieSparkQuickstart.incrementalQuery(spark, tablePath, tableName);
      HoodieSparkQuickstart.pointInTimeQuery(spark, tablePath, tableName);

      HoodieSparkQuickstart.delete(spark, tablePath, tableName);
      HoodieSparkQuickstart.deleteByPartition(spark, tablePath, tableName);
    } finally {
      Utils.deleteRecursively(new File(tablePath));
    }
  }
}
