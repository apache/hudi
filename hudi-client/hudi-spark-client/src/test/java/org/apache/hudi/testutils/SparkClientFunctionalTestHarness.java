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

package org.apache.hudi.testutils;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.providers.HoodieMetaClientProvider;
import org.apache.hudi.testutils.providers.HoodieWriteClientProvider;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;

public class SparkClientFunctionalTestHarness implements SparkProvider, HoodieMetaClientProvider, HoodieWriteClientProvider {

  private static transient SparkSession spark;
  private static transient SQLContext sqlContext;
  private static transient JavaSparkContext jsc;
  private static transient HoodieSparkEngineContext context;

  /**
   * An indicator of the initialization status.
   */
  protected boolean initialized = false;
  @TempDir
  protected java.nio.file.Path tempDir;

  public String basePath() {
    return tempDir.toAbsolutePath().toUri().toString();
  }

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

  public HoodieTableMetaClient getHoodieMetaClient(Configuration hadoopConf, String basePath) throws IOException {
    return getHoodieMetaClient(hadoopConf, basePath, new Properties());
  }

  @Override
  public HoodieTableMetaClient getHoodieMetaClient(Configuration hadoopConf, String basePath, Properties props) throws IOException {
    props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(props)
        .build();
    return HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, basePath, props);
  }

  @Override
  public SparkRDDWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) throws IOException {
    return new SparkRDDWriteClient(context(), cfg);
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

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        jsc.close();
        jsc = null;
        spark.stop();
        spark = null;
      }));
    }
  }

  @AfterAll
  public static synchronized void cleanUpAfterAll() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
    if (jsc != null) {
      jsc.close();
      jsc = null;
    }
    sqlContext = null;
    context = null;
  }
}
