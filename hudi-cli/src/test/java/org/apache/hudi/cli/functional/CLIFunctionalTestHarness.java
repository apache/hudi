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

package org.apache.hudi.cli.functional;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.providers.SparkProvider;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.shell.Bootstrap;
import org.springframework.shell.core.JLineShellComponent;

import java.nio.file.Paths;

public class CLIFunctionalTestHarness implements SparkProvider {

  protected static int timelineServicePort =
      FileSystemViewStorageConfig.REMOTE_PORT_NUM.defaultValue();
  protected static transient TimelineService timelineService;
  protected static transient HoodieSparkEngineContext context;
  private static transient SparkSession spark;
  private static transient SQLContext sqlContext;
  private static transient JavaSparkContext jsc;
  private static transient JLineShellComponent shell;
  /**
   * An indicator of the initialization status.
   */
  protected boolean initialized = false;
  @TempDir
  protected java.nio.file.Path tempDir;

  public String basePath() {
    return tempDir.toAbsolutePath().toString();
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

  public JLineShellComponent shell() {
    return shell;
  }

  public String tableName() {
    return tableName("_test_table");
  }

  public String tableName(String suffix) {
    return getClass().getSimpleName() + suffix;
  }

  public String tablePath(String tableName) {
    return Paths.get(basePath(), tableName).toString();
  }

  public Configuration hadoopConf() {
    return jsc().hadoopConfiguration();
  }

  @BeforeEach
  public synchronized void runBeforeEach() {
    initialized = spark != null && shell != null;
    if (!initialized) {
      SparkConf sparkConf = conf();
      SparkRDDWriteClient.registerClasses(sparkConf);
      HoodieReadClient.addHoodieSupport(sparkConf);
      spark = SparkSession.builder().config(sparkConf).getOrCreate();
      sqlContext = spark.sqlContext();
      jsc = new JavaSparkContext(spark.sparkContext());
      context = new HoodieSparkEngineContext(jsc);
      shell = new Bootstrap().getJLineShellComponent();
      timelineService = HoodieClientTestUtils.initTimelineService(
          context, basePath(), incrementTimelineServicePortToUse());
      timelineServicePort = timelineService.getServerPort();
    }
  }

  @AfterAll
  public static synchronized void cleanUpAfterAll() {
    if (spark != null) {
      spark.close();
      spark = null;
    }
    if (shell != null) {
      shell.stop();
      shell = null;
    }
    if (timelineService != null) {
      timelineService.close();
    }
  }

  /**
   * Helper to prepare string for matching.
   *
   * @param str Input string.
   * @return pruned string with non word characters removed.
   */
  protected static String removeNonWordAndStripSpace(String str) {
    return str.replaceAll("[\\s]+", ",").replaceAll("[\\W]+", ",");
  }

  protected int incrementTimelineServicePortToUse() {
    // Increment the timeline service port for each individual test
    // to avoid port reuse causing failures
    timelineServicePort = (timelineServicePort + 1 - 1024) % (65536 - 1024) + 1024;
    return timelineServicePort;
  }
}
