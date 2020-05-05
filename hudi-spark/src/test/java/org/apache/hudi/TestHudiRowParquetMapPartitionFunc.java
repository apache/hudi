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

package org.apache.hudi;

import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarnessJunit5;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

import static org.apache.hudi.common.HoodieTestDataGenerator.AVRO_SCHEMA_1;

public class TestHudiRowParquetMapPartitionFunc extends HoodieCommonTestHarnessJunit5 {

  private static final Logger LOG = LoggerFactory.getLogger(TestHudiRowParquetMapPartitionFunc.class);
  private JavaSparkContext jsc;
  private SQLContext sqlContext;
  private HoodieTestDataGenerator dataGen;
  protected transient FileSystem fs;

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHudiParquetWriter");
    initPath();
    initTestDataGenerator();
    initFileSystem();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupSparkContexts();
    cleanupFileSystem();
  }

  @Test
  public void simpleTest() {
    String commitTime = "000";
    try {
      List<GenericRecord> records = dataGen.generateInsertsGenRec(commitTime, 10);
      JavaRDD<GenericRecord> javaRDD = jsc.parallelize(records).coalesce(1);

      Dataset<Row> rowDataset = AvroConversionUtils.createDataFrame(javaRDD.rdd(), AVRO_SCHEMA_1.toString(), sqlContext.sparkSession());
      SerializableConfiguration serConfig = new SerializableConfiguration(jsc.sc().hadoopConfiguration());

      List<Attribute> attributes = JavaConversions.asJavaCollection(rowDataset.schema().toAttributes()).stream().map(Attribute::toAttribute).collect(Collectors.toList());
      ExpressionEncoder encoder = RowEncoder.apply(rowDataset.schema())
          .resolveAndBind(JavaConverters.asScalaBufferConverter(attributes).asScala().toSeq(), SimpleAnalyzer$.MODULE$);
      Dataset<Boolean> result = rowDataset.mapPartitions(new HudiRowParquetMapPartitionFunc(basePath, encoder, serConfig, "SNAPPY"), Encoders.BOOLEAN());
      result.collect();
    } catch (Exception e) {
      LOG.error("Test complete with failure :::::::::: " + e.getMessage() + " ... " + e.getCause());
      e.printStackTrace();
    }
  }

  protected void initSparkContexts(String appName) {
    // Initialize a local spark env
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest(appName));
    jsc.setLogLevel("ERROR");

    // SQLContext stuff
    sqlContext = new SQLContext(jsc);
  }

  protected void initTestDataGenerator() {
    dataGen = new HoodieTestDataGenerator();
  }

  /**
   * Initializes a file system with the hadoop configuration of Spark context.
   */
  protected void initFileSystem() {
    if (jsc == null) {
      throw new IllegalStateException("The Spark context has not been initialized.");
    }

    initFileSystemWithConfiguration(jsc.hadoopConfiguration());
  }

  private void initFileSystemWithConfiguration(Configuration configuration) {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    fs = FSUtils.getFs(basePath, configuration);
    if (fs instanceof LocalFileSystem) {
      LocalFileSystem lfs = (LocalFileSystem) fs;
      // With LocalFileSystem, with checksum disabled, fs.open() returns an inputStream which is FSInputStream
      // This causes ClassCastExceptions in LogRecordScanner (and potentially other places) calling fs.open
      // So, for the tests, we enforce checksum verification to circumvent the problem
      lfs.setVerifyChecksum(true);
    }
  }

  protected void cleanupSparkContexts() {
    if (sqlContext != null) {
      LOG.info("Clearing sql context cache of spark-session used in previous test-case");
      sqlContext.clearCache();
      sqlContext = null;
    }

    if (jsc != null) {
      LOG.info("Closing spark context used in previous test-case");
      jsc.close();
      jsc.stop();
      jsc = null;
    }
  }

  /**
   * Cleanups file system.
   */
  protected void cleanupFileSystem() throws IOException {
    if (fs != null) {
      LOG.warn("Closing file-system instance used in previous test-run");
      fs.close();
    }
  }

}
