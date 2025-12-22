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

package org.apache.hudi.integ.testsuite.streaming;

import org.apache.hudi.exception.HoodieException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Saprk-submit to test spark streaming
 * 
 * Sample command.
 * ./bin/spark-submit --master local[2]  --driver-memory 1g  --executor-memory 1g \
 * --class org.apache.hudi.streaming.StructuredStreamingSinkUtil  PATH TO hudi-integ-test-bundle-0.13.0-SNAPSHOT.jar \
 * --spark-master local[2] \
 * --source-path /tmp/parquet_ny/ \
 * --target-path /tmp/hudi_streaming_kafka10/MERGE_ON_READ3/ \
 * --checkpoint-path /tmp/hudi_streaming_kafka10/checkpoint_mor3/ \
 * --table-type COPY_ON_WRITE \
 * --partition-field date_col \
 * --record-key-field tpep_pickup_datetime \
 * --pre-combine-field tpep_dropoff_datetime \
 * --table-name test_tbl
 *
 * Ensure "source-path" has parquet data.
 */
public class StructuredStreamingSinkUtil implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(StructuredStreamingSinkUtil.class);

  private transient JavaSparkContext jsc;
  private SparkSession sparkSession;
  private Config cfg;

  public StructuredStreamingSinkUtil(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.sparkSession = SparkSession.builder().config(jsc.getConf()).getOrCreate();
    this.cfg = cfg;
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--source-path", "-sp"}, description = "Source path to consume data from", required = true)
    public String sourcePath = null;

    @Parameter(names = {"--target-path", "-tp"}, description = "Target path of the table of interest.", required = true)
    public String targetPath = null;

    @Parameter(names = {"--table-type", "-ty"}, description = "Target path of the table of interest.", required = true)
    public String tableType = "COPY_ON_WRITE";

    @Parameter(names = {"--checkpoint-path", "-cp"}, description = "Checkppint path of the table of interest", required = true)
    public String checkpointPath = null;

    @Parameter(names = {"--partition-field", "-pp"}, description = "Partitioning field", required = true)
    public String partitionField = null;

    @Parameter(names = {"--record-key-field", "-rk"}, description = "record key field", required = true)
    public String recordKeyField = null;

    @Parameter(names = {"--pre-combine-field", "-pc"}, description = "Precombine field", required = true)
    public String preCombineField = null;

    @Parameter(names = {"--table-name", "-tn"}, description = "Table name", required = true)
    public String tableName = null;

    @Parameter(names = {"--disable-metadata", "-dmdt"}, description = "Disable metadata while querying", required = false)
    public Boolean disableMetadata = false;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = buildSparkConf("Spark-structured-streaming-test", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      StructuredStreamingSinkUtil streamingSinkUtil = new StructuredStreamingSinkUtil(jsc, cfg);
      streamingSinkUtil.run();
    } catch (Throwable throwable) {
      LOG.error("Fail to execute tpcds read benchmarks for " + cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      LOG.info(cfg.toString());
      StructuredStreamingSinkTestWriter.triggerStreaming(sparkSession, cfg.tableType, cfg.sourcePath, cfg.targetPath, cfg.checkpointPath,
          cfg.tableName, cfg.partitionField, cfg.recordKeyField, cfg.preCombineField);
      StructuredStreamingSinkTestWriter.waitUntilCondition(1000 * 60 * 10, 1000 * 30);
    } catch (Exception e) {
      throw new HoodieException("Unable to test spark structured writes to hudi " + cfg.targetPath, e);
    } finally {
      LOG.warn("Completing Spark Structured Streaming test");
    }
  }

  public static SparkConf buildSparkConf(String appName, String defaultMaster) {
    return buildSparkConf(appName, defaultMaster, new HashMap<>());
  }

  private static SparkConf buildSparkConf(String appName, String defaultMaster, Map<String, String> additionalConfigs) {
    final SparkConf sparkConf = new SparkConf().setAppName(appName);
    String master = sparkConf.get("spark.master", defaultMaster);
    sparkConf.setMaster(master);
    if (master.startsWith("yarn")) {
      sparkConf.set("spark.eventLog.overwrite", "true");
      sparkConf.set("spark.eventLog.enabled", "true");
    }
    sparkConf.set("spark.ui.port", "8090");
    sparkConf.setIfMissing("spark.driver.maxResultSize", "2g");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar");
    sparkConf.set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
    sparkConf.set("spark.hadoop.mapred.output.compress", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK");

    additionalConfigs.forEach(sparkConf::set);
    return sparkConf;
  }
}
