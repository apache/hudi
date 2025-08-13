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

package org.apache.hudi.integ.testsuite;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.HoodieRepairTool;
import org.apache.hudi.utilities.IdentitySplitter;
import org.apache.hudi.utilities.UtilHelpers;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sample command
 *
 * TODO: [HUDI-8294]
 * ./bin/spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.4 --driver-memory 4g   --executor-memory 4g \
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer   --conf spark.sql.catalogImplementation=hive \
 * --class org.apache.hudi.integ.testsuite.SparkDSContinuousIngestTool \
 * ${HUDI_ROOT_DIR}/packaging/hudi-integ-test-bundle/target/hudi-integ-test-bundle-0.11.0-SNAPSHOT.jar \
 * --source-path file:${SOURCE_DIR}/spark_ds_continuous   --checkpoint-file-path /tmp/hudi/checkpoint  \
 * --base-path file:///tmp/hudi/tbl_path/   --props /tmp/hudi_props.out
 *
 * Contents of hudi.properties
 *
 * hoodie.insert.shuffle.parallelism=4
 * hoodie.upsert.shuffle.parallelism=4
 * hoodie.bulkinsert.shuffle.parallelism=4
 * hoodie.delete.shuffle.parallelism=4
 * hoodie.datasource.write.recordkey.field=VendorID
 * hoodie.datasource.write.partitionpath.field=date_col
 * hoodie.datasource.write.operation=upsert
 * hoodie.datasource.write.precombine.fields=tpep_pickup_datetime
 * hoodie.metadata.enable=false
 * hoodie.table.name=hudi_tbl
 */

public class SparkDataSourceContinuousIngestTool {

  private static final Logger LOG = LoggerFactory.getLogger(SparkDataSourceContinuousIngestTool.class);

  private final Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;
  private HoodieSparkEngineContext context;
  private SparkSession sparkSession;

  public SparkDataSourceContinuousIngestTool(JavaSparkContext jsc, Config cfg) {
    if (cfg.propsFilePath != null) {
      cfg.propsFilePath = HadoopFSUtils.addSchemeIfLocalPath(cfg.propsFilePath).toString();
    }
    this.context = new HoodieSparkEngineContext(jsc);
    this.sparkSession = SparkSession.builder().config(jsc.getConf()).getOrCreate();
    this.cfg = cfg;
    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    final JavaSparkContext jsc = UtilHelpers.buildSparkContext("spark-datasource-continuous-ingestion-tool", cfg.sparkMaster, cfg.sparkMemory);
    try {
      new SparkDataSourceContinuousIngestTool(jsc, cfg).run();
    } catch (Throwable throwable) {
      LOG.error("Fail to run Continuous Ingestion for spark datasource " + cfg.basePath, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      SparkDataSourceContinuousIngest sparkDataSourceContinuousIngest =
          new SparkDataSourceContinuousIngest(
              sparkSession, context.getStorageConf().unwrapAs(Configuration.class), new Path(cfg.sourcePath), cfg.sparkFormat,
              new Path(cfg.checkpointFilePath), new Path(cfg.basePath), getPropsAsMap(props),
              cfg.minSyncIntervalSeconds);
      sparkDataSourceContinuousIngest.startIngestion();
    } finally {
      sparkSession.stop();
      context.getJavaSparkContext().stop();
    }
  }

  private Map<String, String> getPropsAsMap(TypedProperties typedProperties) {
    Map<String, String> props = new HashMap<>();
    typedProperties.entrySet().forEach(entry -> props.put(entry.getKey().toString(), entry.getValue().toString()));
    return props;
  }

  /**
   * Reads config from the file system.
   *
   * @param jsc {@link JavaSparkContext} instance.
   * @param cfg {@link HoodieRepairTool.Config} instance.
   * @return the {@link TypedProperties} instance.
   */
  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--source-path", "-sp"}, description = "Source path for the parquet data to consume", required = true)
    public String sourcePath = null;
    @Parameter(names = {"--source-format", "-sf"}, description = "source data format", required = false)
    public String sparkFormat = "parquet";
    @Parameter(names = {"--checkpoint-file-path", "-cpf"}, description = "Checkpoint file path to store/fetch checkpointing info", required = true)
    public String checkpointFilePath = null;
    @Parameter(names = {"--base-path", "-bp"}, description = "Base path for the hudi table", required = true)
    public String basePath = null;
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";
    @Parameter(names = {"--min-sync-interval-seconds"},
        description = "the min sync interval of each sync in continuous mode")
    public Integer minSyncIntervalSeconds = 0;
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for table repair")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();
  }
}
