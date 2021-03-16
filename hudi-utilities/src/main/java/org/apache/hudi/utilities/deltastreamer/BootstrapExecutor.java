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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

/**
 * Performs bootstrap from a non-hudi source.
 */
public class BootstrapExecutor  implements Serializable {

  private static final Logger LOG = LogManager.getLogger(BootstrapExecutor.class);

  /**
   *  Config.
   */
  private final HoodieDeltaStreamer.Config cfg;

  /**
   * Schema provider that supplies the command for reading the input and writing out the target table.
   */
  private transient SchemaProvider schemaProvider;

  /**
   * Spark context.
   */
  private transient JavaSparkContext jssc;

  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  private final TypedProperties props;

  /**
   * Hadoop Configuration.
   */
  private final Configuration configuration;

  /**
   * Bootstrap Configuration.
   */
  private final HoodieWriteConfig bootstrapConfig;

  /**
   * FileSystem instance.
   */
  private transient FileSystem fs;

  private String bootstrapBasePath;

  /**
   * Bootstrap Executor.
   * @param cfg DeltaStreamer Config
   * @param jssc Java Spark Context
   * @param fs File System
   * @param properties Bootstrap Writer Properties
   * @throws IOException
   */
  public BootstrapExecutor(HoodieDeltaStreamer.Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                           TypedProperties properties) throws IOException {
    this.cfg = cfg;
    this.jssc = jssc;
    this.fs = fs;
    this.configuration = conf;
    this.props = properties;

    ValidationUtils.checkArgument(properties.containsKey(HoodieTableConfig.HOODIE_BOOTSTRAP_BASE_PATH),
        HoodieTableConfig.HOODIE_BOOTSTRAP_BASE_PATH + " must be specified.");
    this.bootstrapBasePath = properties.getString(HoodieTableConfig.HOODIE_BOOTSTRAP_BASE_PATH);

    // Add more defaults if full bootstrap requested
    this.props.putIfAbsent(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY(),
        DataSourceWriteOptions.DEFAULT_PAYLOAD_OPT_VAL());
    this.schemaProvider = UtilHelpers.createSchemaProvider(cfg.schemaProviderClassName, props, jssc);
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder().withPath(cfg.targetBasePath)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().withInlineCompaction(false).build())
            .forTable(cfg.targetTableName)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .withAutoCommit(true)
            .withProps(props);

    if (null != schemaProvider && null != schemaProvider.getTargetSchema()) {
      builder = builder.withSchema(schemaProvider.getTargetSchema().toString());
    }
    this.bootstrapConfig = builder.build();
    LOG.info("Created bootstrap executor with configs : " + bootstrapConfig.getProps());
  }

  /**
   * Executes Bootstrap.
   */
  public void execute() throws IOException {
    initializeTable();
    SparkRDDWriteClient bootstrapClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jssc), bootstrapConfig);

    try {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      checkpointCommitMetadata.put(HoodieDeltaStreamer.CHECKPOINT_KEY, cfg.checkpoint);
      if (cfg.checkpoint != null) {
        checkpointCommitMetadata.put(HoodieDeltaStreamer.CHECKPOINT_RESET_KEY, cfg.checkpoint);
      }
      bootstrapClient.bootstrap(Option.of(checkpointCommitMetadata));
      syncHive();
    } finally {
      bootstrapClient.close();
    }
  }

  /**
   * Sync to Hive.
   */
  private void syncHive() {
    if (cfg.enableHiveSync) {
      HiveSyncConfig hiveSyncConfig = DataSourceUtils.buildHiveSyncConfig(props, cfg.targetBasePath, cfg.baseFileFormat);
      LOG.info("Syncing target hoodie table with hive table(" + hiveSyncConfig.tableName + "). Hive metastore URL :"
          + hiveSyncConfig.jdbcUrl + ", basePath :" + cfg.targetBasePath);
      new HiveSyncTool(hiveSyncConfig, new HiveConf(configuration, HiveConf.class), fs).syncHoodieTable();
    }
  }

  private void initializeTable() throws IOException {
    if (fs.exists(new Path(cfg.targetBasePath))) {
      throw new HoodieException("target base path already exists at " + cfg.targetBasePath
          + ". Cannot bootstrap data on top of an existing table");
    }
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(cfg.tableType)
        .setTableName(cfg.targetTableName)
        .setArchiveLogFolder("archived")
        .setPayloadClassName(cfg.payloadClassName)
        .setBaseFileFormat(cfg.baseFileFormat)
        .setBootstrapIndexClass(cfg.bootstrapIndexClass)
        .setBootstrapBasePath(bootstrapBasePath)
        .initTable(new Configuration(jssc.hadoopConfiguration()), cfg.targetBasePath);
  }

  public HoodieWriteConfig getBootstrapConfig() {
    return bootstrapConfig;
  }
}