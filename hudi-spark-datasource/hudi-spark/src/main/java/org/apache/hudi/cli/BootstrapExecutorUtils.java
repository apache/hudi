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

package org.apache.hudi.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;

/**
 * Performs bootstrap from a non-hudi source.
 */
public class BootstrapExecutorUtils implements Serializable {

  private static final Logger LOG = LogManager.getLogger(BootstrapExecutorUtils.class);

  /**
   * Config.
   */
  private final Config cfg;

  /**
   * Spark context.
   */
  private final transient JavaSparkContext jssc;

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
  private final transient FileSystem fs;

  private final String bootstrapBasePath;

  public static final String CHECKPOINT_KEY = HoodieWriteConfig.DELTASTREAMER_CHECKPOINT_KEY;

  /**
   * Bootstrap Executor.
   *
   * @param cfg        DeltaStreamer Config
   * @param jssc       Java Spark Context
   * @param fs         File System
   * @param properties Bootstrap Writer Properties
   * @throws IOException
   */
  public BootstrapExecutorUtils(Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                                TypedProperties properties) throws IOException {
    this.cfg = cfg;
    this.jssc = jssc;
    this.fs = fs;
    this.configuration = conf;
    this.props = properties;

    ValidationUtils.checkArgument(properties.containsKey(HoodieTableConfig.BOOTSTRAP_BASE_PATH
            .key()),
        HoodieTableConfig.BOOTSTRAP_BASE_PATH.key() + " must be specified.");
    this.bootstrapBasePath = properties.getString(HoodieTableConfig.BOOTSTRAP_BASE_PATH.key());

    // Add more defaults if full bootstrap requested
    this.props.putIfAbsent(DataSourceWriteOptions.PAYLOAD_CLASS_NAME().key(),
        DataSourceWriteOptions.PAYLOAD_CLASS_NAME().defaultValue());
    /**
     * Schema provider that supplies the command for reading the input and writing out the target table.
     */
    SchemaProvider schemaProvider = createSchemaProvider(cfg.schemaProviderClass, props, jssc);
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder().withPath(cfg.basePath)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().withInlineCompaction(false).build())
            .forTable(cfg.tableName)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .withAutoCommit(true)
            .withProps(props);

    if (null != schemaProvider && null != schemaProvider.getTargetSchema()) {
      builder = builder.withSchema(schemaProvider.getTargetSchema().toString());
    }
    this.bootstrapConfig = builder.build();
    LOG.info("Created bootstrap executor with configs : " + bootstrapConfig.getProps());
  }

  public static SchemaProvider createSchemaProvider(String schemaProviderClass, TypedProperties cfg,
                                                    JavaSparkContext jssc) throws IOException {
    try {
      return StringUtils.isNullOrEmpty(schemaProviderClass) ? null
          : (SchemaProvider) ReflectionUtils.loadClass(schemaProviderClass, cfg, jssc);
    } catch (Throwable e) {
      throw new IOException("Could not load schema provider class " + schemaProviderClass, e);
    }
  }

  /**
   * Executes Bootstrap.
   */
  public void execute() throws IOException {
    initializeTable();

    try (SparkRDDWriteClient bootstrapClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jssc), bootstrapConfig)) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      checkpointCommitMetadata.put(CHECKPOINT_KEY, Config.checkpoint);
      bootstrapClient.bootstrap(Option.of(checkpointCommitMetadata));
      syncHive();
    }
  }

  /**
   * Sync to Hive.
   */
  private void syncHive() {
    if (cfg.enableHiveSync) {
      TypedProperties metaProps = new TypedProperties();
      metaProps.putAll(props);
      metaProps.put(HoodieSyncConfig.META_SYNC_BASE_PATH.key(), cfg.basePath);
      metaProps.put(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT.key(), cfg.baseFileFormat);
      if (props.getBoolean(HiveSyncConfig.HIVE_SYNC_BUCKET_SYNC.key(), HiveSyncConfig.HIVE_SYNC_BUCKET_SYNC.defaultValue())) {
        metaProps.put(HiveSyncConfig.HIVE_SYNC_BUCKET_SYNC_SPEC.key(), HiveSyncConfig.getBucketSpec(props.getString(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key()),
            props.getInteger(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key())));
      }

      new HiveSyncTool(metaProps, configuration, fs).syncHoodieTable();
    }
  }

  private void initializeTable() throws IOException {
    Path basePath = new Path(cfg.basePath);
    if (fs.exists(basePath)) {
      if (cfg.bootstrapOverwrite) {
        LOG.warn("Target base path already exists, overwrite it");
        fs.delete(basePath, true);
      } else {
        throw new HoodieException("target base path already exists at " + cfg.basePath
            + ". Cannot bootstrap data on top of an existing table");
      }
    }
    HoodieTableMetaClient.withPropertyBuilder()
        .fromProperties(props)
        .setTableType(cfg.tableType)
        .setTableName(cfg.tableName)
        .setArchiveLogFolder(ARCHIVELOG_FOLDER.defaultValue())
        .setPayloadClassName(cfg.payloadClass)
        .setBaseFileFormat(cfg.baseFileFormat)
        .setBootstrapIndexClass(cfg.bootstrapIndexClass)
        .setBootstrapBasePath(bootstrapBasePath)
        .initTable(new Configuration(jssc.hadoopConfiguration()), cfg.basePath);
  }

  public static class Config {
    private String tableName;
    private String tableType;

    private String basePath;

    private String baseFileFormat;
    private String bootstrapIndexClass;
    private String schemaProviderClass;
    private String payloadClass;
    private Boolean enableHiveSync;

    private Boolean bootstrapOverwrite;

    public static String checkpoint = null;

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public void setTableType(String tableType) {
      this.tableType = tableType;
    }

    public void setBasePath(String basePath) {
      this.basePath = basePath;
    }

    public void setBaseFileFormat(String baseFileFormat) {
      this.baseFileFormat = baseFileFormat;
    }

    public void setBootstrapIndexClass(String bootstrapIndexClass) {
      this.bootstrapIndexClass = bootstrapIndexClass;
    }

    public void setSchemaProviderClass(String schemaProviderClass) {
      this.schemaProviderClass = schemaProviderClass;
    }

    public void setPayloadClass(String payloadClass) {
      this.payloadClass = payloadClass;
    }

    public void setEnableHiveSync(Boolean enableHiveSync) {
      this.enableHiveSync = enableHiveSync;
    }

    public void setBootstrapOverwrite(Boolean bootstrapOverwrite) {
      this.bootstrapOverwrite = bootstrapOverwrite;
    }
  }
}
