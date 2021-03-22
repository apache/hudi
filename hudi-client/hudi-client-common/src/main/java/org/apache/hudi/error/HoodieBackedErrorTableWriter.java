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

package org.apache.hudi.error;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.HoodieErrorTableConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Writer implementation backed by an internal hudi table. Error records are saved within an internal COW table
 * called Error table.
 */
public abstract class HoodieBackedErrorTableWriter<T extends HoodieRecordPayload, I, K, O>  implements Serializable {

  protected HoodieWriteConfig errorTableWriteConfig;
  protected HoodieWriteConfig datasetWriteConfig;
  protected String tableName;

  protected HoodieTableMetaClient metaClient;
  protected SerializableConfiguration hadoopConf;
  protected final transient HoodieEngineContext engineContext;
  protected String basePath;

  protected HoodieBackedErrorTableWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    this.datasetWriteConfig = writeConfig;
    this.engineContext = engineContext;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);

    if (writeConfig.enableErrorTable()) {
      this.tableName = writeConfig.getTableName() + HoodieErrorTableConfig.ERROR_TABLE_NAME_SUFFIX;
      this.basePath = getErrorTableBasePath(writeConfig);
      this.errorTableWriteConfig = createErrorDataWriteConfig(writeConfig);
      initialize(engineContext, metaClient);
      this.metaClient = HoodieTableMetaClient.builder()
                            .setConf(hadoopConf)
                            .setBasePath(datasetWriteConfig.getBasePath()).build();
    }
  }

  /**
   * Create a {@code HoodieWriteConfig} to use for the Error Table.
   *
   * @param writeConfig {@code HoodieWriteConfig} of the main dataset writer
   */
  private HoodieWriteConfig createErrorDataWriteConfig(HoodieWriteConfig writeConfig) {
    int parallelism = writeConfig.getErrorTableInsertParallelism();
    // Create the write config for the metadata table by borrowing options from the main write config.
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
                                            .withEmbeddedTimelineServerEnabled(false)
                                            .withPath(basePath)
                                            .withSchema(HoodieErrorTableConfig.ERROR_TABLE_SCHEMA)
                                            .forTable(getErrorTableName(writeConfig))
                                            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                                                                      .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
                                                                      .retainCommits(writeConfig.getErrorTableCleanerCommitsRetained())
                                                                      .archiveCommitsWith(writeConfig.getErrorTableMinCommitsToKeep(),
                                                                          writeConfig.getMetadataMaxCommitsToKeep()).build())
                                            .withParallelism(parallelism, parallelism);

    return builder.build();
  }

  public HoodieWriteConfig getWriteConfig() {
    return errorTableWriteConfig;
  }

  /**
   *  Init if hudi error table not exit.
   * @param datasetMetaClient
   * @throws IOException
   */
  protected void bootstrapErrorTable(HoodieTableMetaClient datasetMetaClient) throws IOException {

    if (datasetMetaClient == null) {
      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE)
          .setTableName(tableName)
          .setArchiveLogFolder("archived")
          .setPayloadClassName(OverwriteWithLatestAvroPayload.class.getName())
          .setBaseFileFormat(HoodieFileFormat.PARQUET.toString())
          .initTable(new Configuration(hadoopConf.get()), errorTableWriteConfig.getBasePath());
    } else {
      boolean exists = datasetMetaClient.getFs().exists(new Path(errorTableWriteConfig.getBasePath(), HoodieTableMetaClient.METAFOLDER_NAME));
      if (!exists) {
        HoodieTableMetaClient.withPropertyBuilder()
            .setTableType(HoodieTableType.COPY_ON_WRITE)
            .setTableName(tableName)
            .setArchiveLogFolder("archived")
            .setPayloadClassName(OverwriteWithLatestAvroPayload.class.getName())
            .setBaseFileFormat(HoodieFileFormat.PARQUET.toString())
            .initTable(new Configuration(hadoopConf.get()), errorTableWriteConfig.getBasePath());
      }
    }
  }

  /**
   *  Build hudi error table base path.
   * @param writeConfig
   * @return
   */
  private String getErrorTableBasePath(HoodieWriteConfig writeConfig) {

    if (StringUtils.isNullOrEmpty(writeConfig.getErrorTableBasePath())) {
      return writeConfig.getBasePath() + Path.SEPARATOR +  HoodieTableMetaClient.METAFOLDER_NAME + Path.SEPARATOR + "errors";
    }
    return writeConfig.getErrorTableBasePath();
  }

  /**
   *  Build hudi error table name.
   * @param writeConfig
   * @return
   */
  private String getErrorTableName(HoodieWriteConfig writeConfig) {

    return StringUtils.isNullOrEmpty(writeConfig.getErrorTableName())
               ? writeConfig.getTableName() + HoodieErrorTableConfig.ERROR_TABLE_NAME_SUFFIX : writeConfig.getErrorTableName();
  }

  protected abstract void initialize(HoodieEngineContext engineContext, HoodieTableMetaClient datasetMetaClient);

  public abstract void commit(O writeStatuses, HoodieTable<T, I, K, O> hoodieTable);
}



