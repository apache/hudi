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

package org.apache.hudi.snowflake.sync;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.HoodieSyncClient;

import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.StructType;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_PROPERTIES_FILE;
import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT;

/*
 * Snowflake Hudi client to perform all the table operations on Snowflake Cloud Platform.
 */
public class HoodieSnowflakeSyncClient extends HoodieSyncClient {
  private static final Logger LOG = LogManager.getLogger(HoodieSnowflakeSyncClient.class);
  private final SnowflakeSyncConfig config;
  private transient Session snowflakeSession;

  public HoodieSnowflakeSyncClient(final SnowflakeSyncConfig config) {
    super(config);
    this.config = config;
    this.createSnowflakeConnection();
  }

  private void createSnowflakeConnection() {
    if (snowflakeSession == null) {
      try {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests.
        snowflakeSession = Session.builder().configFile(config.getString(SNOWFLAKE_SYNC_PROPERTIES_FILE)).create();
        LOG.info("Successfully established Snowflake connection.");
      } catch (Exception e) {
        throw new HoodieSnowflakeSyncException("Cannot create snowflake connection ", e);
      }
    }
  }

  public void createStage(String stageName, String basePath, String storageIntegration) {
    try {
      String query = "CREATE OR REPLACE STAGE " + stageName
          + " url='" + basePath.replace("gs://", "gcs://")
          + "' STORAGE_INTEGRATION = " + storageIntegration;
      snowflakeSession.sql(query).show();
      LOG.info("Manifest External table created.");
    } catch (Exception e) {
      throw new HoodieSnowflakeSyncException("Manifest External table was not created ", e);
    }
  }

  public void createManifestTable(String stageName, String tableName) {
    try {
      String query = "CREATE OR REPLACE EXTERNAL TABLE " + tableName + " ("
          + "    filename VARCHAR AS split_part(VALUE:c1, '/', -1)"
          + "  )"
          + "WITH LOCATION = @" + stageName + "/.hoodie/manifest/"
          + "  FILE_FORMAT = (TYPE = CSV)"
          + "  AUTO_REFRESH = False";
      snowflakeSession.sql(query).show();
      LOG.info("Manifest External table created.");
    } catch (Exception e) {
      throw new HoodieSnowflakeSyncException("Manifest External table was not created ", e);
    }
  }

  private void createCustomFileFormat(String fileFormatName) {
    try {
      String query = "CREATE OR REPLACE FILE FORMAT " + fileFormatName + " TYPE = '" + config.getString(SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT) + "';";
      snowflakeSession.sql(query).show();
    } catch (Exception e) {
      throw new HoodieSnowflakeSyncException("Custom file format was not created. ", e);
    }
  }

  private List<String> generateSchemaWithoutPartitionColumns(String stageName, String fileFormatName) {
    try {
      String query = "SELECT"
          + "  generate_column_description(array_agg(object_construct(*)), 'external_table') as columns"
          + " FROM"
          + "  table("
          + "    infer_schema("
          + "      location => '@" + stageName + "',"
          + "      file_format => '" + fileFormatName + "'"
          + "    )"
          + ")";
      Optional<Row> row = snowflakeSession.sql(query).first();
      String columns = row.get().get(0).toString();
      if (columns.isEmpty()) {
        throw new HoodieSnowflakeSyncException("Unable to infer the schema with the given data files.");
      }
      return Arrays.asList(columns.split(",", -1));
    } catch (Exception e) {
      throw new HoodieSnowflakeSyncException("Unable to infer the schema with the given data files. ", e);
    }
  }

  public void createVersionsTable(String stageName, String tableName, String partitionFields, String partitionExtractExpr) {
    try {
      String fileFormatName = "my_custom_file_format";
      createCustomFileFormat(fileFormatName);
      List<String> inferredColumns = new ArrayList<String>();
      inferredColumns.addAll(generateSchemaWithoutPartitionColumns(stageName, fileFormatName));
      String query = "";
      if (partitionFields.isEmpty()) {
        query = "CREATE OR REPLACE EXTERNAL TABLE " + tableName + "("
            + String.join(", ", inferredColumns) + ") ";
      } else {
        // Configuring partitioning options for partitioned table.
        inferredColumns.addAll(Arrays.asList(partitionExtractExpr.split(",")));
        query = "CREATE OR REPLACE EXTERNAL TABLE " + tableName + "("
            + String.join(", ", inferredColumns)
            + ") PARTITION BY (" + partitionFields + ") ";
      }
      query += " WITH LOCATION = @" + stageName
          + "  FILE_FORMAT = (TYPE = " + config.getString(SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT) + ")"
          + "  PATTERN = '.*[.]" + config.getString(SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT).toLowerCase() + "'"
          + "  AUTO_REFRESH = false";
      snowflakeSession.sql(query).show();
      LOG.info("External versions table created.");
    } catch (Exception e) {
      throw new HoodieSnowflakeSyncException("External versions table was not created ", e);
    }
  }

  public void createSnapshotView(String viewName, String versionsTableName, String manifestTableName) {
    try {
      String query = "CREATE OR REPLACE VIEW " + viewName + " AS"
          + " SELECT * FROM " + versionsTableName
          + " WHERE \"_hoodie_file_name\" IN (SELECT filename FROM " + manifestTableName + ")";
      snowflakeSession.sql(query).show();
      LOG.info("View created successfully");
    } catch (Exception e) {
      throw new HoodieSnowflakeSyncException("View was not created ", e);
    }
  }

  @Override
  public void addPartitionsToTable(final String tableName, final List<String> partitionsToAdd) {
    try {
      String query = "ALTER EXTERNAL TABLE " + tableName + " REFRESH";
      snowflakeSession.sql(query).show();
      LOG.info("Table metadata refreshed successfully");
    } catch (Exception e) {
      throw new HoodieSnowflakeSyncException("Table metadata not refreshed ", e);
    }
  }

  @Override
  public boolean tableExists(String tableName) {
    try {
      StructType schema = snowflakeSession.table(tableName).schema();
      return true;
    } catch (Exception e) {
      LOG.info("Table doesn't exist " + tableName);
      return false;
    }
  }

  @Override
  public Option<String> getLastCommitTimeSynced(final String tableName) {
    // snowflake doesn't support tblproperties, so do nothing.
    throw new UnsupportedOperationException("Not support getLastCommitTimeSynced yet.");
  }

  @Override
  public void updateLastCommitTimeSynced(final String tableName) {
    // snowflake doesn't support tblproperties, so do nothing.
    throw new UnsupportedOperationException("No support for updateLastCommitTimeSynced yet.");
  }

  @Override
  public Option<String> getLastReplicatedTime(String tableName) {
    // snowflake doesn't support tblproperties, so do nothing.
    throw new UnsupportedOperationException("Not support getLastReplicatedTime yet.");
  }

  @Override
  public void updateLastReplicatedTimeStamp(String tableName, String timeStamp) {
    // snowflake doesn't support tblproperties, so do nothing.
    throw new UnsupportedOperationException("No support for updateLastReplicatedTimeStamp yet.");
  }

  @Override
  public void deleteLastReplicatedTimeStamp(String tableName) {
    // snowflake doesn't support tblproperties, so do nothing.
    throw new UnsupportedOperationException("No support for deleteLastReplicatedTimeStamp yet.");
  }

  @Override
  public void updatePartitionsToTable(final String tableName, final List<String> changedPartitions) {
    try {
      String query = "ALTER EXTERNAL TABLE " + tableName + " REFRESH";
      snowflakeSession.sql(query).show();
      LOG.info("Table metadata refreshed successfully");
    } catch (Exception e) {
      throw new HoodieSnowflakeSyncException("Table metadata not refreshed ", e);
    }
  }

  @Override
  public void dropPartitions(String tableName, List<String> partitionsToDrop) {
    try {
      String query = "ALTER EXTERNAL TABLE " + tableName + " REFRESH";
      snowflakeSession.sql(query).show();
      LOG.info("Table metadata refreshed successfully");
    } catch (Exception e) {
      throw new HoodieSnowflakeSyncException("Table metadata not refreshed ", e);
    }
  }

  @Override
  public void close() {
    snowflakeSession.close();
  }
}
