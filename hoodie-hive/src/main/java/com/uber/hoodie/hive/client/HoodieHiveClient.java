/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.hive.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.uber.hoodie.hive.HoodieHiveConfiguration;
import com.uber.hoodie.hive.HoodieHiveDatasetException;
import com.uber.hoodie.hive.PartitionStrategy;
import com.uber.hoodie.hive.model.HoodieDatasetReference;
import com.uber.hoodie.hive.model.SchemaDifference;
import com.uber.hoodie.hive.model.StoragePartition;
import com.uber.hoodie.hive.model.TablePartition;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.schema.MessageType;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Client to access Hive
 */
public class HoodieHiveClient implements Closeable {
    private static Logger LOG = LoggerFactory.getLogger(HoodieHiveClient.class);
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    static {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Could not find " + driverName + " in classpath. ", e);
        }
    }

    private final HoodieHiveConfiguration configuration;
    private Connection connection;
    private HiveConf hiveConf;

    public HoodieHiveClient(HoodieHiveConfiguration configuration) {
        this.configuration = configuration;
        this.hiveConf = new HiveConf();
        this.hiveConf.addResource(configuration.getConfiguration());
        try {
            this.connection = getConnection();
        } catch (SQLException e) {
            throw new HoodieHiveDatasetException("Failed to connect to hive metastore ", e);
        }
    }

    /**
     * Scan all the partitions for the given {@link HoodieDatasetReference} with the given {@link PartitionStrategy}
     *
     * @param metadata
     * @return
     */
    public List<TablePartition> scanPartitions(HoodieDatasetReference metadata) {
        if (!checkTableExists(metadata)) {
            throw new IllegalArgumentException(
                "Failed to scan partitions as table " + metadata.getDatabaseTableName()
                    + " does not exist");
        }
        List<TablePartition> partitions = Lists.newArrayList();
        HiveMetaStoreClient client = null;
        try {
            client = new HiveMetaStoreClient(hiveConf);
            List<Partition> hivePartitions = client
                .listPartitions(metadata.getDatabaseName(), metadata.getTableName(), (short) -1);
            for (Partition partition : hivePartitions) {
                partitions.add(new TablePartition(metadata, partition));
            }
            return partitions;
        } catch (Exception e) {
            throw new HoodieHiveDatasetException("Failed to scan partitions for " + metadata, e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    /**
     * Check if table exists
     *
     * @param metadata
     * @return
     */
    public boolean checkTableExists(HoodieDatasetReference metadata) {
        ResultSet resultSet = null;
        try {
            Connection conn = getConnection();
            resultSet = conn.getMetaData()
                .getTables(null, metadata.getDatabaseName(), metadata.getTableName(), null);
            return resultSet.next();
        } catch (SQLException e) {
            throw new HoodieHiveDatasetException("Failed to check if table exists " + metadata, e);
        } finally {
            closeQuietly(resultSet, null);
        }
    }

    /**
     * Update the hive metastore pointed to by {@link HoodieDatasetReference} with the difference
     * in schema {@link SchemaDifference}
     *
     * @param metadata
     * @param hivePartitionFieldNames
     * @param newSchema               @return
     */
    public boolean updateTableDefinition(HoodieDatasetReference metadata,
        String[] hivePartitionFieldNames, MessageType newSchema) {
        try {
            String newSchemaStr = SchemaUtil.generateSchemaString(newSchema);
            // Cascade clause should not be present for non-partitioned tables
            String cascadeClause = hivePartitionFieldNames.length > 0 ? " cascade" : "";
            StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE ").append("`")
                .append(metadata.getDatabaseTableName()).append("`").append(" REPLACE COLUMNS(")
                .append(newSchemaStr).append(" )").append(cascadeClause);
            LOG.info("Creating table with " + sqlBuilder);
            return updateHiveSQL(sqlBuilder.toString());
        } catch (IOException e) {
            throw new HoodieHiveDatasetException("Failed to update table for " + metadata, e);
        }
    }

    /**
     * Execute a update in hive metastore with this SQL
     *
     * @param s SQL to execute
     * @return
     */
    public boolean updateHiveSQL(String s) {
        Statement stmt = null;
        try {
            Connection conn = getConnection();
            stmt = conn.createStatement();
            LOG.info("Executing SQL " + s);
            return stmt.execute(s);
        } catch (SQLException e) {
            throw new HoodieHiveDatasetException("Failed in executing SQL " + s, e);
        } finally {
            closeQuietly(null, stmt);
        }
    }

    /**
     * Get the table schema
     *
     * @param datasetReference
     * @return
     */
    public Map<String, String> getTableSchema(HoodieDatasetReference datasetReference) {
        if (!checkTableExists(datasetReference)) {
            throw new IllegalArgumentException(
                "Failed to get schema as table " + datasetReference.getDatabaseTableName()
                    + " does not exist");
        }
        Map<String, String> schema = Maps.newHashMap();
        ResultSet result = null;
        try {
            Connection connection = getConnection();
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            result = databaseMetaData.getColumns(null, datasetReference.getDatabaseName(),
                datasetReference.getTableName(), null);
            while (result.next()) {
                String columnName = result.getString(4);
                String columnType = result.getString(6);
                schema.put(columnName, columnType);
            }
            return schema;
        } catch (SQLException e) {
            throw new HoodieHiveDatasetException(
                "Failed to get table schema for " + datasetReference, e);
        } finally {
            closeQuietly(result, null);
        }
    }

    public void addPartitionsToTable(HoodieDatasetReference datasetReference,
        List<StoragePartition> partitionsToAdd, PartitionStrategy strategy) {
        if (partitionsToAdd.isEmpty()) {
            LOG.info("No partitions to add for " + datasetReference);
            return;
        }
        LOG.info("Adding partitions " + partitionsToAdd.size() + " to dataset " + datasetReference);
        String sql = constructAddPartitions(datasetReference, partitionsToAdd, strategy);
        updateHiveSQL(sql);
    }

    public void updatePartitionsToTable(HoodieDatasetReference datasetReference,
        List<StoragePartition> changedPartitions, PartitionStrategy partitionStrategy) {
        if (changedPartitions.isEmpty()) {
            LOG.info("No partitions to change for " + datasetReference);
            return;
        }
        LOG.info(
            "Changing partitions " + changedPartitions.size() + " on dataset " + datasetReference);
        List<String> sqls =
            constructChangePartitions(datasetReference, changedPartitions, partitionStrategy);
        for (String sql : sqls) {
            updateHiveSQL(sql);
        }
    }

    public void createTable(MessageType storageSchema, HoodieDatasetReference metadata,
        String[] partitionKeys, String inputFormatClass, String outputFormatClass) {
        try {
            String createSQLQuery = SchemaUtil
                .generateCreateDDL(storageSchema, metadata, partitionKeys, inputFormatClass,
                    outputFormatClass);
            LOG.info("Creating table with " + createSQLQuery);
            updateHiveSQL(createSQLQuery);
        } catch (IOException e) {
            throw new HoodieHiveDatasetException("Failed to create table for " + metadata, e);
        }
    }

    private static void closeQuietly(ResultSet resultSet, Statement stmt) {
        try {
            if (stmt != null)
                stmt.close();
            if (resultSet != null)
                resultSet.close();
        } catch (SQLException e) {
            LOG.error("Could not close the resultset opened ", e);
        }
    }

    private Connection getConnection() throws SQLException {
        int count = 0;
        int maxTries = 3;
        if (connection == null) {
            Configuration conf = configuration.getConfiguration();
            DataSource ds = getDatasource();
            LOG.info("Getting Hive Connection from Datasource " + ds);
            while (true) {
                try {
                    this.connection = ds.getConnection();
                    break;
                } catch (SQLException e) {
                    if (++count == maxTries)
                        throw e;
                }
            }
        }
        return connection;
    }

    private DataSource getDatasource() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(driverName);
        ds.setUrl(getHiveJdbcUrlWithDefaultDBName());
        ds.setUsername(configuration.getHiveUsername());
        ds.setPassword(configuration.getHivePassword());
        return ds;
    }

    public String getHiveJdbcUrlWithDefaultDBName() {
        String hiveJdbcUrl = configuration.getHiveJdbcUrl();
        String urlAppend = null;
        // If the hive url contains addition properties like ;transportMode=http;httpPath=hs2
        if (hiveJdbcUrl.contains(";")) {
            urlAppend = hiveJdbcUrl.substring(hiveJdbcUrl.indexOf(";"));
            hiveJdbcUrl = hiveJdbcUrl.substring(0, hiveJdbcUrl.indexOf(";"));
        }
        if (!hiveJdbcUrl.endsWith("/")) {
            hiveJdbcUrl = hiveJdbcUrl + "/";
        }
        return hiveJdbcUrl + configuration.getDbName() + (urlAppend == null ? "" : urlAppend);
    }

    private static List<String> constructChangePartitions(HoodieDatasetReference metadata,
        List<StoragePartition> partitions, PartitionStrategy partitionStrategy) {
        String[] partitionFieldNames = partitionStrategy.getHivePartitionFieldNames();

        List<String> changePartitions = Lists.newArrayList();
        String alterTable = "ALTER TABLE " + metadata.getDatabaseTableName();
        for (StoragePartition partition : partitions) {
            StringBuilder partBuilder = new StringBuilder();
            String[] partitionValues = partition.getPartitionFieldValues();
            Preconditions.checkArgument(partitionFieldNames.length == partitionValues.length,
                "Partition key parts " + Arrays.toString(partitionFieldNames)
                    + " does not match with partition values " + Arrays.toString(partitionValues)
                    + ". Check partition strategy. ");
            for (int i = 0; i < partitionFieldNames.length; i++) {
                partBuilder.append(partitionFieldNames[i]).append("=").append("'")
                    .append(partitionValues[i]).append("'");
            }
            String changePartition =
                alterTable + " PARTITION (" + partBuilder.toString() + ") SET LOCATION '"
                    + "hdfs://nameservice1" + partition.getPartitionPath() + "'";
            changePartitions.add(changePartition);
        }
        return changePartitions;
    }

    private static String constructAddPartitions(HoodieDatasetReference metadata,
        List<StoragePartition> partitions, PartitionStrategy partitionStrategy) {
        return constructAddPartitions(metadata.getDatabaseTableName(), partitions,
            partitionStrategy);
    }

    private static String constructAddPartitions(String newDbTableName,
        List<StoragePartition> partitions, PartitionStrategy partitionStrategy) {
        String[] partitionFieldNames = partitionStrategy.getHivePartitionFieldNames();
        StringBuilder alterSQL = new StringBuilder("ALTER TABLE ");
        alterSQL.append(newDbTableName).append(" ADD IF NOT EXISTS ");
        for (StoragePartition partition : partitions) {
            StringBuilder partBuilder = new StringBuilder();
            String[] partitionValues = partition.getPartitionFieldValues();
            Preconditions.checkArgument(partitionFieldNames.length == partitionValues.length,
                "Partition key parts " + Arrays.toString(partitionFieldNames)
                    + " does not match with partition values " + Arrays.toString(partitionValues)
                    + ". Check partition strategy. ");
            for (int i = 0; i < partitionFieldNames.length; i++) {
                partBuilder.append(partitionFieldNames[i]).append("=").append("'")
                    .append(partitionValues[i]).append("'");
            }
            alterSQL.append("  PARTITION (").append(partBuilder.toString()).append(") LOCATION '")
                .append(partition.getPartitionPath()).append("' ");
        }

        return alterSQL.toString();
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.error("Could not close the connection opened ", e);
            }
        }
    }
}
