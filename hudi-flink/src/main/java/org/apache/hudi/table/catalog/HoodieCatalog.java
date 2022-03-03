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

package org.apache.hudi.table.catalog;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.hudi.table.catalog.CatalogOptions.CATALOG_PATH;
import static org.apache.hudi.table.catalog.CatalogOptions.DEFAULT_DATABASE;

/**
 * Catalog that can set up common options for underneath table.
 */
public class HoodieCatalog extends AbstractCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieCatalog.class);

  private final org.apache.hadoop.conf.Configuration hadoopConf;
  private final String catalogPathStr;
  private final Map<String, String> tableCommonOptions;

  private Path catalogPath;
  private FileSystem fs;

  public HoodieCatalog(String name, Configuration options) {
    super(name, options.get(DEFAULT_DATABASE));
    this.catalogPathStr = options.get(CATALOG_PATH);
    this.hadoopConf = StreamerUtil.getHadoopConf();
    this.tableCommonOptions = CatalogOptions.tableCommonOptions(options);
  }

  @Override
  public void open() throws CatalogException {
    fs = FSUtils.getFs(catalogPathStr, hadoopConf);
    catalogPath = new Path(catalogPathStr);
    try {
      if (!fs.exists(catalogPath)) {
        throw new CatalogException(String.format("Catalog %s path %s does not exist.", getName(), catalogPathStr));
      }
    } catch (IOException e) {
      throw new CatalogException(String.format("Checking catalog path %s exists exception.", catalogPathStr), e);
    }
  }

  @Override
  public void close() throws CatalogException {
    try {
      fs.close();
    } catch (IOException e) {
      throw new CatalogException("Closing FileSystem exception.", e);
    }
  }

  // ------ databases ------

  @Override
  public List<String> listDatabases() throws CatalogException {
    try {
      FileStatus[] fileStatuses = fs.listStatus(catalogPath);
      return Arrays.stream(fileStatuses)
          .filter(FileStatus::isDirectory)
          .map(fileStatus -> fileStatus.getPath().getName())
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new CatalogException("Listing database exception.", e);
    }
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
    if (databaseExists(databaseName)) {
      return new CatalogDatabaseImpl(Collections.emptyMap(), null);
    } else {
      throw new DatabaseNotExistException(getName(), databaseName);
    }
  }

  @Override
  public boolean databaseExists(String databaseName) throws CatalogException {
    checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

    return listDatabases().contains(databaseName);
  }

  @Override
  public void createDatabase(String databaseName, CatalogDatabase catalogDatabase, boolean ignoreIfExists)
      throws DatabaseAlreadyExistException, CatalogException {
    if (databaseExists(databaseName)) {
      if (ignoreIfExists) {
        return;
      } else {
        throw new DatabaseAlreadyExistException(getName(), databaseName);
      }
    }

    if (!CollectionUtil.isNullOrEmpty(catalogDatabase.getProperties())) {
      throw new CatalogException("Hudi catalog doesn't support to create database with options.");
    }

    Path dbPath = new Path(catalogPath, databaseName);
    try {
      fs.mkdirs(dbPath);
    } catch (IOException e) {
      throw new CatalogException(String.format("Creating database %s exception.", databaseName), e);
    }
  }

  @Override
  public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    if (!databaseExists(databaseName)) {
      if (ignoreIfNotExists) {
        return;
      } else {
        throw new DatabaseNotExistException(getName(), databaseName);
      }
    }

    List<String> tables = listTables(databaseName);
    if (!tables.isEmpty() && !cascade) {
      throw new DatabaseNotEmptyException(getName(), databaseName);
    }

    if (databaseName.equals(getDefaultDatabase())) {
      throw new IllegalArgumentException(
          "Hudi catalog doesn't support to drop the default database.");
    }

    Path dbPath = new Path(catalogPath, databaseName);
    try {
      fs.delete(dbPath, true);
    } catch (IOException e) {
      throw new CatalogException(String.format("Dropping database %s exception.", databaseName), e);
    }
  }

  @Override
  public void alterDatabase(String databaseName, CatalogDatabase catalogDatabase, boolean ignoreIfNotExists)
      throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException("Altering database is not implemented.");
  }

  // ------ tables ------

  @Override
  public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
    if (!databaseExists(databaseName)) {
      throw new DatabaseNotExistException(getName(), databaseName);
    }

    Path dbPath = new Path(catalogPath, databaseName);
    try {
      return Arrays.stream(fs.listStatus(dbPath))
          .filter(FileStatus::isDirectory)
          .map(fileStatus -> fileStatus.getPath().getName())
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new CatalogException(String.format("Listing table in database %s exception.", dbPath), e);
    }
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
    if (!tableExists(tablePath)) {
      throw new TableNotExistException(getName(), tablePath);
    }

    final String path = inferTablePath(catalogPathStr, tablePath);
    Map<String, String> options = TableOptionProperties.loadFromProperties(path, hadoopConf);
    final Schema latestSchema = getLatestTableSchema(path);
    if (latestSchema != null) {
      org.apache.flink.table.api.Schema.Builder builder = org.apache.flink.table.api.Schema.newBuilder()
          .fromRowDataType(AvroSchemaConverter.convertToDataType(latestSchema));
      final String pkConstraintName = TableOptionProperties.getPkConstraintName(options);
      if (pkConstraintName != null) {
        builder.primaryKeyNamed(pkConstraintName, TableOptionProperties.getPkColumns(options));
      }
      final org.apache.flink.table.api.Schema schema = builder.build();
      return CatalogTable.of(
          schema,
          TableOptionProperties.getComment(options),
          TableOptionProperties.getPartitionColumns(options),
          TableOptionProperties.getTableOptions(options));
    } else {
      throw new TableNotExistException(getName(), tablePath);
    }
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable catalogTable, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    if (!databaseExists(tablePath.getDatabaseName())) {
      throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
    }
    if (tableExists(tablePath)) {
      if (ignoreIfExists) {
        return;
      } else {
        throw new TableAlreadyExistException(getName(), tablePath);
      }
    }

    if (catalogTable instanceof CatalogView) {
      throw new UnsupportedOperationException(
          "Hudi catalog doesn't support to CREATE VIEW.");
    }

    ResolvedCatalogTable resolvedTable = (ResolvedCatalogTable) catalogTable;
    final String tablePathStr = inferTablePath(catalogPathStr, tablePath);
    Map<String, String> options = applyOptionsHook(tablePathStr, catalogTable.getOptions());
    Configuration conf = Configuration.fromMap(options);
    conf.setString(FlinkOptions.PATH, tablePathStr);
    ResolvedSchema resolvedSchema = resolvedTable.getResolvedSchema();
    if (!resolvedSchema.getPrimaryKey().isPresent()) {
      throw new CatalogException("Primary key definition is missing");
    }
    final String avroSchema = AvroSchemaConverter.convertToSchema(resolvedSchema.toPhysicalRowDataType().getLogicalType()).toString();
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, avroSchema);

    // stores two copies of options:
    // - partition keys
    // - primary keys
    // because the HoodieTableMetaClient is a heavy impl, we try to avoid initializing it
    // when calling #getTable.

    final String pkColumns = String.join(",", resolvedSchema.getPrimaryKey().get().getColumns());
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, pkColumns);
    options.put(TableOptionProperties.PK_CONSTRAINT_NAME, resolvedSchema.getPrimaryKey().get().getName());
    options.put(TableOptionProperties.PK_COLUMNS, pkColumns);

    if (resolvedTable.isPartitioned()) {
      final String partitions = String.join(",", resolvedTable.getPartitionKeys());
      conf.setString(FlinkOptions.PARTITION_PATH_FIELD, partitions);
      options.put(TableOptionProperties.PARTITION_COLUMNS, partitions);
    }
    conf.setString(FlinkOptions.TABLE_NAME, tablePath.getObjectName());
    try {
      StreamerUtil.initTableIfNotExists(conf);
      // prepare the non-table-options properties
      if (!StringUtils.isNullOrWhitespaceOnly(resolvedTable.getComment())) {
        options.put(TableOptionProperties.COMMENT, resolvedTable.getComment());
      }
      TableOptionProperties.createProperties(tablePathStr, hadoopConf, options);
    } catch (IOException e) {
      throw new CatalogException(String.format("Initialize table path %s exception.", tablePathStr), e);
    }
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    return StreamerUtil.tableExists(inferTablePath(catalogPathStr, tablePath), hadoopConf);
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    if (!tableExists(tablePath)) {
      if (ignoreIfNotExists) {
        return;
      } else {
        throw new TableNotExistException(getName(), tablePath);
      }
    }

    Path path = new Path(inferTablePath(catalogPathStr, tablePath));
    try {
      this.fs.delete(path, true);
    } catch (IOException e) {
      throw new CatalogException(String.format("Dropping table %s exception.", tablePath), e);
    }
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
      throws TableNotExistException, TableAlreadyExistException, CatalogException {
    throw new UnsupportedOperationException("renameTable is not implemented.");
  }

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable catalogBaseTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException("alterTable is not implemented.");
  }

  @Override
  public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec)
      throws PartitionNotExistException, CatalogException {
    throw new PartitionNotExistException(getName(), tablePath, catalogPartitionSpec);
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec) throws CatalogException {
    return false;
  }

  @Override
  public void createPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean ignoreIfExists)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
    throw new UnsupportedOperationException("createPartition is not implemented.");
  }

  @Override
  public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException("dropPartition is not implemented.");
  }

  @Override
  public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException("alterPartition is not implemented.");
  }

  @Override
  public List<String> listFunctions(String databaseName) throws DatabaseNotExistException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
    throw new FunctionNotExistException(getName(), functionPath);
  }

  @Override
  public boolean functionExists(ObjectPath functionPath) throws CatalogException {
    return false;
  }

  @Override
  public void createFunction(ObjectPath functionPath, CatalogFunction catalogFunction, boolean ignoreIfExists)
      throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException("createFunction is not implemented.");
  }

  @Override
  public void alterFunction(ObjectPath functionPath, CatalogFunction catalogFunction, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {
    throw new UnsupportedOperationException("alterFunction is not implemented.");
  }

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {
    throw new UnsupportedOperationException("dropFunction is not implemented.");
  }

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec)
      throws PartitionNotExistException, CatalogException {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec)
      throws PartitionNotExistException, CatalogException {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics catalogTableStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException("alterTableStatistics is not implemented.");
  }

  @Override
  public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics catalogColumnStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException, TablePartitionedException {
    throw new UnsupportedOperationException("alterTableColumnStatistics is not implemented.");
  }

  @Override
  public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, CatalogTableStatistics catalogTableStatistics, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException("alterPartitionStatistics is not implemented.");
  }

  @Override
  public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, CatalogColumnStatistics catalogColumnStatistics, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException("alterPartitionColumnStatistics is not implemented.");
  }

  private @Nullable Schema getLatestTableSchema(String path) {
    if (path != null && StreamerUtil.tableExists(path, hadoopConf)) {
      try {
        HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(path, hadoopConf);
        return new TableSchemaResolver(metaClient).getTableAvroSchema(false); // change log mode is not supported now
      } catch (Throwable throwable) {
        LOG.warn("Error while resolving the latest table schema.", throwable);
        // ignored
      }
    }
    return null;
  }

  private Map<String, String> applyOptionsHook(String tablePath, Map<String, String> options) {
    Map<String, String> newOptions = new HashMap<>(options);
    newOptions.put("connector", "hudi");
    newOptions.computeIfAbsent(FlinkOptions.PATH.key(), k -> tablePath);
    tableCommonOptions.forEach(newOptions::putIfAbsent);
    return newOptions;
  }

  private String inferTablePath(String catalogPath, ObjectPath tablePath) {
    return String.format("%s/%s/%s", catalogPath, tablePath.getDatabaseName(), tablePath.getObjectName());
  }
}
