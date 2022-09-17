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
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieCatalogException;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.sync.common.util.ConfigUtils;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabase;
import org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabaseOwner;
import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveDatabase;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabase.ALTER_DATABASE_OP;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabaseOwner.DATABASE_OWNER_NAME;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabaseOwner.DATABASE_OWNER_TYPE;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;
import static org.apache.hudi.configuration.FlinkOptions.PATH;
import static org.apache.hudi.table.catalog.CatalogOptions.DEFAULT_DB;
import static org.apache.hudi.table.catalog.TableOptionProperties.COMMENT;
import static org.apache.hudi.table.catalog.TableOptionProperties.PK_CONSTRAINT_NAME;
import static org.apache.hudi.table.catalog.TableOptionProperties.SPARK_SOURCE_PROVIDER;

/**
 * A catalog implementation for Hoodie based on MetaStore.
 */
public class HoodieHiveCatalog extends AbstractCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieHiveCatalog.class);

  private final HiveConf hiveConf;
  private IMetaStoreClient client;

  // optional catalog base path: used for db/table path inference.
  private final String catalogPath;

  public HoodieHiveCatalog(String catalogName, String catalogPath, String defaultDatabase, String hiveConfDir) {
    this(catalogName, catalogPath, defaultDatabase, HoodieCatalogUtil.createHiveConf(hiveConfDir), false);
  }

  public HoodieHiveCatalog(
      String catalogName,
      String catalogPath,
      String defaultDatabase,
      HiveConf hiveConf,
      boolean allowEmbedded) {
    super(catalogName, defaultDatabase == null ? DEFAULT_DB : defaultDatabase);
    // fallback to hive.metastore.warehouse.dir if catalog path is not specified
    this.catalogPath = catalogPath == null ? hiveConf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE) : catalogPath;
    this.hiveConf = hiveConf;
    if (!allowEmbedded) {
      checkArgument(
          !HoodieCatalogUtil.isEmbeddedMetastore(this.hiveConf),
          "Embedded metastore is not allowed. Make sure you have set a valid value for "
              + HiveConf.ConfVars.METASTOREURIS);
    }
    LOG.info("Created Hoodie Catalog '{}' in hms mode", catalogName);
  }

  @Override
  public void open() throws CatalogException {
    if (this.client == null) {
      try {
        this.client = Hive.get(hiveConf).getMSC();
      } catch (Exception e) {
        throw new HoodieCatalogException("Failed to create hive metastore client", e);
      }
      LOG.info("Connected to Hive metastore");
    }
    if (!databaseExists(getDefaultDatabase())) {
      LOG.info("{} does not exist, will be created.", getDefaultDatabase());
      CatalogDatabase database = new CatalogDatabaseImpl(Collections.emptyMap(), "default database");
      try {
        createDatabase(getDefaultDatabase(), database, true);
      } catch (DatabaseAlreadyExistException e) {
        throw new HoodieCatalogException(getName(), e);
      }
    }
  }

  @Override
  public void close() throws CatalogException {
    if (client != null) {
      client.close();
      client = null;
      LOG.info("Disconnect to hive metastore");
    }
  }

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  // ------ databases ------

  @Override
  public List<String> listDatabases() throws CatalogException {
    try {
      return client.getAllDatabases();
    } catch (TException e) {
      throw new HoodieCatalogException(
          String.format("Failed to list all databases in %s", getName()), e);
    }
  }

  private Database getHiveDatabase(String databaseName) throws DatabaseNotExistException {
    try {
      return client.getDatabase(databaseName);
    } catch (NoSuchObjectException e) {
      throw new DatabaseNotExistException(getName(), databaseName);
    } catch (TException e) {
      throw new HoodieCatalogException(
          String.format("Failed to get database %s from %s", databaseName, getName()), e);
    }
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    Database hiveDatabase = getHiveDatabase(databaseName);

    Map<String, String> properties = new HashMap<>(hiveDatabase.getParameters());

    properties.put(SqlCreateHiveDatabase.DATABASE_LOCATION_URI, hiveDatabase.getLocationUri());

    return new CatalogDatabaseImpl(properties, hiveDatabase.getDescription());
  }

  @Override
  public boolean databaseExists(String databaseName) throws CatalogException {
    try {
      return client.getDatabase(databaseName) != null;
    } catch (NoSuchObjectException e) {
      return false;
    } catch (TException e) {
      throw new HoodieCatalogException(
          String.format(
              "Failed to determine whether database %s exists or not", databaseName),
          e);
    }
  }

  @Override
  public void createDatabase(
      String databaseName, CatalogDatabase database, boolean ignoreIfExists)
      throws DatabaseAlreadyExistException, CatalogException {
    checkArgument(
        !isNullOrWhitespaceOnly(databaseName), "Database name can not null or empty");
    checkNotNull(database, "database cannot be null");

    Map<String, String> properties = database.getProperties();

    String dbLocationUri = properties.remove(SqlCreateHiveDatabase.DATABASE_LOCATION_URI);
    if (dbLocationUri == null && this.catalogPath != null) {
      // infer default location uri
      dbLocationUri = new Path(this.catalogPath, databaseName).toString();
    }

    Database hiveDatabase =
        new Database(databaseName, database.getComment(), dbLocationUri, properties);

    try {
      client.createDatabase(hiveDatabase);
    } catch (AlreadyExistsException e) {
      if (!ignoreIfExists) {
        throw new DatabaseAlreadyExistException(getName(), hiveDatabase.getName());
      }
    } catch (TException e) {
      throw new HoodieCatalogException(
          String.format("Failed to create database %s", hiveDatabase.getName()), e);
    }
  }

  @Override
  public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    try {
      client.dropDatabase(name, true, ignoreIfNotExists, cascade);
    } catch (NoSuchObjectException e) {
      if (!ignoreIfNotExists) {
        throw new DatabaseNotExistException(getName(), name);
      }
    } catch (InvalidOperationException e) {
      throw new DatabaseNotEmptyException(getName(), name);
    } catch (TException e) {
      throw new HoodieCatalogException(String.format("Failed to drop database %s", name), e);
    }
  }

  @Override
  public void alterDatabase(
      String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
      throws DatabaseNotExistException, CatalogException {
    checkArgument(
        !isNullOrWhitespaceOnly(databaseName), "Database name cannot be null or empty");
    checkNotNull(newDatabase, "New database cannot be null");

    // client.alterDatabase doesn't throw any exception if there is no existing database
    Database hiveDB;
    try {
      hiveDB = getHiveDatabase(databaseName);
    } catch (DatabaseNotExistException e) {
      if (!ignoreIfNotExists) {
        throw new DatabaseNotExistException(getName(), databaseName);
      }

      return;
    }

    try {
      client.alterDatabase(databaseName, alterDatabase(hiveDB, newDatabase));
    } catch (TException e) {
      throw new HoodieCatalogException(
          String.format("Failed to alter database %s", databaseName), e);
    }
  }

  private static Database alterDatabase(Database hiveDB, CatalogDatabase newDatabase) {
    Map<String, String> newParams = newDatabase.getProperties();
    String opStr = newParams.remove(ALTER_DATABASE_OP);
    if (opStr == null) {
      // by default is to alter db properties
      opStr = SqlAlterHiveDatabase.AlterHiveDatabaseOp.CHANGE_PROPS.name();
    }
    String newLocation = newParams.remove(SqlCreateHiveDatabase.DATABASE_LOCATION_URI);
    SqlAlterHiveDatabase.AlterHiveDatabaseOp op =
        SqlAlterHiveDatabase.AlterHiveDatabaseOp.valueOf(opStr);
    switch (op) {
      case CHANGE_PROPS:
        hiveDB.setParameters(newParams);
        break;
      case CHANGE_LOCATION:
        hiveDB.setLocationUri(newLocation);
        break;
      case CHANGE_OWNER:
        String ownerName = newParams.remove(DATABASE_OWNER_NAME);
        String ownerType = newParams.remove(DATABASE_OWNER_TYPE);
        hiveDB.setOwnerName(ownerName);
        switch (ownerType) {
          case SqlAlterHiveDatabaseOwner.ROLE_OWNER:
            hiveDB.setOwnerType(PrincipalType.ROLE);
            break;
          case SqlAlterHiveDatabaseOwner.USER_OWNER:
            hiveDB.setOwnerType(PrincipalType.USER);
            break;
          default:
            throw new CatalogException("Unsupported database owner type: " + ownerType);
        }
        break;
      default:
        throw new CatalogException("Unsupported alter database op:" + opStr);
    }
    // is_generic is deprecated, remove it
    if (hiveDB.getParameters() != null) {
      hiveDB.getParameters().remove(CatalogPropertiesUtil.IS_GENERIC);
    }
    return hiveDB;
  }

  // ------ tables ------

  private Table isHoodieTable(Table hiveTable) {
    if (!hiveTable.getParameters().getOrDefault(SPARK_SOURCE_PROVIDER, "").equalsIgnoreCase("hudi")
        && !isFlinkHoodieTable(hiveTable)) {
      throw new HoodieCatalogException(String.format("the %s is not hoodie table", hiveTable.getTableName()));
    }
    return hiveTable;
  }

  private boolean isFlinkHoodieTable(Table hiveTable) {
    return hiveTable.getParameters().getOrDefault(CONNECTOR.key(), "").equalsIgnoreCase("hudi");
  }

  @VisibleForTesting
  public Table getHiveTable(ObjectPath tablePath) throws TableNotExistException {
    try {
      Table hiveTable = client.getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
      return isHoodieTable(hiveTable);
    } catch (NoSuchObjectException e) {
      throw new TableNotExistException(getName(), tablePath);
    } catch (TException e) {
      throw new HoodieCatalogException(String.format("Failed to get table %s from Hive metastore", tablePath.getObjectName()));
    }
  }

  private Table translateSparkTable2Flink(ObjectPath tablePath, Table hiveTable) {
    if (!isFlinkHoodieTable(hiveTable)) {
      try {
        Map<String, String> parameters = hiveTable.getParameters();
        parameters.putAll(TableOptionProperties.translateSparkTableProperties2Flink(hiveTable));
        String path = hiveTable.getSd().getLocation();
        parameters.put(PATH.key(), path);
        if (!parameters.containsKey(FlinkOptions.HIVE_STYLE_PARTITIONING.key())) {
          Path hoodieTablePath = new Path(path);
          boolean hiveStyle = Arrays.stream(FSUtils.getFs(hoodieTablePath, hiveConf).listStatus(hoodieTablePath))
              .map(fileStatus -> fileStatus.getPath().getName())
              .filter(f -> !f.equals(".hoodie") && !f.equals("default"))
              .anyMatch(FilePathUtils::isHiveStylePartitioning);
          parameters.put(FlinkOptions.HIVE_STYLE_PARTITIONING.key(), String.valueOf(hiveStyle));
        }
        client.alter_table(tablePath.getDatabaseName(), tablePath.getObjectName(), hiveTable);
      } catch (Exception e) {
        throw new HoodieCatalogException("Failed to update table schema", e);
      }
    }
    return hiveTable;
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
    checkNotNull(tablePath, "Table path cannot be null");
    Table hiveTable = translateSparkTable2Flink(tablePath, getHiveTable(tablePath));
    String path = hiveTable.getSd().getLocation();
    Map<String, String> parameters = hiveTable.getParameters();
    Schema latestTableSchema = StreamerUtil.getLatestTableSchema(path, hiveConf);
    org.apache.flink.table.api.Schema schema;
    if (latestTableSchema != null) {
      org.apache.flink.table.api.Schema.Builder builder = org.apache.flink.table.api.Schema.newBuilder()
          .fromRowDataType(AvroSchemaConverter.convertToDataType(latestTableSchema));
      String pkConstraintName = parameters.get(PK_CONSTRAINT_NAME);
      String pkColumns = parameters.get(FlinkOptions.RECORD_KEY_FIELD.key());
      if (!StringUtils.isNullOrEmpty(pkConstraintName)) {
        // pkColumns expect not to be null
        builder.primaryKeyNamed(pkConstraintName, StringUtils.split(pkColumns, ","));
      } else if (pkColumns != null) {
        builder.primaryKey(StringUtils.split(pkColumns, ","));
      }
      schema = builder.build();
    } else {
      LOG.warn("{} does not have any hoodie schema, and use hive table schema to infer the table schema", tablePath);
      schema = HiveSchemaUtils.convertTableSchema(hiveTable);
    }
    Map<String, String> options = supplementOptions(tablePath, parameters);
    return CatalogTable.of(schema, parameters.get(COMMENT),
        HiveSchemaUtils.getFieldNames(hiveTable.getPartitionKeys()), options);
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    checkNotNull(tablePath, "Table path cannot be null");
    checkNotNull(table, "Table cannot be null");

    if (!databaseExists(tablePath.getDatabaseName())) {
      throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
    }

    if (!table.getOptions().getOrDefault(CONNECTOR.key(), "").equalsIgnoreCase("hudi")) {
      throw new HoodieCatalogException(String.format("The %s is not hoodie table", tablePath.getObjectName()));
    }

    if (table instanceof CatalogView) {
      throw new HoodieCatalogException("CREATE VIEW is not supported.");
    }

    try {
      boolean isMorTable = OptionsResolver.isMorTable(table.getOptions());
      Table hiveTable = instantiateHiveTable(tablePath, table, inferTablePath(tablePath, table), isMorTable);
      //create hive table
      client.createTable(hiveTable);
      //init hoodie metaClient
      initTableIfNotExists(tablePath, (CatalogTable) table);
    } catch (AlreadyExistsException e) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistException(getName(), tablePath, e);
      }
    } catch (Exception e) {
      throw new HoodieCatalogException(
          String.format("Failed to create table %s", tablePath.getFullName()), e);
    }
  }

  private void initTableIfNotExists(ObjectPath tablePath, CatalogTable catalogTable) {
    Configuration flinkConf = Configuration.fromMap(catalogTable.getOptions());
    final String avroSchema = AvroSchemaConverter.convertToSchema(catalogTable.getSchema().toPersistedRowDataType().getLogicalType()).toString();
    flinkConf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, avroSchema);

    // stores two copies of options:
    // - partition keys
    // - primary keys
    // because the HoodieTableMetaClient is a heavy impl, we try to avoid initializing it
    // when calling #getTable.

    if (catalogTable.getUnresolvedSchema().getPrimaryKey().isPresent()
        && !flinkConf.contains(FlinkOptions.RECORD_KEY_FIELD)) {
      final String pkColumns = String.join(",", catalogTable.getUnresolvedSchema().getPrimaryKey().get().getColumnNames());
      flinkConf.setString(FlinkOptions.RECORD_KEY_FIELD, pkColumns);
    }

    if (catalogTable.isPartitioned() && !flinkConf.contains(FlinkOptions.PARTITION_PATH_FIELD)) {
      final String partitions = String.join(",", catalogTable.getPartitionKeys());
      flinkConf.setString(FlinkOptions.PARTITION_PATH_FIELD, partitions);
    }

    if (!flinkConf.getOptional(PATH).isPresent()) {
      flinkConf.setString(PATH, inferTablePath(tablePath, catalogTable));
    }

    flinkConf.setString(FlinkOptions.TABLE_NAME, tablePath.getObjectName());
    try {
      StreamerUtil.initTableIfNotExists(flinkConf, hiveConf);
    } catch (IOException e) {
      throw new HoodieCatalogException("Initialize table exception.", e);
    }
  }

  private String inferTablePath(ObjectPath tablePath, CatalogBaseTable table) {
    String location = table.getOptions().getOrDefault(PATH.key(), "");
    if (StringUtils.isNullOrEmpty(location)) {
      try {
        Path dbLocation = new Path(client.getDatabase(tablePath.getDatabaseName()).getLocationUri());
        location = new Path(dbLocation, tablePath.getObjectName()).toString();
      } catch (TException e) {
        throw new HoodieCatalogException(String.format("Failed to infer hoodie table path for table %s", tablePath), e);
      }
    }
    return location;
  }

  private Table instantiateHiveTable(ObjectPath tablePath, CatalogBaseTable table, String location, boolean useRealTimeInputFormat) throws IOException {
    // let Hive set default parameters for us, e.g. serialization.format
    Table hiveTable =
        org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(
            tablePath.getDatabaseName(), tablePath.getObjectName());

    hiveTable.setOwner(UserGroupInformation.getCurrentUser().getUserName());
    hiveTable.setCreateTime((int) (System.currentTimeMillis() / 1000));

    Map<String, String> properties = new HashMap<>(table.getOptions());

    if (Boolean.parseBoolean(table.getOptions().get(CatalogOptions.TABLE_EXTERNAL.key()))) {
      hiveTable.setTableType(TableType.EXTERNAL_TABLE.toString());
      properties.put("EXTERNAL", "TRUE");
    }

    // Table comment
    if (table.getComment() != null) {
      properties.put(COMMENT, table.getComment());
    }

    //set pk
    if (table.getUnresolvedSchema().getPrimaryKey().isPresent()
        && !properties.containsKey(FlinkOptions.RECORD_KEY_FIELD.key())) {
      String pkColumns = String.join(",", table.getUnresolvedSchema().getPrimaryKey().get().getColumnNames());
      properties.put(PK_CONSTRAINT_NAME, table.getUnresolvedSchema().getPrimaryKey().get().getConstraintName());
      properties.put(FlinkOptions.RECORD_KEY_FIELD.key(), pkColumns);
    }

    if (!properties.containsKey(FlinkOptions.PATH.key())) {
      properties.put(FlinkOptions.PATH.key(), location);
    }

    //set sd
    StorageDescriptor sd = new StorageDescriptor();
    // the metadata fields should be included to keep sync with the hive sync tool,
    // because since Hive 3.x, there is validation when altering table,
    // when the metadata fields are synced through the hive sync tool,
    // a compatability issue would be reported.
    List<FieldSchema> allColumns = HiveSchemaUtils.toHiveFieldSchema(table.getSchema());

    // Table columns and partition keys
    CatalogTable catalogTable = (CatalogTable) table;

    final List<String> partitionKeys = HoodieCatalogUtil.getPartitionKeys(catalogTable);
    if (partitionKeys.size() > 0) {
      Pair<List<FieldSchema>, List<FieldSchema>> splitSchemas = HiveSchemaUtils.splitSchemaByPartitionKeys(allColumns, partitionKeys);
      List<FieldSchema> regularColumns = splitSchemas.getLeft();
      List<FieldSchema> partitionColumns = splitSchemas.getRight();

      sd.setCols(regularColumns);
      hiveTable.setPartitionKeys(partitionColumns);
    } else {
      sd.setCols(allColumns);
      hiveTable.setPartitionKeys(Collections.emptyList());
    }

    HoodieFileFormat baseFileFormat = HoodieFileFormat.PARQUET;
    //ignore uber input Format
    String inputFormatClassName = HoodieInputFormatUtils.getInputFormatClassName(baseFileFormat, useRealTimeInputFormat);
    String outputFormatClassName = HoodieInputFormatUtils.getOutputFormatClassName(baseFileFormat);
    String serDeClassName = HoodieInputFormatUtils.getSerDeClassName(baseFileFormat);
    sd.setInputFormat(inputFormatClassName);
    sd.setOutputFormat(outputFormatClassName);
    Map<String, String> serdeProperties = new HashMap<>();
    serdeProperties.put("path", location);
    serdeProperties.put(ConfigUtils.IS_QUERY_AS_RO_TABLE, String.valueOf(!useRealTimeInputFormat));
    serdeProperties.put("serialization.format", "1");

    serdeProperties.putAll(TableOptionProperties.translateFlinkTableProperties2Spark(catalogTable, hiveConf, properties, partitionKeys));

    sd.setSerdeInfo(new SerDeInfo(null, serDeClassName, serdeProperties));

    sd.setLocation(location);
    hiveTable.setSd(sd);

    hiveTable.setParameters(properties);
    return hiveTable;
  }

  @Override
  public List<String> listTables(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    checkArgument(
        !isNullOrWhitespaceOnly(databaseName), "Database name cannot be null or empty");

    try {
      return client.getAllTables(databaseName);
    } catch (UnknownDBException e) {
      throw new DatabaseNotExistException(getName(), databaseName);
    } catch (TException e) {
      throw new HoodieCatalogException(
          String.format("Failed to list tables in database %s", databaseName), e);
    }
  }

  @Override
  public List<String> listViews(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    throw new HoodieCatalogException("Hoodie catalog does not support to listViews");
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    checkNotNull(tablePath, "Table path cannot be null");

    try {
      return client.tableExists(tablePath.getDatabaseName(), tablePath.getObjectName());
    } catch (UnknownDBException e) {
      return false;
    } catch (TException e) {
      throw new CatalogException(
          String.format(
              "Failed to check whether table %s exists or not.",
              tablePath.getFullName()),
          e);
    }
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    checkNotNull(tablePath, "Table path cannot be null");

    try {
      client.dropTable(
          tablePath.getDatabaseName(),
          tablePath.getObjectName(),
          // Indicate whether associated data should be deleted.
          // Set to 'true' for now because Flink tables shouldn't have data in Hive. Can
          // be changed later if necessary
          true,
          ignoreIfNotExists);
    } catch (NoSuchObjectException e) {
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(getName(), tablePath);
      }
    } catch (TException e) {
      throw new HoodieCatalogException(
          String.format("Failed to drop table %s", tablePath.getFullName()), e);
    }
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
      throws TableNotExistException, TableAlreadyExistException, CatalogException {
    checkNotNull(tablePath, "Table path cannot be null");
    checkArgument(
        !isNullOrWhitespaceOnly(newTableName), "New table name cannot be null or empty");

    try {
      // alter_table() doesn't throw a clear exception when target table doesn't exist.
      // Thus, check the table existence explicitly
      if (tableExists(tablePath)) {
        ObjectPath newPath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
        // alter_table() doesn't throw a clear exception when new table already exists.
        // Thus, check the table existence explicitly
        if (tableExists(newPath)) {
          throw new TableAlreadyExistException(getName(), newPath);
        } else {
          Table hiveTable = getHiveTable(tablePath);

          //update hoodie
          StorageDescriptor sd = hiveTable.getSd();
          String location = sd.getLocation();
          HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(location).setConf(hiveConf).build();
          //Init table with new name
          HoodieTableMetaClient.withPropertyBuilder().fromProperties(metaClient.getTableConfig().getProps())
              .setTableName(newTableName)
              .initTable(hiveConf, location);

          hiveTable.setTableName(newTableName);
          client.alter_table(
              tablePath.getDatabaseName(), tablePath.getObjectName(), hiveTable);
        }
      } else if (!ignoreIfNotExists) {
        throw new TableNotExistException(getName(), tablePath);
      }
    } catch (Exception e) {
      throw new HoodieCatalogException(
          String.format("Failed to rename table %s", tablePath.getFullName()), e);
    }
  }

  private boolean sameOptions(Map<String, String> existingOptions, Map<String, String> newOptions, ConfigOption option) {
    return existingOptions.getOrDefault(option.key(), String.valueOf(option.defaultValue()))
        .equalsIgnoreCase(newOptions.getOrDefault(option.key(), String.valueOf(option.defaultValue())));
  }

  @Override
  public void alterTable(
      ObjectPath tablePath, CatalogBaseTable newCatalogTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    checkNotNull(tablePath, "Table path cannot be null");
    checkNotNull(newCatalogTable, "New catalog table cannot be null");

    if (!newCatalogTable.getOptions().getOrDefault(CONNECTOR.key(), "").equalsIgnoreCase("hudi")) {
      throw new HoodieCatalogException(String.format("The %s is not hoodie table", tablePath.getObjectName()));
    }
    if (newCatalogTable instanceof CatalogView) {
      throw new HoodieCatalogException("Hoodie catalog does not support to ALTER VIEW");
    }

    try {
      Table hiveTable = getHiveTable(tablePath);
      if (!sameOptions(hiveTable.getParameters(), newCatalogTable.getOptions(), FlinkOptions.TABLE_TYPE)
          || !sameOptions(hiveTable.getParameters(), newCatalogTable.getOptions(), FlinkOptions.INDEX_TYPE)) {
        throw new HoodieCatalogException("Hoodie catalog does not support to alter table type and index type");
      }
    } catch (TableNotExistException e) {
      if (!ignoreIfNotExists) {
        throw e;
      }
      return;
    }

    try {
      boolean isMorTable = OptionsResolver.isMorTable(newCatalogTable.getOptions());
      Table hiveTable = instantiateHiveTable(tablePath, newCatalogTable, inferTablePath(tablePath, newCatalogTable), isMorTable);
      //alter hive table
      client.alter_table(tablePath.getDatabaseName(), tablePath.getObjectName(), hiveTable);
    } catch (Exception e) {
      LOG.error("Failed to alter table {}", tablePath.getObjectName(), e);
      throw new HoodieCatalogException(String.format("Failed to alter table %s", tablePath.getObjectName()), e);
    }
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    throw new HoodieCatalogException("Not supported.");
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws TableNotExistException, TableNotPartitionedException,
      PartitionSpecInvalidException, CatalogException {
    throw new HoodieCatalogException("Not supported.");
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(
      ObjectPath tablePath, List<Expression> expressions)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    throw new HoodieCatalogException("Not supported.");
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    throw new HoodieCatalogException("Not supported.");
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    throw new HoodieCatalogException("Not supported.");
  }

  @Override
  public void createPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition partition,
      boolean ignoreIfExists)
      throws TableNotExistException, TableNotPartitionedException,
      PartitionSpecInvalidException, PartitionAlreadyExistsException,
      CatalogException {
    throw new HoodieCatalogException("Not supported.");
  }

  @Override
  public void dropPartition(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new HoodieCatalogException("Not supported.");
  }

  @Override
  public void alterPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition newPartition,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new HoodieCatalogException("Not supported.");
  }

  @Override
  public List<String> listFunctions(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath functionPath)
      throws FunctionNotExistException, CatalogException {
    throw new FunctionNotExistException(getName(), functionPath);
  }

  @Override
  public boolean functionExists(ObjectPath functionPath) throws CatalogException {
    return false;
  }

  @Override
  public void createFunction(
      ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
      throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new HoodieCatalogException("Not supported.");
  }

  @Override
  public void alterFunction(
      ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {
    throw new HoodieCatalogException("Not supported.");
  }

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {
    throw new HoodieCatalogException("Not supported.");
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
  public CatalogTableStatistics getPartitionStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public void alterTableStatistics(
      ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    throw new HoodieCatalogException("Not supported.");
  }

  @Override
  public void alterTableColumnStatistics(
      ObjectPath tablePath,
      CatalogColumnStatistics columnStatistics,
      boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException, TablePartitionedException {
    throw new HoodieCatalogException("Not supported.");
  }

  @Override
  public void alterPartitionStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogTableStatistics partitionStatistics,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new HoodieCatalogException("Not supported.");
  }

  @Override
  public void alterPartitionColumnStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogColumnStatistics columnStatistics,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new HoodieCatalogException("Not supported.");
  }

  private Map<String, String> supplementOptions(
      ObjectPath tablePath,
      Map<String, String> options) {
    if (HoodieCatalogUtil.isEmbeddedMetastore(hiveConf)) {
      return options;
    } else {
      Map<String, String> newOptions = new HashMap<>(options);
      // set up hive sync options
      newOptions.putIfAbsent(FlinkOptions.HIVE_SYNC_ENABLED.key(), "true");
      newOptions.putIfAbsent(FlinkOptions.HIVE_SYNC_METASTORE_URIS.key(), hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
      newOptions.putIfAbsent(FlinkOptions.HIVE_SYNC_MODE.key(), "hms");
      newOptions.putIfAbsent(FlinkOptions.HIVE_SYNC_SUPPORT_TIMESTAMP.key(), "true");
      newOptions.computeIfAbsent(FlinkOptions.HIVE_SYNC_DB.key(), k -> tablePath.getDatabaseName());
      newOptions.computeIfAbsent(FlinkOptions.HIVE_SYNC_TABLE.key(), k -> tablePath.getObjectName());
      return newOptions;
    }
  }
}
