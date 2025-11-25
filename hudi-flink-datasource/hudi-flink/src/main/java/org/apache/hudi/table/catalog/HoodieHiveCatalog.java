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

import org.apache.hudi.adapter.HiveCatalogConstants.AlterHiveDatabaseOp;
import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieCatalogException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.table.HoodieTableFactory;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.CatalogUtils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;
import static org.apache.hudi.adapter.HiveCatalogConstants.ALTER_DATABASE_OP;
import static org.apache.hudi.adapter.HiveCatalogConstants.DATABASE_LOCATION_URI;
import static org.apache.hudi.adapter.HiveCatalogConstants.DATABASE_OWNER_NAME;
import static org.apache.hudi.adapter.HiveCatalogConstants.DATABASE_OWNER_TYPE;
import static org.apache.hudi.adapter.HiveCatalogConstants.ROLE_OWNER;
import static org.apache.hudi.adapter.HiveCatalogConstants.USER_OWNER;
import static org.apache.hudi.configuration.FlinkOptions.PATH;
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
  private final boolean external;

  public HoodieHiveCatalog(String catalogName, Configuration options) {
    this(catalogName, options, HoodieCatalogUtil.createHiveConf(options.get(CatalogOptions.HIVE_CONF_DIR), options), false);
  }

  public HoodieHiveCatalog(
      String catalogName,
      Configuration options,
      HiveConf hiveConf,
      boolean allowEmbedded) {
    super(catalogName, options.get(CatalogOptions.DEFAULT_DATABASE));
    // fallback to hive.metastore.warehouse.dir if catalog path is not specified
    this.hiveConf = hiveConf;
    this.catalogPath = options.getString(CatalogOptions.CATALOG_PATH.key(), hiveConf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE));
    this.external = options.get(CatalogOptions.TABLE_EXTERNAL);
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

    properties.put(DATABASE_LOCATION_URI, hiveDatabase.getLocationUri());

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

    String dbLocationUri = properties.remove(DATABASE_LOCATION_URI);
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
      opStr = AlterHiveDatabaseOp.CHANGE_PROPS.name();
    }
    String newLocation = newParams.remove(DATABASE_LOCATION_URI);
    AlterHiveDatabaseOp op = AlterHiveDatabaseOp.valueOf(opStr);
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
          case ROLE_OWNER:
            hiveDB.setOwnerType(PrincipalType.ROLE);
            break;
          case USER_OWNER:
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
      throw new HoodieCatalogException(String.format("Table %s is not a hoodie table", hiveTable.getTableName()));
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
      throw new HoodieCatalogException(String.format("Failed to get table %s from Hive metastore", tablePath.getObjectName()), e);
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
          // read the table config first
          final boolean hiveStyle;
          Option<HoodieTableConfig> tableConfig = StreamerUtil.getTableConfig(path, hiveConf);
          if (tableConfig.isPresent() && tableConfig.get().contains(FlinkOptions.HIVE_STYLE_PARTITIONING.key())) {
            hiveStyle = Boolean.parseBoolean(tableConfig.get().getHiveStylePartitioningEnable());
          } else {
            // fallback to the partition path pattern
            Path hoodieTablePath = new Path(path);
            hiveStyle = Arrays.stream(HadoopFSUtils.getFs(hoodieTablePath, hiveConf).listStatus(hoodieTablePath))
                .map(fileStatus -> fileStatus.getPath().getName())
                .filter(f -> !f.equals(".hoodie") && !f.equals("default"))
                .anyMatch(FilePathUtils::isHiveStylePartitioning);
          }
          if (hiveStyle) {
            parameters.put(FlinkOptions.HIVE_STYLE_PARTITIONING.key(), "true");
          }
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
    HoodieSchema latestTableSchema = StreamerUtil.getLatestTableSchema(path, hiveConf);
    org.apache.flink.table.api.Schema schema;
    if (latestTableSchema != null) {
      String pkColumnsStr = parameters.get(FlinkOptions.RECORD_KEY_FIELD.key());
      List<String> pkColumns = StringUtils.isNullOrEmpty(pkColumnsStr)
          ? null : StringUtils.split(pkColumnsStr, ",");
      // if the table is initialized from spark, the write schema is nullable for pk columns.
      DataType tableDataType = DataTypeUtils.ensureColumnsAsNonNullable(
          AvroSchemaConverter.convertToDataType(latestTableSchema.getAvroSchema()), pkColumns);
      org.apache.flink.table.api.Schema.Builder builder = org.apache.flink.table.api.Schema.newBuilder()
          .fromRowDataType(tableDataType);
      String pkConstraintName = parameters.get(PK_CONSTRAINT_NAME);
      if (!StringUtils.isNullOrEmpty(pkConstraintName)) {
        // pkColumns expect not to be null
        builder.primaryKeyNamed(pkConstraintName, pkColumns);
      } else if (pkColumns != null) {
        builder.primaryKey(pkColumns);
      }
      schema = builder.build();
    } else {
      LOG.warn(" Table: {}, does not have a hoodie schema. Using hive table schema instead.", tablePath);
      schema = HiveSchemaUtils.convertTableSchema(hiveTable);
    }
    Map<String, String> options = supplementOptions(tablePath, parameters);
    return CatalogUtils.createCatalogTable(
        schema, HiveSchemaUtils.getFieldNames(hiveTable.getPartitionKeys()), options, parameters.get(COMMENT));
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    checkNotNull(tablePath, "Table path cannot be null");
    checkNotNull(table, "Table cannot be null");

    if (!databaseExists(tablePath.getDatabaseName())) {
      throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
    }

    if (!table.getOptions().getOrDefault(CONNECTOR.key(), "").equalsIgnoreCase(HoodieTableFactory.FACTORY_ID)) {
      throw new HoodieCatalogException(String.format("Unsupported connector identity %s, supported identity is %s",
          table.getOptions().getOrDefault(CONNECTOR.key(), ""), HoodieTableFactory.FACTORY_ID));
    }

    if (table instanceof CatalogView) {
      throw new HoodieCatalogException("CREATE VIEW is not supported.");
    }

    // validate parameter consistency
    validateParameterConsistency(table);

    try {
      boolean isMorTable = OptionsResolver.isMorTable(table.getOptions());
      Table hiveTable = instantiateHiveTable(tablePath, table, inferTablePath(tablePath, table), isMorTable);
      //create hive table
      client.createTable(hiveTable);
      //init hoodie metaClient
      HoodieTableMetaClient metaClient = initTableIfNotExists(tablePath, (CatalogTable) table);
      HoodieCatalogUtil.initPartitionBucketIndexMeta(metaClient, table);
    } catch (AlreadyExistsException e) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistException(getName(), tablePath, e);
      }
    } catch (Exception e) {
      throw new HoodieCatalogException(
          String.format("Failed to create table %s", tablePath.getFullName()), e);
    }
  }

  private HoodieTableMetaClient initTableIfNotExists(ObjectPath tablePath, CatalogTable catalogTable) {
    Configuration flinkConf = Configuration.fromMap(catalogTable.getOptions());
    final String avroSchema = AvroSchemaConverter.convertToSchema(
        DataTypeUtils.toRowType(catalogTable.getUnresolvedSchema()),
        AvroSchemaUtils.getAvroRecordQualifiedName(tablePath.getObjectName())).toString();
    flinkConf.set(FlinkOptions.SOURCE_AVRO_SCHEMA, avroSchema);

    // stores two copies of options:
    // - partition keys
    // - primary keys
    // because the HoodieTableMetaClient is a heavy impl, we try to avoid initializing it
    // when calling #getTable.

    if (catalogTable.getUnresolvedSchema().getPrimaryKey().isPresent()
        && !flinkConf.contains(FlinkOptions.RECORD_KEY_FIELD)) {
      final String pkColumns = String.join(",", catalogTable.getUnresolvedSchema().getPrimaryKey().get().getColumnNames());
      flinkConf.set(FlinkOptions.RECORD_KEY_FIELD, pkColumns);
    }

    if (catalogTable.isPartitioned() && !flinkConf.contains(FlinkOptions.PARTITION_PATH_FIELD)) {
      final String partitions = String.join(",", catalogTable.getPartitionKeys());
      flinkConf.set(FlinkOptions.PARTITION_PATH_FIELD, partitions);
      final String[] pks = flinkConf.get(FlinkOptions.RECORD_KEY_FIELD).split(",");
      boolean complexHoodieKey = pks.length > 1 || catalogTable.getPartitionKeys().size() > 1;
      StreamerUtil.checkKeygenGenerator(complexHoodieKey, flinkConf);
    }

    if (!catalogTable.isPartitioned()) {
      flinkConf.setString(FlinkOptions.KEYGEN_CLASS_NAME.key(), NonpartitionedAvroKeyGenerator.class.getName());
    }

    if (!flinkConf.getOptional(PATH).isPresent()) {
      flinkConf.set(PATH, inferTablePath(tablePath, catalogTable));
    }

    flinkConf.set(FlinkOptions.TABLE_NAME, tablePath.getObjectName());

    List<String> fields = new ArrayList<>();
    catalogTable.getUnresolvedSchema().getColumns().forEach(column -> fields.add(column.getName()));
    StreamerUtil.checkOrderingFields(flinkConf, fields);

    try {
      return StreamerUtil.initTableIfNotExists(flinkConf, hiveConf);
    } catch (IOException e) {
      throw new HoodieCatalogException("Initialize table exception.", e);
    }
  }

  @VisibleForTesting
  public String inferTablePath(ObjectPath tablePath, CatalogBaseTable table) {
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

    hiveTable.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
    hiveTable.setCreateTime((int) (System.currentTimeMillis() / 1000));

    Map<String, String> properties = new HashMap<>(table.getOptions());
    if (properties.containsKey(FlinkOptions.INDEX_TYPE.key())
        && !properties.containsKey(HoodieIndexConfig.INDEX_TYPE.key())) {
      properties.put(HoodieIndexConfig.INDEX_TYPE.key(), properties.get(FlinkOptions.INDEX_TYPE.key()));
    }
    properties.remove(FlinkOptions.INDEX_TYPE.key());
    hiveConf.getAllProperties().forEach((k, v) -> properties.put("hadoop." + k, String.valueOf(v)));

    if (external) {
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
    // a compatibility issue would be reported.
    boolean withOperationField = Boolean.parseBoolean(table.getOptions().getOrDefault(FlinkOptions.CHANGELOG_ENABLED.key(), "false"));
    List<FieldSchema> allColumns = HiveSchemaUtils.toHiveFieldSchema(table.getUnresolvedSchema(), withOperationField);

    // Table columns and partition keys
    CatalogTable catalogTable = (CatalogTable) table;

    final List<String> partitionKeys = HoodieCatalogUtil.getPartitionKeys(catalogTable);
    if (partitionKeys.size() > 0) {
      Pair<List<FieldSchema>, List<FieldSchema>> splitSchemas = HiveSchemaUtils.splitSchemaByPartitionKeys(allColumns, partitionKeys);
      List<FieldSchema> regularColumns = splitSchemas.getLeft();
      List<FieldSchema> partitionColumns = splitSchemas.getRight();

      String hivePartitionKeys = partitionColumns.stream().map(FieldSchema::getName).collect(Collectors.joining(","));
      ValidationUtils.checkArgument(hivePartitionKeys.equals(String.join(",", partitionKeys)),
          String.format("The order of regular fields(%s) and partition fields(%s) needs to be consistent", hivePartitionKeys, String.join(",", partitionKeys)));

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

    serdeProperties.putAll(TableOptionProperties.translateFlinkTableProperties2Spark(catalogTable, hiveConf, properties, partitionKeys, withOperationField));

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
          HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(location)
              .setConf(HadoopFSUtils.getStorageConfWithCopy(hiveConf)).build();
          //Init table with new name
          HoodieTableMetaClient.newTableBuilder().fromProperties(metaClient.getTableConfig().getProps())
              .setTableName(newTableName)
              .initTable(HadoopFSUtils.getStorageConfWithCopy(hiveConf), location);

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

  @Override
  public void alterTable(
      ObjectPath tablePath, CatalogBaseTable newCatalogTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    HoodieCatalogUtil.alterTable(this, tablePath, newCatalogTable, Collections.emptyList(), ignoreIfNotExists, hiveConf, this::inferTablePath, this::refreshHMSTable);
  }

  public void alterTable(ObjectPath tablePath, CatalogBaseTable newCatalogTable, List tableChanges,
                         boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
    HoodieCatalogUtil.alterTable(this, tablePath, newCatalogTable, tableChanges, ignoreIfNotExists, hiveConf, this::inferTablePath, this::refreshHMSTable);
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws TableNotExistException, TableNotPartitionedException,
      PartitionSpecInvalidException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(
      ObjectPath tablePath, List<Expression> expressions)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    return Collections.emptyList();
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
    checkNotNull(tablePath, "Table path cannot be null");
    checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

    final CatalogBaseTable table;
    try {
      table = getTable(tablePath);
    } catch (TableNotExistException e) {
      if (!ignoreIfNotExists) {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
      } else {
        return;
      }
    }
    try (HoodieFlinkWriteClient<?> writeClient = HoodieCatalogUtil.createWriteClient(tablePath, table, hiveConf, this::inferTablePath)) {
      boolean hiveStylePartitioning = Boolean.parseBoolean(table.getOptions().get(FlinkOptions.HIVE_STYLE_PARTITIONING.key()));
      String instantTime = writeClient.startDeletePartitionCommit();
      writeClient.deletePartitions(Collections.singletonList(HoodieCatalogUtil.inferPartitionPath(hiveStylePartitioning, partitionSpec)), instantTime)
          .forEach(writeStatus -> {
            if (writeStatus.hasErrors()) {
              throw new HoodieMetadataException(String.format("Failed to commit metadata table records at file id %s.", writeStatus.getFileId()));
            }
          });

      client.dropPartition(
          tablePath.getDatabaseName(),
          tablePath.getObjectName(),
          HoodieCatalogUtil.getOrderedPartitionValues(
              getName(), getHiveConf(), partitionSpec, ((CatalogTable) table).getPartitionKeys(), tablePath),
          true);
    } catch (NoSuchObjectException e) {
      if (!ignoreIfNotExists) {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
      }
    } catch (MetaException | PartitionSpecInvalidException e) {
      throw new PartitionNotExistException(getName(), tablePath, partitionSpec, e);
    } catch (Exception e) {
      throw new CatalogException(
          String.format(
              "Failed to drop partition %s of table %s", partitionSpec, tablePath), e);
    }
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

  private void refreshHMSTable(ObjectPath tablePath, CatalogBaseTable newCatalogTable) {
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

  public void validateParameterConsistency(CatalogBaseTable table) {
    Map<String, String> properties = new HashMap<>(table.getOptions());

    //Check consistency between pk statement and option 'hoodie.datasource.write.recordkey.field'.
    String pkError = String.format("Primary key fields definition has inconsistency between pk statement and option '%s'",
        FlinkOptions.RECORD_KEY_FIELD.key());
    if (table.getUnresolvedSchema().getPrimaryKey().isPresent()
        && properties.containsKey(FlinkOptions.RECORD_KEY_FIELD.key())) {
      List<String> pks = table.getUnresolvedSchema().getPrimaryKey().get().getColumnNames();
      String[] pkFromOptions = properties.get(FlinkOptions.RECORD_KEY_FIELD.key()).split(",");
      if (pkFromOptions.length != pks.size()) {
        throw new HoodieValidationException(pkError);
      }
      for (String field : pkFromOptions) {
        if (!pks.contains(field)) {
          throw new HoodieValidationException(pkError);
        }
      }
    }

    //Check consistency between partition key statement and option 'hoodie.datasource.write.partitionpath.field'.
    String partitionKeyError = String.format("Partition key fields definition has inconsistency between partition key statement and option '%s'",
        FlinkOptions.PARTITION_PATH_FIELD.key());
    CatalogTable catalogTable = (CatalogTable) table;
    if (catalogTable.isPartitioned() && properties.containsKey(FlinkOptions.PARTITION_PATH_FIELD.key())) {
      final List<String> partitions = catalogTable.getPartitionKeys();
      final String[] partitionsFromOptions = properties.get(FlinkOptions.PARTITION_PATH_FIELD.key()).split(",");
      if (partitionsFromOptions.length != partitions.size()) {
        throw new HoodieValidationException(pkError);
      }
      for (String field : partitionsFromOptions) {
        if (!partitions.contains(field)) {
          throw new HoodieValidationException(partitionKeyError);
        }
      }
    }
  }

  @VisibleForTesting
  public IMetaStoreClient getClient() {
    return client;
  }
}
