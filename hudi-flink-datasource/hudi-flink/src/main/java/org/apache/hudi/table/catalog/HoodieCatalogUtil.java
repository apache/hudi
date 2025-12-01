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

import org.apache.hudi.adapter.Utils;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.PartitionBucketIndexHashingConfig;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieCatalogException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.convert.InternalSchemaConverter;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;
import static org.apache.hudi.table.catalog.CatalogOptions.HIVE_SITE_FILE;

/**
 * Utilities for Hoodie Catalog.
 */
public class HoodieCatalogUtil {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieCatalogUtil.class);

  /**
   * Returns a new {@code HiveConf}.
   *
   * @param hiveConfDir Hive conf directory path.
   * @return A HiveConf instance.
   */
  public static HiveConf createHiveConf(@Nullable String hiveConfDir, org.apache.flink.configuration.Configuration flinkConf) {
    // create HiveConf from hadoop configuration with hadoop conf directory configured.
    Configuration hadoopConf = HadoopConfigurations.getHadoopConf(flinkConf);

    // ignore all the static conf file URLs that HiveConf may have set
    HiveConf.setHiveSiteLocation(null);
    HiveConf.setLoadMetastoreConfig(false);
    HiveConf.setLoadHiveServer2Config(false);
    HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);

    LOG.info("Setting hive conf dir as {}", hiveConfDir);

    if (hiveConfDir != null) {
      Path hiveSite = new Path(hiveConfDir, HIVE_SITE_FILE);
      if (!hiveSite.toUri().isAbsolute()) {
        // treat relative URI as local file to be compatible with previous behavior
        hiveSite = new Path(new File(hiveSite.toString()).toURI());
      }
      try (InputStream inputStream = hiveSite.getFileSystem(hadoopConf).open(hiveSite)) {
        hiveConf.addResource(inputStream, hiveSite.toString());
        // trigger a read from the conf so that the input stream is read
        isEmbeddedMetastore(hiveConf);
      } catch (IOException e) {
        throw new CatalogException(
            "Failed to load hive-site.xml from specified path:" + hiveSite, e);
      }
    } else {
      // user doesn't provide hive conf dir, we try to find it in classpath
      URL hiveSite =
          Thread.currentThread().getContextClassLoader().getResource(HIVE_SITE_FILE);
      if (hiveSite != null) {
        LOG.info("Found {} in classpath: {}", HIVE_SITE_FILE, hiveSite);
        hiveConf.addResource(hiveSite);
      }
    }
    return hiveConf;
  }

  /**
   * Check whether the hive.metastore.uris is empty
   */
  public static boolean isEmbeddedMetastore(HiveConf hiveConf) {
    return isNullOrWhitespaceOnly(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
  }

  /**
   * Returns the partition key list with given table.
   */
  public static List<String> getPartitionKeys(CatalogTable table) {
    // the PARTITIONED BY syntax always has higher priority than option FlinkOptions#PARTITION_PATH_FIELD
    if (table.isPartitioned()) {
      return table.getPartitionKeys();
    } else if (table.getOptions().containsKey(FlinkOptions.PARTITION_PATH_FIELD.key())) {
      return Arrays.stream(table.getOptions().get(FlinkOptions.PARTITION_PATH_FIELD.key()).split(","))
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  /**
   * Returns the partition path with given {@link CatalogPartitionSpec}.
   */
  public static String inferPartitionPath(boolean hiveStylePartitioning, CatalogPartitionSpec catalogPartitionSpec) {
    return catalogPartitionSpec.getPartitionSpec().entrySet()
        .stream().map(entry ->
            hiveStylePartitioning
                ? String.format("%s=%s", entry.getKey(), entry.getValue())
                : entry.getValue())
        .collect(Collectors.joining("/"));
  }

  /**
   * Returns a list of ordered partition values by re-arranging them based on the given list of
   * partition keys. If the partition value is null, it'll be converted into default partition
   * name.
   *
   * @param partitionSpec The partition spec
   * @param partitionKeys The partition keys
   * @param tablePath     The table path
   * @return A list of partition values ordered by partition keys
   * @throws PartitionSpecInvalidException thrown if partitionSpec and partitionKeys have
   *     different sizes, or any key in partitionKeys doesn't exist in partitionSpec.
   */
  @VisibleForTesting
  public static List<String> getOrderedPartitionValues(
      String catalogName,
      HiveConf hiveConf,
      CatalogPartitionSpec partitionSpec,
      List<String> partitionKeys,
      ObjectPath tablePath)
      throws PartitionSpecInvalidException {
    Map<String, String> spec = partitionSpec.getPartitionSpec();
    if (spec.size() != partitionKeys.size()) {
      throw new PartitionSpecInvalidException(catalogName, partitionKeys, tablePath, partitionSpec);
    }

    List<String> values = new ArrayList<>(spec.size());
    for (String key : partitionKeys) {
      if (!spec.containsKey(key)) {
        throw new PartitionSpecInvalidException(catalogName, partitionKeys, tablePath, partitionSpec);
      } else {
        String value = spec.get(key);
        if (value == null) {
          value = hiveConf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME);
        }
        values.add(value);
      }
    }

    return values;
  }

  /**
   * Modifies an existing hoodie table. Note that the new and old {@link CatalogBaseTable} must be of the same kind. For example, this doesn't allow altering a regular table to partitioned table, or
   * altering a mor table to a cow table, or altering index type and vice versa.
   *
   * @param catalog            hoodie catalog
   * @param tablePath          path of the table or view to be modified
   * @param newTable           the new table definition
   * @param tableChanges       change to describe the modification between the newTable and the original table
   * @param ignoreIfNotExists  flag to specify behavior when the table or view does not exist: if set to false, throw an exception, if set to true, do nothing.
   * @param hadoopConf         hadoop configuration
   * @param inferTablePathFunc function to infer hoodie table path
   * @param postAlterTableFunc function to do post process after alter table
   * @throws TableNotExistException if the table does not exist
   * @throws CatalogException       in case of any runtime exception
   */
  protected static void alterTable(
      AbstractCatalog catalog,
      ObjectPath tablePath,
      CatalogBaseTable newTable,
      List tableChanges,
      boolean ignoreIfNotExists,
      org.apache.hadoop.conf.Configuration hadoopConf,
      BiFunction<ObjectPath, CatalogBaseTable, String> inferTablePathFunc,
      BiConsumer<ObjectPath, CatalogBaseTable> postAlterTableFunc) throws TableNotExistException, CatalogException {
    checkNotNull(tablePath, "Table path cannot be null");
    checkNotNull(newTable, "New catalog table cannot be null");

    if (!isUpdatePermissible(catalog, tablePath, newTable, ignoreIfNotExists)) {
      return;
    }
    if (!tableChanges.isEmpty()) {
      CatalogBaseTable oldTable = catalog.getTable(tablePath);
      HoodieFlinkWriteClient<?> writeClient = createWriteClient(tablePath, oldTable, hadoopConf, inferTablePathFunc);
      Pair<InternalSchema, HoodieTableMetaClient> pair = writeClient.getInternalSchemaAndMetaClient();
      InternalSchema oldSchema = pair.getLeft();
      Function<LogicalType, Type> convertFunc = (LogicalType logicalType) -> InternalSchemaConverter.convertToField(HoodieSchema.fromAvroSchema(AvroSchemaConverter.convertToSchema(logicalType)));
      InternalSchema newSchema = Utils.applyTableChange(oldSchema, tableChanges, convertFunc);
      if (!oldSchema.equals(newSchema)) {
        writeClient.setOperationType(WriteOperationType.ALTER_SCHEMA);
        writeClient.commitTableChange(newSchema, pair.getRight());
      }
    }
    postAlterTableFunc.accept(tablePath, newTable);
  }

  protected static HoodieFlinkWriteClient<?> createWriteClient(
      ObjectPath tablePath,
      CatalogBaseTable table,
      org.apache.hadoop.conf.Configuration hadoopConf,
      BiFunction<ObjectPath, CatalogBaseTable, String> inferTablePathFunc) {
    Map<String, String> options = table.getOptions();
    String tablePathStr = inferTablePathFunc.apply(tablePath, table);
    return createWriteClient(options, tablePathStr, tablePath, hadoopConf);
  }

  protected static HoodieFlinkWriteClient<?> createWriteClient(
      Map<String, String> options,
      String tablePathStr,
      ObjectPath tablePath,
      org.apache.hadoop.conf.Configuration hadoopConf) {
    return FlinkWriteClients.createWriteClientV2(
        org.apache.flink.configuration.Configuration.fromMap(options)
            .set(FlinkOptions.TABLE_NAME, tablePath.getObjectName())
            .set(
                FlinkOptions.SOURCE_AVRO_SCHEMA,
                StreamerUtil.createMetaClient(tablePathStr, hadoopConf)
                    .getTableConfig().getTableCreateSchema().get().toString()));
  }

  private static boolean sameOptions(Map<String, String> parameters1, Map<String, String> parameters2, ConfigOption<String> option) {
    return parameters1.getOrDefault(option.key(), String.valueOf(option.defaultValue()))
        .equalsIgnoreCase(parameters2.getOrDefault(option.key(), String.valueOf(option.defaultValue())));
  }

  private static boolean isUpdatePermissible(AbstractCatalog catalog, ObjectPath tablePath, CatalogBaseTable newCatalogTable, boolean ignoreIfNotExists) throws TableNotExistException {
    if (!newCatalogTable.getOptions().getOrDefault(CONNECTOR.key(), "").equalsIgnoreCase("hudi")) {
      throw new HoodieCatalogException(String.format("The %s is not hoodie table", tablePath.getObjectName()));
    }
    if (newCatalogTable instanceof CatalogView) {
      throw new HoodieCatalogException("Hoodie catalog does not support to ALTER VIEW");
    }

    if (!catalog.tableExists(tablePath)) {
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(catalog.getName(), tablePath);
      } else {
        return false;
      }
    }
    CatalogBaseTable oldCatalogTable = catalog.getTable(tablePath);
    List<String> oldPartitionKeys = HoodieCatalogUtil.getPartitionKeys((CatalogTable) oldCatalogTable);
    List<String> newPartitionKeys = HoodieCatalogUtil.getPartitionKeys((CatalogTable) newCatalogTable);
    if (!oldPartitionKeys.equals(newPartitionKeys)) {
      throw new HoodieCatalogException("Hoodie catalog does not support to alter table partition keys");
    }
    Map<String, String> oldOptions = oldCatalogTable.getOptions();
    if (!sameOptions(oldOptions, newCatalogTable.getOptions(), FlinkOptions.TABLE_TYPE)
        || !sameOptions(oldOptions, newCatalogTable.getOptions(), FlinkOptions.INDEX_TYPE)) {
      throw new HoodieCatalogException("Hoodie catalog does not support to alter table type and index type");
    }
    return true;
  }

  public static boolean initPartitionBucketIndexMeta(HoodieTableMetaClient metaClient, CatalogBaseTable catalogTable) {
    Map<String, String> options = catalogTable.getOptions();
    String expressions = options.getOrDefault(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS.key(), "");
    String rule = options.getOrDefault(FlinkOptions.BUCKET_INDEX_PARTITION_RULE.key(), FlinkOptions.BUCKET_INDEX_PARTITION_RULE.defaultValue());
    int bucketNumber = options.containsKey(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key())
        ? Integer.valueOf(options.get(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key())) : FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.defaultValue();

    return PartitionBucketIndexHashingConfig.saveHashingConfig(metaClient, expressions, rule, bucketNumber, null);
  }
}
