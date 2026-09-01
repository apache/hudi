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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;
import java.util.Map;

/**
 * Upgrade handle to assist in upgrading hoodie table from version 1 to 2.
 */
public class OneToTwoUpgradeHandler implements UpgradeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(OneToTwoUpgradeHandler.class);
  private static final String NESTED_FIELD_SEPARATOR = ".";

  @Override
  public Map<ConfigProperty, String> upgrade(
      HoodieWriteConfig config, HoodieEngineContext context, String instantTime,
      SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    Map<ConfigProperty, String> tablePropsToAdd = new Hashtable<>();
    tablePropsToAdd.put(HoodieTableConfig.PARTITION_FIELDS, upgradeDowngradeHelper.getPartitionColumns(config));
    tablePropsToAdd.put(HoodieTableConfig.RECORDKEY_FIELDS, config.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()));
    tablePropsToAdd.put(HoodieTableConfig.BASE_FILE_FORMAT, config.getString(HoodieTableConfig.BASE_FILE_FORMAT));
    HoodieTableMetaClient metaClient = upgradeDowngradeHelper.getTable(config, context).getMetaClient();
    getPreCombineFieldToPersist(config, metaClient)
        .ifPresent(preCombineField -> tablePropsToAdd.put(HoodieTableConfig.PRECOMBINE_FIELD, preCombineField));
    return tablePropsToAdd;
  }

  /**
   * Returns the ordering field to record in {@code hoodie.properties}, if one can be established.
   *
   * <p>{@link HoodieTableConfig#PRECOMBINE_FIELD} is only written at table creation, and only since
   * 0.8.0, so a table created before that records none even though its writer merges on one, and
   * everything that resolves ordering from the table config alone then falls back to no ordering.
   * Only a table that records none is filled in, and only from this upgrade. A top level field is
   * recorded once the schema has it, which is what keeps out the "ts" default that every write
   * config materializes; a field nested under dot notation is recorded as configured, since a
   * default is never nested.
   */
  private static Option<String> getPreCombineFieldToPersist(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    if (StringUtils.nonEmpty(metaClient.getTableConfig().getPreCombineField())) {
      // the table already records one, and the upgrade only fills in a missing ordering field
      return Option.empty();
    }
    String preCombineField = config.getPreCombineField();
    if (StringUtils.isNullOrEmpty(preCombineField)) {
      return Option.empty();
    }
    if (preCombineField.contains(NESTED_FIELD_SEPARATOR)) {
      // only an explicit config can name a nested field, so take the writer at its word
      return Option.of(preCombineField);
    }
    Option<Schema> schema = resolveSchema(config, metaClient);
    if (!schema.isPresent()) {
      LOG.warn("Skipping the ordering field {} while upgrading {} to table version two: no schema is available to "
          + "resolve it against", preCombineField, config.getBasePath());
      return Option.empty();
    }
    if (!AvroSchemaUtils.containsFieldInSchema(schema.get(), preCombineField)) {
      LOG.warn("Skipping the ordering field {} while upgrading {} to table version two: the schema has no such "
          + "top level field", preCombineField, config.getBasePath());
      return Option.empty();
    }
    return Option.of(preCombineField);
  }

  /**
   * The table's own schema, falling back to the writer's for a table with no committed data, and to
   * nothing if neither can be read.
   */
  private static Option<Schema> resolveSchema(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    try {
      Option<Schema> tableSchema = new TableSchemaResolver(metaClient).getTableAvroSchemaIfPresent(false);
      if (tableSchema.isPresent()) {
        return tableSchema;
      }
      String writeSchema = config.getWriteSchema();
      return StringUtils.isNullOrEmpty(writeSchema)
          ? Option.empty()
          : Option.of(new Schema.Parser().parse(writeSchema));
    } catch (Exception e) {
      // the upgrade gates every write, so a schema that cannot be read or parsed leaves the field
      // unrecorded rather than blocking the table
      LOG.warn("Failed to resolve the schema of " + config.getBasePath()
          + " while upgrading to table version two, leaving the ordering field unrecorded", e);
      return Option.empty();
    }
  }
}
