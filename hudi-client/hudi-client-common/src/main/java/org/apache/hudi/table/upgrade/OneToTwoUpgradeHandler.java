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
   * <p>{@link HoodieTableConfig#PRECOMBINE_FIELD} is only written at table creation, and only
   * started being written in 0.8.0, so a table created before that carries no ordering field even
   * though its writer merges on one. Everything that resolves ordering from the table config alone
   * - Spark SQL DML, the merge-on-read snapshot merge, Flink, and the ordering fields of later
   * table versions - then silently falls back to no ordering at all.
   *
   * <p>The value comes from the write config, but is only recorded once it resolves against the
   * schema. {@link HoodieWriteConfig#PRECOMBINE_FIELD_NAME} carries a default of "ts" that
   * every write config materializes whether or not the user asked for it; recording that default on
   * a table that has no such field would leave behind an ordering field no reader can resolve, and
   * would make any later writer that configures a real ordering field fail table config validation
   * with a config conflict. A field that cannot be resolved, including one nested under dot
   * notation, is left unrecorded rather than recorded unverified.
   */
  private static Option<String> getPreCombineFieldToPersist(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    String preCombineField = config.getPreCombineField();
    if (StringUtils.isNullOrEmpty(preCombineField)) {
      return Option.empty();
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
   * The schema to resolve the ordering field against: the table's own schema, falling back to the
   * schema the writer is about to write with for a table that has not committed one yet.
   */
  private static Option<Schema> resolveSchema(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    Option<Schema> tableSchema = new TableSchemaResolver(metaClient).getTableAvroSchemaIfPresent(false);
    if (tableSchema.isPresent()) {
      return tableSchema;
    }
    String writeSchema = config.getWriteSchema();
    return StringUtils.isNullOrEmpty(writeSchema)
        ? Option.empty()
        : Option.of(new Schema.Parser().parse(writeSchema));
  }
}
