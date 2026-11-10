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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Upgrade handle to assist in upgrading hoodie table from version 1 to 2.
 */
public class OneToTwoUpgradeHandler implements UpgradeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(OneToTwoUpgradeHandler.class);
  private static final String NESTED_FIELD_SEPARATOR = ".";

  @Override
  public UpgradeDowngrade.TableConfigChangeSet upgrade(
      HoodieWriteConfig config,
      HoodieEngineContext context,
      String instantTime,
      SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    Map<ConfigProperty, String> tablePropsToAdd = new Hashtable<>();
    tablePropsToAdd.put(HoodieTableConfig.PARTITION_FIELDS, upgradeDowngradeHelper.getPartitionColumns(config));
    tablePropsToAdd.put(HoodieTableConfig.RECORDKEY_FIELDS, config.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()));
    tablePropsToAdd.put(HoodieTableConfig.BASE_FILE_FORMAT, config.getString(HoodieTableConfig.BASE_FILE_FORMAT));
    HoodieTableMetaClient metaClient = upgradeDowngradeHelper.getTable(config, context).getMetaClient();
    getOrderingFieldsToRecord(config, metaClient)
        .ifPresent(orderingFields -> tablePropsToAdd.put(HoodieTableConfig.PRECOMBINE_FIELD, orderingFields));
    return new UpgradeDowngrade.TableConfigChangeSet(tablePropsToAdd, Collections.emptySet());
  }

  /**
   * Returns the ordering fields to record in {@code hoodie.properties}, if they can be established.
   *
   * <p>The ordering field is only written at table creation, and only since 0.8.0, so a table
   * created before that records none even though its writer merges on one, and everything that
   * resolves ordering from the table config alone then falls back to no ordering. Only a table that
   * records none is filled in, and only from this upgrade. A top level field is recorded once the
   * schema has it, which is what keeps out a materialized default; a field nested under dot
   * notation is recorded as configured, since a default is never nested. They are recorded under
   * the deprecated {@link HoodieTableConfig#PRECOMBINE_FIELD}, the key a table at this version is
   * expected to carry and the one a 0.x reader understands; {@code EightToNineUpgradeHandler}
   * migrates it to {@link HoodieTableConfig#ORDERING_FIELDS} in turn.
   */
  private static Option<String> getOrderingFieldsToRecord(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    if (!metaClient.getTableConfig().getOrderingFields().isEmpty()) {
      // the table already records them, and the upgrade only fills in missing ordering fields
      return Option.empty();
    }
    List<String> orderingFields = config.getPreCombineFields();
    if (orderingFields.isEmpty()) {
      return Option.empty();
    }
    // only an explicit config can name a nested field, so take the writer at its word for those
    List<String> topLevelFields = orderingFields.stream()
        .filter(field -> !field.contains(NESTED_FIELD_SEPARATOR))
        .collect(Collectors.toList());
    if (topLevelFields.isEmpty()) {
      return Option.of(String.join(",", orderingFields));
    }
    Option<HoodieSchema> schema = resolveSchema(config, metaClient);
    if (!schema.isPresent()) {
      LOG.warn("Skipping the ordering fields {} while upgrading {} to table version two: no schema is available to "
          + "resolve them against", orderingFields, config.getBasePath());
      return Option.empty();
    }
    if (!topLevelFields.stream().allMatch(field -> schema.get().getField(field).isPresent())) {
      LOG.warn("Skipping the ordering fields {} while upgrading {} to table version two: the schema does not have "
          + "all of them as top level fields", orderingFields, config.getBasePath());
      return Option.empty();
    }
    return Option.of(String.join(",", orderingFields));
  }

  /**
   * The table's own schema, falling back to the writer's for a table with no committed data, and to
   * nothing if neither can be read.
   */
  private static Option<HoodieSchema> resolveSchema(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    try {
      Option<HoodieSchema> tableSchema = new TableSchemaResolver(metaClient).getTableSchemaIfPresent(false);
      if (tableSchema.isPresent()) {
        return tableSchema;
      }
      String writeSchema = config.getWriteSchema();
      return StringUtils.isNullOrEmpty(writeSchema)
          ? Option.empty()
          : Option.of(HoodieSchema.parse(writeSchema));
    } catch (Exception e) {
      // the upgrade gates every write, so a schema that cannot be read or parsed leaves the fields
      // unrecorded rather than blocking the table
      LOG.warn("Failed to resolve the schema of " + config.getBasePath()
          + " while upgrading to table version two, leaving the ordering fields unrecorded", e);
      return Option.empty();
    }
  }
}
