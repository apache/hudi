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
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Version 10 writes native log files by default. Downgrading to version 9 requires
 * full compaction of native data/delete logs before the downgrade completes.
 *
 * <p>Version 10 also introduced {@code hoodie.meta.fields.mode}. Version 9 does not understand it,
 * so the property is dropped here and {@code hoodie.populate.meta.fields} is written back from it
 * ({@code ALL -> true}, every other mode {@code -> false}), mirroring how
 * {@link NineToEightDowngradeHandler} restores {@code hoodie.table.payload.class}. Writing the
 * derived boolean explicitly is what makes the round trip safe: {@code POPULATE_META_FIELDS}
 * defaults to {@code true}, so a table carrying only the mode would otherwise downgrade to
 * {@code ALL} and claim {@code _hoodie_record_key} is populated on files where it is null.
 *
 * <p>{@code ALL} and {@code NONE} round-trip losslessly, since those are precisely the two states
 * the legacy boolean can express. Selective modes cannot be, so such a table degrades to
 * {@code NONE} for version 9 readers and we warn — under-claiming rather than over-claiming, which
 * is the safe direction.
 */
public class TenToNineDowngradeHandler implements DowngradeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TenToNineDowngradeHandler.class);

  @Override
  public UpgradeDowngrade.TableConfigChangeSet downgrade(
      HoodieWriteConfig config,
      HoodieEngineContext context,
      String instantTime,
      SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    Set<ConfigProperty> propertiesToDelete = new HashSet<>();
    propertiesToDelete.add(HoodieTableConfig.TABLE_STORAGE_LAYOUT);
    propertiesToDelete.add(HoodieTableConfig.META_FIELDS_MODE);

    Map<ConfigProperty, String> propertiesToUpdate = new HashMap<>();
    if (upgradeDowngradeHelper != null) {
      MetaFieldsMode metaFieldsMode =
          upgradeDowngradeHelper.getTable(config, context).getMetaClient().getTableConfig().getMetaFieldsMode();
      // Write the legacy boolean back from the mode so version 9 readers, which only understand that
      // property, see the table's real meta-column layout. Without this the mode is deleted and
      // POPULATE_META_FIELDS falls back to its `true` default, i.e. the table silently downgrades to
      // ALL. For ALL / NONE tables this is a no-op that just restates what was already there.
      propertiesToUpdate.put(HoodieTableConfig.POPULATE_META_FIELDS,
          String.valueOf(metaFieldsMode.toLegacyPopulateMetaFields()));
      if (metaFieldsMode.isSelective()) {
        LOG.warn("Table is using {}={}, which table version 9 cannot express. The property is being "
                + "removed and {} is set to false, so the table presents as having no meta columns to "
                + "version 9 readers. Already-written files keep their populated meta columns, but "
                + "incremental queries that relied on {} will stop returning rows, and re-upgrading "
                + "will not restore the mode — it resolves to NONE and widening it back is rejected. "
                + "Recreate the table if you need that behavior back.",
            HoodieTableConfig.META_FIELDS_MODE.key(), metaFieldsMode,
            HoodieTableConfig.POPULATE_META_FIELDS.key(), metaFieldsMode);
      }
    }
    // No helper means the table config is unreachable, so the mode cannot be read. Deleting the
    // property is still correct (version 9 cannot interpret it), but the legacy boolean is left
    // alone rather than guessed at — writing a derived value from an assumed mode is how a table
    // would end up claiming meta columns it does not have.

    return new UpgradeDowngrade.TableConfigChangeSet(
        propertiesToUpdate,
        propertiesToDelete);
  }
}
