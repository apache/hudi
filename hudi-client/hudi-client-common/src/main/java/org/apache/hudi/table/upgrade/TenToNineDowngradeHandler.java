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
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;

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
 * the legacy boolean can express. A selective mode has no version 9 representation at all, so the
 * downgrade is rejected rather than silently degraded: collapsing it to {@code NONE} would leave a
 * table whose files still carry populated meta columns that nothing advertises, and re-upgrading
 * could not restore the mode ({@code NineToTenUpgradeHandler} derives it from the legacy boolean,
 * which by then reads {@code false}). Failing here keeps the lossy state unreachable instead of
 * documenting it — rewrite the table to {@code ALL} or {@code NONE} first if a downgrade is needed.
 */
public class TenToNineDowngradeHandler implements DowngradeHandler {

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
      // A selective mode cannot be expressed by the legacy boolean, so there is no honest value to
      // write back: `false` would under-claim a table whose files do carry commit times or file
      // names, and the mode is unrecoverable afterwards because re-upgrading derives it from that
      // same boolean. Reject instead of degrading, so the table stays in a state both versions agree
      // on.
      if (metaFieldsMode.isSelective()) {
        throw new HoodieUpgradeDowngradeException(String.format(
            "Cannot downgrade to table version 9: %s=%s has no version 9 representation. Version 9 "
                + "only understands %s, which cannot express a table that populates some meta columns "
                + "but not others. Rewrite the table with %s=%s or %s=%s before downgrading; "
                + "downgrading as-is would leave populated meta columns that nothing advertises, and "
                + "the mode could not be restored by upgrading again.",
            HoodieTableConfig.META_FIELDS_MODE.key(), metaFieldsMode,
            HoodieTableConfig.POPULATE_META_FIELDS.key(),
            HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.ALL,
            HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.NONE));
      }
      // Write the legacy boolean back from the mode so version 9 readers, which only understand that
      // property, see the table's real meta-column layout. Without this the mode is deleted and
      // POPULATE_META_FIELDS falls back to its `true` default, i.e. the table silently downgrades to
      // ALL. For ALL / NONE tables this is a no-op that just restates what was already there.
      propertiesToUpdate.put(HoodieTableConfig.POPULATE_META_FIELDS,
          String.valueOf(metaFieldsMode.toLegacyPopulateMetaFields()));
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
