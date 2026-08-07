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
 * the legacy boolean can express.
 *
 * <p>A selective mode needs no special handling either, because every selective mode already
 * persists {@code populate.meta.fields=false} — {@link MetaFieldsMode#toLegacyPopulateMetaFields()}
 * is true only for {@link MetaFieldsMode#ALL}, and both {@code TableBuilder} and the hudi-cli derive
 * the boolean from the mode rather than taking it from the caller. So dropping the mode and keeping
 * {@code false} is exactly what version 9 should see: version 9 has no code that reads
 * {@code _hoodie_commit_time} selectively, so presenting the table as having no meta columns is the
 * only honest reading. Already-written files keep their populated columns; only how the table
 * advertises itself changes.
 *
 * <p>The lossy part is that a re-upgrade cannot restore the mode: {@code NineToTenUpgradeHandler}
 * derives it from the boolean, which by then reads {@code false}, so the table comes back as
 * {@link MetaFieldsMode#NONE} and the hudi-cli cannot widen it back — recovering the mode means
 * recreating the table. That is safe, since the table under-claims rather than advertising columns
 * it lacks, but it is not reversible, so we warn rather than let it pass unremarked.
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
      // ALL. For ALL / NONE this restates what was already there; for a selective mode it restates
      // the `false` already on disk, since the boolean is always derived from the mode when written.
      propertiesToUpdate.put(HoodieTableConfig.POPULATE_META_FIELDS,
          String.valueOf(metaFieldsMode.toLegacyPopulateMetaFields()));

      if (metaFieldsMode.isSelective()) {
        LOG.warn("Downgrading a table on {}={} to table version 9. Version 9 cannot express a mode "
                + "that populates only some meta columns, so the table will present as {}=false, i.e. "
                + "as having no meta columns at all. Already-written files keep their populated "
                + "columns, but incremental queries relying on the mode will stop returning rows, and "
                + "upgrading again will NOT restore it — the mode is derived from {} on upgrade, which "
                + "now reads false, and widening it back is rejected. Recreating the table is the only "
                + "way to get {} back.",
            HoodieTableConfig.META_FIELDS_MODE.key(), metaFieldsMode,
            HoodieTableConfig.POPULATE_META_FIELDS.key(),
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
