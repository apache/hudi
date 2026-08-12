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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;

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
 * <p>Version 10 also introduced {@code hoodie.meta.fields.mode}. {@code hoodie.populate.meta.fields}
 * is always written back from it ({@code ALL -> true}, every other mode {@code -> false}), mirroring
 * how {@link NineToEightDowngradeHandler} restores {@code hoodie.table.payload.class}. That restate
 * is load-bearing: {@code POPULATE_META_FIELDS} defaults to {@code true}, so a table carrying only
 * the mode would otherwise downgrade to {@code ALL} and claim {@code _hoodie_record_key} is
 * populated on files where it is null.
 *
 * <p>What happens to the mode itself depends on whether the legacy boolean can express it:
 *
 * <ul>
 *   <li>{@link MetaFieldsMode#ALL} / {@link MetaFieldsMode#NONE} — dropped. These are exactly the
 *       two states the boolean expresses, so the mode carries nothing the downgraded table lacks.</li>
 *   <li>A selective mode, restated by the writer — <b>retained</b>. Restating it is the operator
 *       asserting that every reader of this table honors the mode rather than the boolean alone.
 *       Keeping it is also what makes the round trip lossless: a later re-upgrade finds the mode
 *       intact rather than deriving {@code NONE} from the boolean.</li>
 *   <li>A selective mode, not restated (or restated as a different value) — <b>rejected</b>.
 *       Dropping it would collapse the table to {@code NONE} irreversibly, and that is not a call to
 *       make on the operator's behalf.</li>
 * </ul>
 *
 * <p>Retaining the mode on a version 9 table is safe mechanically: the property carries no
 * {@code sinceVersion}, so {@code dropInvalidConfigs} does not strip it on load.
 */
public class TenToNineDowngradeHandler implements DowngradeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TenToNineDowngradeHandler.class);

  /**
   * The meta-fields mode the writer explicitly asked for, if any.
   *
   * <p>Empty when the property is absent or blank -- which is the ordinary case, since almost no
   * caller restates meta-field settings. Restating it during a downgrade is therefore a deliberate
   * signal rather than something that happens by accident, which is what makes it usable as consent.
   */
  private static Option<MetaFieldsMode> statedMetaFieldsMode(HoodieWriteConfig config) {
    if (config == null || !config.contains(HoodieTableConfig.META_FIELDS_MODE)) {
      return Option.empty();
    }
    String raw = config.getString(HoodieTableConfig.META_FIELDS_MODE);
    return StringUtils.isNullOrEmpty(raw) ? Option.empty() : Option.of(MetaFieldsMode.parse(raw));
  }

  @Override
  public UpgradeDowngrade.TableConfigChangeSet downgrade(
      HoodieWriteConfig config,
      HoodieEngineContext context,
      String instantTime,
      SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    Set<ConfigProperty> propertiesToDelete = new HashSet<>();
    propertiesToDelete.add(HoodieTableConfig.TABLE_STORAGE_LAYOUT);

    Map<ConfigProperty, String> propertiesToUpdate = new HashMap<>();
    if (upgradeDowngradeHelper != null) {
      MetaFieldsMode metaFieldsMode =
          upgradeDowngradeHelper.getTable(config, context).getMetaClient().getTableConfig().getMetaFieldsMode();

      // Always restate the legacy boolean from the mode. Version 9 readers understand only that
      // property, and without it POPULATE_META_FIELDS falls back to its `true` default -- i.e. the
      // table silently downgrades to ALL and claims meta columns it does not have. For ALL / NONE
      // this restates what was already there; for a selective mode it writes `false`, so a reader
      // that does not honor the mode under-claims rather than over-claims.
      propertiesToUpdate.put(HoodieTableConfig.POPULATE_META_FIELDS,
          String.valueOf(metaFieldsMode.toLegacyPopulateMetaFields()));

      if (!metaFieldsMode.isSelective()) {
        // ALL and NONE are exactly what the boolean can express, so the mode carries no information
        // the downgraded table lacks. Drop it.
        propertiesToDelete.add(HoodieTableConfig.META_FIELDS_MODE);
      } else if (statedMetaFieldsMode(config).map(metaFieldsMode::equals).orElse(false)) {
        // Selective mode, and the writer restated it. That restatement is the operator asserting
        // "I know this table is selective and every reader of it honors the mode" -- so the mode is
        // retained on the downgraded table rather than dropped. Two consequences they are taking on:
        // the property outlives the version that formally understands it (harmless -- it carries no
        // sinceVersion, so it is not stripped on load), and a reader that ignores it sees
        // populate.meta.fields=false and treats the table as NONE.
        //
        // Keeping it is also what makes the round trip lossless: a later re-upgrade finds the mode
        // intact instead of deriving NONE from the boolean.
        LOG.warn("Downgrading a table on {}={} to table version 9. The mode is being retained on the "
                + "downgraded table because it was explicitly restated on the writer. Version 9 does "
                + "not formally understand it, so every reader of this table must honor {} rather "
                + "than relying on {} alone -- which now reads false.",
            HoodieTableConfig.META_FIELDS_MODE.key(), metaFieldsMode,
            HoodieTableConfig.META_FIELDS_MODE.key(),
            HoodieTableConfig.POPULATE_META_FIELDS.key());
      } else {
        // Selective mode and the writer did not restate it (or restated a different one). Dropping
        // the mode here would silently collapse the table to NONE and could not be undone: a
        // re-upgrade derives the mode from the boolean, which by then reads false. Rather than make
        // that call on the operator's behalf, require them to say so explicitly.
        throw new HoodieUpgradeDowngradeException(String.format(
            "Cannot downgrade to table version 9: %s=%s populates only some meta columns, which "
                + "version 9 cannot express. Dropping it would collapse the table to NONE and could "
                + "not be reversed by upgrading again. Set %s=%s on the writer to confirm you want "
                + "the mode retained on the downgraded table -- every reader must then honor it "
                + "rather than %s, which will read false. Alternatively rewrite the table as %s or "
                + "%s first.",
            HoodieTableConfig.META_FIELDS_MODE.key(), metaFieldsMode,
            HoodieTableConfig.META_FIELDS_MODE.key(), metaFieldsMode,
            HoodieTableConfig.POPULATE_META_FIELDS.key(),
            MetaFieldsMode.ALL, MetaFieldsMode.NONE));
      }
    } else {
      // No helper means the table config is unreachable, so the mode cannot be read. Deleting the
      // property is still correct (version 9 cannot interpret it), but the legacy boolean is left
      // alone rather than guessed at -- writing a derived value from an assumed mode is how a table
      // would end up claiming meta columns it does not have.
      propertiesToDelete.add(HoodieTableConfig.META_FIELDS_MODE);
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
