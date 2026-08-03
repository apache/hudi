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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Version 10 writes native log files by default. Downgrading to version 9 requires
 * full compaction of native data/delete logs before the downgrade completes.
 *
 * <p>Version 10 also introduced {@code hoodie.meta.fields.mode}. Version 9 does not understand it,
 * so the property is dropped here while {@code hoodie.populate.meta.fields} is left exactly as it
 * stands — {@code ALL} and {@code NONE} tables round-trip unchanged because those are precisely the
 * two states the legacy boolean can express. Selective modes cannot be expressed in version 9, so
 * the table degrades to what its legacy boolean says (which is {@code false}, i.e. NONE) and we warn.
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

    // The warning is best-effort: dropping the property is what matters, and the helper is not
    // always available (some callers drive the change set directly).
    MetaFieldsMode metaFieldsMode = upgradeDowngradeHelper == null
        ? MetaFieldsMode.ALL
        : upgradeDowngradeHelper.getTable(config, context).getMetaClient().getTableConfig().getMetaFieldsMode();
    if (metaFieldsMode.isSelective()) {
      LOG.warn("Table is using {}={}, which table version 9 cannot express. The property is being "
              + "removed and the table will behave as {}=false (no meta columns) to version 9 readers. "
              + "Already-written files keep their populated meta columns, but incremental queries that "
              + "relied on {} will stop returning rows. Recreate the table if you need that behavior back.",
          HoodieTableConfig.META_FIELDS_MODE.key(), metaFieldsMode,
          HoodieTableConfig.POPULATE_META_FIELDS.key(), metaFieldsMode);
    }
    // hoodie.populate.meta.fields is deliberately left untouched: whatever the table recorded before
    // the downgrade stays, so ALL and NONE tables are bit-identical afterwards.
    propertiesToDelete.add(HoodieTableConfig.META_FIELDS_MODE);

    return new UpgradeDowngrade.TableConfigChangeSet(
        Collections.emptyMap(),
        propertiesToDelete);
  }
}
