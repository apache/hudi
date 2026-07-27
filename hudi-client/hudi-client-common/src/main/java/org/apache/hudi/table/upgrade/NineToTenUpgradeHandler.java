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

import java.util.Collections;
import java.util.Map;

/**
 * Version 10 enables native log format by default for new writes. Existing version 9
 * inline log files remain readable by version 10 readers, so there is no table metadata
 * rewrite required for the upgrade.
 *
 * <p>Version 10 also introduced {@code hoodie.meta.fields.mode}. Version 9 tables predate it and
 * resolve to {@code ALL} / {@code NONE} from the deprecated {@code hoodie.populate.meta.fields}
 * boolean. The upgrade records that derived value explicitly so upgraded tables describe their
 * meta-field layout the same way newly created version 10 tables do, rather than depending on the
 * legacy fallback. Behavior is unchanged either way — this only makes the on-disk state explicit.
 */
public class NineToTenUpgradeHandler implements UpgradeHandler {

  @Override
  public UpgradeDowngrade.TableConfigChangeSet upgrade(
      HoodieWriteConfig config,
      HoodieEngineContext context,
      String instantTime,
      SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    HoodieTableConfig tableConfig =
        upgradeDowngradeHelper.getTable(config, context).getMetaClient().getTableConfig();
    // Resolves from the legacy boolean for a version 9 table, since the mode property is absent.
    MetaFieldsMode metaFieldsMode = tableConfig.getMetaFieldsMode();
    Map<ConfigProperty, String> propertiesToUpdate = Collections.singletonMap(
        HoodieTableConfig.META_FIELDS_MODE, metaFieldsMode.name());
    return new UpgradeDowngrade.TableConfigChangeSet(propertiesToUpdate, Collections.emptySet());
  }
}
