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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Version 10 writes native log files by default. Downgrading to version 9 requires
 * full compaction of native data/delete logs before the downgrade completes. Native
 * CDC log conversion is intentionally unsupported.
 */
public class TenToNineDowngradeHandler implements DowngradeHandler {
  @Override
  public UpgradeDowngrade.TableConfigChangeSet downgrade(
      HoodieWriteConfig config,
      HoodieEngineContext context,
      String instantTime,
      SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    return new UpgradeDowngrade.TableConfigChangeSet(
        Collections.emptyMap(),
        Collections.singleton(HoodieTableConfig.TABLE_STORAGE_LAYOUT));
  }

  static void validateNoNativeCdcLogs(HoodieTableMetaClient metaClient) {
    findNativeCdcLogFile(metaClient).ifPresent(nativeCdcLogFile -> {
      throw new HoodieUpgradeDowngradeException(String.format(
          "Cannot downgrade table from version 10 to 9 because native CDC log file %s exists. "
              + "Native CDC log downgrade is unsupported.",
          nativeCdcLogFile));
    });
  }

  static Option<StoragePath> findNativeCdcLogFile(HoodieTableMetaClient metaClient) {
    AtomicReference<StoragePath> nativeCdcLogFile = new AtomicReference<>();
    try {
      FSUtils.processFiles(metaClient.getStorage(), metaClient.getBasePath().toString(), pathInfo -> {
        if (pathInfo.isFile() && FSUtils.isNativeCDCLogFile(pathInfo.getPath().getName())) {
          nativeCdcLogFile.set(pathInfo.getPath());
          return false;
        }
        return true;
      }, true);
    } catch (IOException e) {
      throw new HoodieUpgradeDowngradeException(
          "Failed to scan table for native CDC log files before downgrading from version 10 to 9", e);
    } catch (HoodieException e) {
      if (nativeCdcLogFile.get() == null) {
        throw e;
      }
    }
    return nativeCdcLogFile.get() == null ? Option.empty() : Option.of(nativeCdcLogFile.get());
  }
}
