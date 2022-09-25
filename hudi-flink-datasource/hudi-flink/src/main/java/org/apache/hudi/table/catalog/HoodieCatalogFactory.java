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

package org.apache.hudi.table.catalog;

import org.apache.hudi.exception.HoodieCatalogException;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * A catalog factory impl that creates {@link HoodieCatalog}.
 */
public class HoodieCatalogFactory implements CatalogFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieCatalogFactory.class);

  public static final String IDENTIFIER = "hudi";

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Catalog createCatalog(Context context) {
    final FactoryUtil.CatalogFactoryHelper helper =
        FactoryUtil.createCatalogFactoryHelper(this, context);
    helper.validate();
    String mode = helper.getOptions().get(CatalogOptions.MODE);
    switch (mode.toLowerCase(Locale.ROOT)) {
      case "hms":
        return new HoodieHiveCatalog(
            context.getName(),
            helper.getOptions().get(CatalogOptions.CATALOG_PATH),
            helper.getOptions().get(CatalogOptions.DEFAULT_DATABASE),
            helper.getOptions().get(CatalogOptions.HIVE_CONF_DIR));
      case "dfs":
        return new HoodieCatalog(
            context.getName(),
            (Configuration) helper.getOptions());
      default:
        throw new HoodieCatalogException(String.format("Invalid catalog mode: %s, supported modes: [hms, dfs].", mode));
    }
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return new HashSet<>(CatalogOptions.allOptions());
  }
}
