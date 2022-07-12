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
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;
import static org.apache.hudi.table.catalog.CatalogOptions.CATALOG_PATH;

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

    if (helper.getOptions().get(HoodieCatalogFactoryOptions.MODE).equalsIgnoreCase("hms")) {
      return new HoodieHiveCatalog(
          context.getName(),
          helper.getOptions().get(HoodieCatalogFactoryOptions.DEFAULT_DATABASE),
          helper.getOptions().get(HoodieCatalogFactoryOptions.HIVE_CONF_DIR),
          helper.getOptions().get(HoodieCatalogFactoryOptions.INIT_FS_TABLE));
    } else if (helper.getOptions().get(HoodieCatalogFactoryOptions.MODE).equalsIgnoreCase("dfs")) {
      return new HoodieCatalog(
          context.getName(),
          (Configuration) helper.getOptions());
    } else {
      throw new HoodieCatalogException("hoodie catalog supports only the hms and dfs modes.");
    }
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(HoodieCatalogFactoryOptions.DEFAULT_DATABASE);
    options.add(PROPERTY_VERSION);
    options.add(HoodieCatalogFactoryOptions.HIVE_CONF_DIR);
    options.add(HoodieCatalogFactoryOptions.MODE);
    options.add(CATALOG_PATH);
    options.add(HoodieCatalogFactoryOptions.INIT_FS_TABLE);
    return options;
  }
}
