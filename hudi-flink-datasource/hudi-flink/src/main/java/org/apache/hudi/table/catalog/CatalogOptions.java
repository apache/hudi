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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CommonCatalogOptions;

import java.util.Map;

/**
 * Hoodie catalog options.
 */
public class CatalogOptions {
  public static final String HIVE_SITE_FILE = "hive-site.xml";
  public static final String DEFAULT_DB = "default";

  public static final ConfigOption<String> CATALOG_PATH =
      ConfigOptions.key("catalog.path")
          .stringType()
          .noDefaultValue()
          .withDescription("Catalog base DFS path, used for inferring the sink table path. "
              + "The default strategy for a table path is: ${catalog.path}/${db_name}/${table_name}");

  public static final ConfigOption<String> DEFAULT_DATABASE =
      ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
          .stringType()
          .defaultValue("default");

  public static final ConfigOption<String> HIVE_CONF_DIR = ConfigOptions
      .key("hive.conf.dir")
      .stringType()
      .noDefaultValue();

  public static final ConfigOption<String> MODE = ConfigOptions
      .key("mode")
      .stringType()
      .defaultValue("dfs");

  public static final ConfigOption<Boolean> HIVE_IS_EXTERNAL = ConfigOptions
      .key("hive.is-external")
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether the table is external, default false");

  /**
   * Returns all the common table options that can be shared.
   *
   * @param catalogOptions The catalog options
   */
  public static Map<String, String> tableCommonOptions(Configuration catalogOptions) {
    Configuration copied = new Configuration(catalogOptions);
    copied.removeConfig(DEFAULT_DATABASE);
    copied.removeConfig(CATALOG_PATH);
    return copied.toMap();
  }
}
