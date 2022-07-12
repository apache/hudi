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
import org.apache.flink.table.catalog.CommonCatalogOptions;

/** {@link ConfigOption}s for {@link HoodieHiveCatalog}. */
public class HoodieCatalogFactoryOptions {
  public static final String DEFAULT_DB = "default";
  public static final String HIVE_SITE_FILE = "hive-site.xml";

  public static final ConfigOption<String> DEFAULT_DATABASE =
      ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
          .stringType()
          .defaultValue(DEFAULT_DB);

  public static final ConfigOption<String> HIVE_CONF_DIR =
      ConfigOptions.key("hive-conf-dir").stringType().noDefaultValue();

  public static final ConfigOption<String> MODE =
      ConfigOptions.key("mode").stringType().defaultValue("dfs");

  public static final ConfigOption<Boolean> INIT_FS_TABLE =
      ConfigOptions.key("init.fs.table").booleanType().defaultValue(true);

  private HoodieCatalogFactoryOptions() {

  }
}
