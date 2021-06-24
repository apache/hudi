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

package org.apache.hudi.keygen.constant;

import org.apache.hudi.common.config.ConfigProperty;

public class KeyGeneratorOptions {

  public static final ConfigProperty<String> URL_ENCODE_PARTITIONING_OPT_KEY = ConfigProperty
      .key("hoodie.datasource.write.partitionpath.urlencode")
      .defaultValue("false")
      .withDocumentation("");

  public static final ConfigProperty<String> HIVE_STYLE_PARTITIONING_OPT_KEY = ConfigProperty
      .key("hoodie.datasource.write.hive_style_partitioning")
      .defaultValue("false")
      .withDocumentation("Flag to indicate whether to use Hive style partitioning.\n"
          + "If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.\n"
          + "By default false (the names of partition folders are only partition values)");

  public static final ConfigProperty<String> RECORDKEY_FIELD_OPT_KEY = ConfigProperty
      .key("hoodie.datasource.write.recordkey.field")
      .defaultValue("uuid")
      .withDocumentation("Record key field. Value to be used as the `recordKey` component of `HoodieKey`.\n"
          + "Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using\n"
          + "the dot notation eg: `a.b.c`");

  public static final ConfigProperty<String> PARTITIONPATH_FIELD_OPT_KEY = ConfigProperty
      .key("hoodie.datasource.write.partitionpath.field")
      .defaultValue("partitionpath")
      .withDocumentation("Partition path field. Value to be used at the partitionPath component of HoodieKey. "
          + "Actual value ontained by invoking .toString()");
}

