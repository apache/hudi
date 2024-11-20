/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.keygen.BaseKeyGenerator;

import java.util.Arrays;
import java.util.stream.Collectors;

public class HoodieTableConfigUtils {

  /**
   * This function returns the partition fields joined by BaseKeyGenerator.FIELD_SEPARATOR. It will also
   * include the key generator partition type with the field. The key generator partition type is used for
   * Custom Key Generator.
   */
  public static Option<String> getPartitionFieldPropForKeyGenerator(HoodieConfig config) {
    return Option.ofNullable(config.getString(HoodieTableConfig.PARTITION_FIELDS));
  }

  /**
   * This function returns the partition fields joined by BaseKeyGenerator.FIELD_SEPARATOR. It will
   * strip the partition key generator related info from the fields.
   */
  public static Option<String> getPartitionFieldProp(HoodieConfig config) {
    if (getTableVersion(config).greaterThan(HoodieTableVersion.SEVEN)) {
      // With table version eight, the table config org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS
      // stores the corresponding partition type as well. This partition type is useful for CustomKeyGenerator
      // and CustomAvroKeyGenerator.
      return getPartitionFields(config).map(fields -> String.join(BaseKeyGenerator.FIELD_SEPARATOR, fields));
    } else {
      return Option.ofNullable(config.getString(HoodieTableConfig.PARTITION_FIELDS));
    }
  }

  /**
   * This function returns the partition fields only. This method strips the key generator related
   * partition key types from the configured fields.
   */
  public static Option<String[]> getPartitionFields(HoodieConfig config) {
    if (HoodieConfig.contains(HoodieTableConfig.PARTITION_FIELDS, config)) {
      return Option.of(Arrays.stream(config.getString(HoodieTableConfig.PARTITION_FIELDS).split(","))
          .filter(p -> !p.isEmpty())
          .map(p -> getPartitionFieldWithoutKeyGenPartitionType(p, config))
          .collect(Collectors.toList()).toArray(new String[] {}));
    }
    return Option.empty();
  }

  /**
   * This function returns the partition fields only. The input partition field would contain partition
   * type corresponding to the custom key generator if table version is eight and if custom key
   * generator is configured. This function would strip the partition type and return the partition field.
   */
  public static String getPartitionFieldWithoutKeyGenPartitionType(String partitionField, HoodieConfig config) {
    return getTableVersion(config).greaterThan(HoodieTableVersion.SEVEN)
        ? partitionField.split(BaseKeyGenerator.CUSTOM_KEY_GENERATOR_SPLIT_REGEX)[0]
        : partitionField;
  }
  
  /**
   * This function returns the hoodie.table.version from hoodie.properties file.
   */
  public static HoodieTableVersion getTableVersion(HoodieConfig config) {
    return HoodieConfig.contains(HoodieTableConfig.VERSION, config)
        ? HoodieTableVersion.fromVersionCode(config.getInt(HoodieTableConfig.VERSION))
        : HoodieTableConfig.VERSION.defaultValue();
  }
}
