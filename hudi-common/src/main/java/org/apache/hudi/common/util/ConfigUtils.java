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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.table.HoodieTableConfig;

import java.util.Properties;

public class ConfigUtils {

  /**
   * Get ordering field.
   */
  public static String getOrderingField(Properties properties) {
    String orderField = null;
    if (properties.containsKey(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY)) {
      orderField = properties.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY);
    } else if (properties.containsKey("hoodie.datasource.write.precombine.field")) {
      orderField = properties.getProperty("hoodie.datasource.write.precombine.field");
    } else if (properties.containsKey(HoodieTableConfig.PRECOMBINE_FIELD.key())) {
      orderField = properties.getProperty(HoodieTableConfig.PRECOMBINE_FIELD.key());
    }
    return orderField;
  }

  /**
   * Get payload class.
   */
  public static String getPayloadClass(Properties properties) {
    String payloadClass = null;
    if (properties.containsKey(HoodieTableConfig.PAYLOAD_CLASS_NAME.key())) {
      payloadClass = properties.getProperty(HoodieTableConfig.PAYLOAD_CLASS_NAME.key());
    } else if (properties.containsKey("hoodie.datasource.write.payload.class")) {
      payloadClass = properties.getProperty("hoodie.datasource.write.payload.class");
    }
    return payloadClass;
  }
}
