/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;

/**
 * Custom key generator for the Hoodie table metadata. The metadata table record payload
 * has an internal schema with a known key field HoodieMetadataPayload.SCHEMA_FIELD_ID_KEY.
 * With or without the virtual keys, getting the key from the metadata table record is always
 * via the above field and there is no real need for a key generator. But, when a write
 * client is instantiated for the metadata table, when virtual keys are enabled, and when
 * key generator class is not configured, the default SimpleKeyGenerator will be used.
 * To avoid using any other key generators for the metadata table which rely on certain
 * config properties, we need this custom key generator exclusively for the metadata table.
 */
public class HoodieTableMetadataKeyGenerator extends BaseKeyGenerator {

  public HoodieTableMetadataKeyGenerator(TypedProperties config) {
    super(config);
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return KeyGenUtils.getRecordKey(record, HoodieMetadataPayload.KEY_FIELD_NAME, isConsistentLogicalTimestampEnabled());
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return "";
  }
}
