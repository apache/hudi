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

package org.apache.hudi.keygen.factory;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.ComplexAvroRecordKeyGenerator;
import org.apache.hudi.keygen.AutoRecordKeyGenerator;
import org.apache.hudi.keygen.RecordKeyGenerator;
import org.apache.hudi.keygen.SimpleAvroRecordKeyGenerator;

import java.util.List;

/**
 * Factory to instantiate RecordKeyGenerator.
 */
public class RecordKeyGeneratorFactory {

  public static RecordKeyGenerator getRecordKeyGenerator(TypedProperties config, List<String> recordKeyFields, boolean consistentLogicalTimestampEnabled,
                                                         List<String> partitionPathFields) {
    if (config.getBoolean(HoodieWriteConfig.AUTO_GENERATE_RECORD_KEYS.key(), HoodieWriteConfig.AUTO_GENERATE_RECORD_KEYS.defaultValue())) {
      return new AutoRecordKeyGenerator(config, partitionPathFields);
    } else if (recordKeyFields.size() == 1) {
      return new SimpleAvroRecordKeyGenerator(recordKeyFields.get(0), consistentLogicalTimestampEnabled);
    } else {
      return new ComplexAvroRecordKeyGenerator(recordKeyFields, consistentLogicalTimestampEnabled);
    }
  }
}
