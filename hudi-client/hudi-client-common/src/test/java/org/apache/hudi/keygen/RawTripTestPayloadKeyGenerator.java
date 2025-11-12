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

package org.apache.hudi.keygen;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;

import org.apache.avro.generic.GenericRecord;

/**
 * Use exclusively for {@link RawTripTestPayload} in tests.
 */
public class RawTripTestPayloadKeyGenerator extends SimpleAvroKeyGenerator {
  public RawTripTestPayloadKeyGenerator(TypedProperties props) {
    super(props);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    String partitionPath = HoodieAvroUtils
        .getNestedFieldValAsString(record, getPartitionPathFields().get(0), true, isConsistentLogicalTimestampEnabled());
    return HoodieTestUtils.extractPartitionFromTimeField(partitionPath);
  }
}
