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

package org.apache.hudi.utilities.keygen;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.ComplexKeyGenerator;

import org.apache.avro.generic.GenericRecord;

/**
 * Complex key generator, which takes names of fields to be used for recordKey and relies on timestamps for
 * partitioning field.
 * <p>
 * This is suitable for IoT scenarios, which usually use deviceId and deviceTime to build combinedKey(recordKey)
 * and take deviceTime(which of course in timestamp format) as partitionPath.
 */
public class TimestampBasedComplexKeyGenerator extends ComplexKeyGenerator {

  private final TimestampProcessor timestampProcessor;

  public TimestampBasedComplexKeyGenerator(TypedProperties props) {
    super(props);
    timestampProcessor = new TimestampProcessor(props);
  }

  @Override
  protected String getPartitionPath(GenericRecord record) {
    if (partitionPathFields.isEmpty()) {
      throw new HoodieKeyException("Unable to find field names for partition path in cfg");
    }
    String partitionPathField = partitionPathFields.get(0);

    Object partitionVal = DataSourceUtils.getNestedFieldVal(record, partitionPathField, true);
    if (partitionVal == null) {
      partitionVal = 1L;
    }

    long timeMs = timestampProcessor.convertPartitionValToLong(partitionVal);
    String partitionPath = timestampProcessor.formatPartitionPath(timeMs);

    return hiveStylePartitioning ? partitionPathField + "=" + partitionPath
        : partitionPath;
  }

}
