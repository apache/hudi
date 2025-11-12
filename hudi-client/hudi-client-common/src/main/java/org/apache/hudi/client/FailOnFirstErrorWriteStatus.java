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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * This class can be used as WriteStatus when we want to fail fast and at the first available exception/error.
 */
public class FailOnFirstErrorWriteStatus extends WriteStatus {
  private static final Logger LOG = LoggerFactory.getLogger(FailOnFirstErrorWriteStatus.class);

  public FailOnFirstErrorWriteStatus(Boolean trackSuccessRecords, Double failureFraction) {
    super(trackSuccessRecords, failureFraction);
  }

  public FailOnFirstErrorWriteStatus(Boolean trackSuccessRecords, Double failureFraction, Boolean isMetadataTable) {
    super(trackSuccessRecords, failureFraction, isMetadataTable);
  }

  @Override
  public void markFailure(HoodieRecord record, Throwable t, Option<Map<String, String>> optionalRecordMetadata) {
    LOG.error(String.format("Error writing record %s with data %s and optionalRecordMetadata %s", record, record.getData(),
        optionalRecordMetadata.orElse(Collections.emptyMap())), t);
    throw new HoodieException("Error writing record " + record, t);
  }

  @Override
  public void markFailure(String recordKey, String partitionPath, Throwable t) {
    LOG.error(String.format("Error writing record %s and partition %s", recordKey, partitionPath), t);
    throw new HoodieException("Error writing record `" + recordKey + "` partitionPath `" + partitionPath + "`", t);
  }
}
