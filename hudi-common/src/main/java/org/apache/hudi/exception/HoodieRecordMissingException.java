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

package org.apache.hudi.exception;

import org.apache.hudi.common.model.HoodieRecord;

/**
 * <p>
 * Exception throws when indexing fails to locate the hoodie record. HoodieRecord current location and partition path
 * does not match. This is an unrecoverable error
 * </p>
 */
public class HoodieRecordMissingException extends HoodieException {

  public HoodieRecordMissingException(HoodieRecord record) {
    super("Record " + record.getRecordKey() + " with partition path " + record.getPartitionPath()
        + " in current location " + record.getCurrentLocation() + " is not found in the partition");
  }
}
