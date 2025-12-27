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

import org.apache.hudi.common.model.EmptyHoodieRecordPayload;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * NOTE: This payload is intended to be used in-memeory ONLY. DO NOT use this in any serializing
 * Used to store the data table partition for delete records. The hoodie record stores the recordkey and
 * partition path. However, the partition path stored will be the mdt partition path which is record-index.
 * We need to have the data table partition because the keys are not global. For inserts or updates, the
 * data table partition path will be in the metadata payload, but obviously the empty payload doesn't store
 * that info. Hence, this wrapper class
 */
@AllArgsConstructor
@Getter
public class EmptyHoodieRecordPayloadWithPartition extends EmptyHoodieRecordPayload {

  private final String partitionPath;
}
