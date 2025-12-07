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

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Class used to hold secondary index metadata stats. These stats are generated from
 * various write handles during write.
 */
@Getter
@Setter
public class SecondaryIndexStats implements Serializable {
  private static final long serialVersionUID = 1L;

  private String recordKey;
  private String secondaryKeyValue;
  private boolean isDeleted;

  public SecondaryIndexStats(String recordKey, String secondaryKeyValue, boolean isDeleted) {
    this.recordKey = recordKey;
    this.secondaryKeyValue = secondaryKeyValue;
    this.isDeleted = isDeleted;
  }
}
