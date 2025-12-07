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

package org.apache.hudi.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A model class defines hoodie rolling stat.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieRollingStat implements Serializable {

  @Setter
  @Getter
  private String fileId;
  @Setter
  @Getter
  private long inserts;
  @Setter
  @Getter
  private long upserts;
  @Setter
  @Getter
  private long deletes;
  // TODO
  @Nullable
  private long totalInputWriteBytesToDisk;
  @Getter
  @Nullable
  private long totalInputWriteBytesOnDisk;

  public HoodieRollingStat() {
    // called by jackson json lib
  }

  public HoodieRollingStat(String fileId, long inserts, long upserts, long deletes, long totalInputWriteBytesOnDisk) {
    this.fileId = fileId;
    this.inserts = inserts;
    this.upserts = upserts;
    this.deletes = deletes;
    this.totalInputWriteBytesOnDisk = totalInputWriteBytesOnDisk;
  }

  public long addInserts(long inserts) {
    this.inserts += inserts;
    return this.inserts;
  }

  public long addUpserts(long upserts) {
    this.upserts += upserts;
    return this.upserts;
  }

  public long addDeletes(long deletes) {
    this.deletes += deletes;
    return this.deletes;
  }

}
