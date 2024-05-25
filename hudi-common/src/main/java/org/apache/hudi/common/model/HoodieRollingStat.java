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

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A model class defines hoodie rolling stat.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieRollingStat implements Serializable {

  private String fileId;
  private long inserts;
  private long upserts;
  private long deletes;
  // TODO
  @Nullable
  private long totalInputWriteBytesToDisk;
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

  public String getFileId() {
    return fileId;
  }

  public void setFileId(String fileId) {
    this.fileId = fileId;
  }

  public long getInserts() {
    return inserts;
  }

  public void setInserts(long inserts) {
    this.inserts = inserts;
  }

  public long getUpserts() {
    return upserts;
  }

  public void setUpserts(long upserts) {
    this.upserts = upserts;
  }

  public long getDeletes() {
    return deletes;
  }

  public void setDeletes(long deletes) {
    this.deletes = deletes;
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

  public long getTotalInputWriteBytesOnDisk() {
    return totalInputWriteBytesOnDisk;
  }
}
