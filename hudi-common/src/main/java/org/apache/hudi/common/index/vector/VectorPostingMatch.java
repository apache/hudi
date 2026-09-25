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

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;

public class VectorPostingMatch implements Serializable {
  private final String recordKey;
  private final int clusterId;
  private final int shardId;
  private final String fileGroupId;
  private final String partitionPath;
  private final String baseInstantTime;
  private final long rowPosition;
  private final byte[] binaryCode;
  private final byte[] extendedCode;
  private final Float scalar;
  private final Float additiveFactor;
  private final Float rescaleFactor;
  private final Float vectorNorm;
  private final boolean delta;
  private final boolean deleted;

  public VectorPostingMatch(String recordKey,
                      int clusterId,
                      int shardId,
                      String fileGroupId,
                      String partitionPath,
                      String baseInstantTime,
                      long rowPosition,
                      byte[] binaryCode,
                      Float scalar) {
    this(recordKey, clusterId, shardId, fileGroupId, partitionPath, baseInstantTime,
        rowPosition, binaryCode, null, scalar, null, null, null, false, false);
  }

  public VectorPostingMatch(String recordKey,
                      int clusterId,
                      int shardId,
                      String fileGroupId,
                      String partitionPath,
                      String baseInstantTime,
                      long rowPosition,
                      byte[] binaryCode,
                      byte[] extendedCode,
                      Float scalar,
                      Float additiveFactor,
                      Float rescaleFactor) {
    this(recordKey, clusterId, shardId, fileGroupId, partitionPath, baseInstantTime,
        rowPosition, binaryCode, extendedCode, scalar, additiveFactor, rescaleFactor, null, false, false);
  }

  public VectorPostingMatch(String recordKey,
                      int clusterId,
                      int shardId,
                      String fileGroupId,
                      String partitionPath,
                      String baseInstantTime,
                      long rowPosition,
                      byte[] binaryCode,
                      byte[] extendedCode,
                      Float scalar,
                      Float additiveFactor,
                      Float rescaleFactor,
                      Float vectorNorm) {
    this(recordKey, clusterId, shardId, fileGroupId, partitionPath, baseInstantTime,
        rowPosition, binaryCode, extendedCode, scalar, additiveFactor, rescaleFactor, vectorNorm, false, false);
  }

  protected VectorPostingMatch(String recordKey,
                       int clusterId,
                       int shardId,
                       String fileGroupId,
                       String partitionPath,
                       String baseInstantTime,
                       long rowPosition,
                       byte[] binaryCode,
                       byte[] extendedCode,
                       Float scalar,
                       Float additiveFactor,
                       Float rescaleFactor,
                       Float vectorNorm,
                       boolean delta,
                       boolean deleted) {
    this.recordKey = recordKey;
    this.clusterId = clusterId;
    this.shardId = shardId;
    this.fileGroupId = fileGroupId;
    this.partitionPath = partitionPath;
    this.baseInstantTime = baseInstantTime;
    this.rowPosition = rowPosition;
    this.binaryCode = binaryCode;
    this.extendedCode = extendedCode;
    this.scalar = scalar;
    this.additiveFactor = additiveFactor;
    this.rescaleFactor = rescaleFactor;
    this.vectorNorm = vectorNorm;
    this.delta = delta;
    this.deleted = deleted;
  }

  public static VectorPostingMatch delta(String recordKey,
                                   int clusterId,
                                   int shardId,
                                   String fileGroupId,
                                   String partitionPath,
                                   String baseInstantTime,
                                   long rowPosition,
                                   byte[] binaryCode,
                                   byte[] extendedCode,
                                   Float scalar,
                                   Float additiveFactor,
                                   Float rescaleFactor) {
    return new VectorPostingMatch(recordKey, clusterId, shardId, fileGroupId, partitionPath, baseInstantTime,
        rowPosition, binaryCode, extendedCode, scalar, additiveFactor, rescaleFactor, null, true, false);
  }

  public static VectorPostingMatch delta(String recordKey,
                                   int clusterId,
                                   int shardId,
                                   String fileGroupId,
                                   String partitionPath,
                                   String baseInstantTime,
                                   long rowPosition,
                                   byte[] binaryCode,
                                   byte[] extendedCode,
                                   Float scalar,
                                   Float additiveFactor,
                                   Float rescaleFactor,
                                   Float vectorNorm) {
    return new VectorPostingMatch(recordKey, clusterId, shardId, fileGroupId, partitionPath, baseInstantTime,
        rowPosition, binaryCode, extendedCode, scalar, additiveFactor, rescaleFactor, vectorNorm, true, false);
  }

  public static VectorPostingMatch tombstone(String recordKey, int[] keyComponents) {
    return new VectorPostingMatch(recordKey, keyComponents[0], keyComponents[1], null, null, null,
        -1L, null, null, null, null, null, null, true, true);
  }

  public String getRecordKey() {
    return recordKey;
  }

  public int getClusterId() {
    return clusterId;
  }

  public int getShardId() {
    return shardId;
  }

  public String getFileGroupId() {
    return fileGroupId;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getBaseInstantTime() {
    return baseInstantTime;
  }

  public long getRowPosition() {
    return rowPosition;
  }

  public byte[] getBinaryCode() {
    return binaryCode;
  }

  public byte[] getExtendedCode() {
    return extendedCode;
  }

  public Float getScalar() {
    return scalar;
  }

  public Float getAdditiveFactor() {
    return additiveFactor;
  }

  public Float getRescaleFactor() {
    return rescaleFactor;
  }

  public Float getVectorNorm() {
    return vectorNorm;
  }

  public boolean isDelta() {
    return delta;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public Option<HoodieRecordGlobalLocation> toLocation() {
    if (partitionPath == null || fileGroupId == null || baseInstantTime == null || rowPosition < 0) {
      return Option.empty();
    }
    return Option.of(new HoodieRecordGlobalLocation(partitionPath, baseInstantTime, fileGroupId, rowPosition));
  }
}

