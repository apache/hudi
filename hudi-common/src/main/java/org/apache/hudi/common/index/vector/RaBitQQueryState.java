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

import java.io.Serializable;

/** Query state reused while scoring RaBitQ candidates. */
public final class RaBitQQueryState implements Serializable {

  private static final long serialVersionUID = 1L;

  private final byte[] binaryCode;
  /** Rotation of the unit query, used by the asymmetric estimator. */
  private final float[] rotatedQuery;
  /** Query norm before normalization, used for metric reconstruction. */
  private final float queryNorm;
  /** Sum of {@link #rotatedQuery}, reused by multibit scoring. */
  private final float querySum;

  RaBitQQueryState(byte[] binaryCode, float[] rotatedQuery, float queryNorm) {
    this.binaryCode = binaryCode.clone();
    this.rotatedQuery = rotatedQuery.clone();
    this.queryNorm = queryNorm;
    float sum = 0f;
    for (float value : rotatedQuery) {
      sum += value;
    }
    this.querySum = sum;
  }

  public byte[] getBinaryCode() {
    return binaryCode.clone();
  }

  public float[] getRotatedQuery() {
    return rotatedQuery.clone();
  }

  public float getQueryNorm() {
    return queryNorm;
  }

  public float getQuerySum() {
    return querySum;
  }

  byte[] binaryCodeUnsafe() {
    return binaryCode;
  }

  float[] rotatedQueryUnsafe() {
    return rotatedQuery;
  }
}
