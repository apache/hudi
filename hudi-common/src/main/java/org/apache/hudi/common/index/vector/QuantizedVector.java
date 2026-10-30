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

/** Encoded vector and its persisted RaBitQ scoring factors. */
public final class QuantizedVector implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Primary packed sign/MSB code. */
  final byte[] code;
  /** Optional packed lower-bit code planes for multibit RaBitQ. */
  final byte[] extendedCode;
  /** Original or residual vector norm. */
  final float scalar;
  /** Optional L2-estimator additive factor used by multibit MDT postings. */
  final Float additiveFactor;
  /** Optional L2-estimator rescale factor used by multibit MDT postings. */
  final Float rescaleFactor;
  /** Sign-only L2-estimator additive factor used by packed-block pass 1. */
  final Float additiveFactor1;
  /** Sign-only L2-estimator rescale factor used by packed-block pass 1. */
  final Float rescaleFactor1;
  /** Query-independent pass-1 distance error factor. */
  final Float error1;
  /** Optional raw vector norm for metric-neutral residual scoring. */
  final Float vectorNorm;
  /** Total RaBitQ bit width represented by this payload. */
  final int bits;

  public QuantizedVector(byte[] code, float scalar) {
    this(code, null, scalar, null, null, null, null, null, null, 1);
  }

  public QuantizedVector(byte[] code,
                         byte[] extendedCode,
                         float scalar,
                         Float additiveFactor,
                         Float rescaleFactor,
                         int bits) {
    this(code, extendedCode, scalar, additiveFactor, rescaleFactor, null, null, null, null, bits);
  }

  public QuantizedVector(byte[] code,
                         byte[] extendedCode,
                         float scalar,
                         Float additiveFactor,
                         Float rescaleFactor,
                         Float additiveFactor1,
                         Float rescaleFactor1,
                         Float error1,
                         int bits) {
    this(code, extendedCode, scalar, additiveFactor, rescaleFactor,
        additiveFactor1, rescaleFactor1, error1, null, bits);
  }

  public QuantizedVector(byte[] code,
                         byte[] extendedCode,
                         float scalar,
                         Float additiveFactor,
                         Float rescaleFactor,
                         Float additiveFactor1,
                         Float rescaleFactor1,
                         Float error1,
                         Float vectorNorm,
                         int bits) {
    if (code == null || code.length == 0) {
      throw new IllegalArgumentException("Primary code must not be empty");
    }
    if (bits < 1 || bits > 8) {
      throw new IllegalArgumentException("RaBitQ bits must be in [1, 8]: " + bits);
    }
    if (!Float.isFinite(scalar) || scalar < 0f) {
      throw new IllegalArgumentException("Scalar must be finite and non-negative: " + scalar);
    }
    if ((bits == 1 && extendedCode != null && extendedCode.length != 0)
        || (bits > 1 && (extendedCode == null || extendedCode.length == 0))) {
      throw new IllegalArgumentException("Extended code presence must match the RaBitQ bit width");
    }
    validateOptionalFactor(additiveFactor, "additiveFactor");
    validateNonNegativeFactor(rescaleFactor, "rescaleFactor");
    validateOptionalFactor(additiveFactor1, "additiveFactor1");
    validateNonNegativeFactor(rescaleFactor1, "rescaleFactor1");
    validateNonNegativeFactor(error1, "error1");
    validateNonNegativeFactor(vectorNorm, "vectorNorm");
    this.code = code.clone();
    this.extendedCode = extendedCode == null ? null : extendedCode.clone();
    this.scalar = scalar;
    this.additiveFactor = additiveFactor;
    this.rescaleFactor = rescaleFactor;
    this.additiveFactor1 = additiveFactor1;
    this.rescaleFactor1 = rescaleFactor1;
    this.error1 = error1;
    this.vectorNorm = vectorNorm;
    this.bits = bits;
  }

  public byte[] getCode() {
    return code.clone();
  }

  public byte[] getExtendedCode() {
    return extendedCode == null ? null : extendedCode.clone();
  }

  public float getScalar() {
    return scalar;
  }

  public Float getAdditiveFactor() {
    return additiveFactor;
  }

  public Float getRescaleFactor() {
    return rescaleFactor;
  }

  public Float getAdditiveFactor1() {
    return additiveFactor1;
  }

  public Float getRescaleFactor1() {
    return rescaleFactor1;
  }

  public Float getError1() {
    return error1;
  }

  public Float getVectorNorm() {
    return vectorNorm;
  }

  public int getBits() {
    return bits;
  }

  private static void validateOptionalFactor(Float value, String name) {
    if (value != null && !Float.isFinite(value)) {
      throw new IllegalArgumentException(name + " must be finite: " + value);
    }
  }

  private static void validateNonNegativeFactor(Float value, String name) {
    validateOptionalFactor(value, name);
    if (value != null && value < 0f) {
      throw new IllegalArgumentException(name + " must be non-negative: " + value);
    }
  }
}
