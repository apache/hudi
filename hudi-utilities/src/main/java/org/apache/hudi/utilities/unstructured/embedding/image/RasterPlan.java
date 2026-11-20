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

package org.apache.hudi.utilities.unstructured.embedding.image;

import javax.imageio.ImageTypeSpecifier;

import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.io.Serializable;

/**
 * How one image will be decoded, decided from its header BEFORE any pixel is decoded (the
 * destination raster is a pure function of the header dimensions, the subsampling and the destination type; predicted equals
 * allocated in every bounded run measured there). The rule: the subsampling
 * {@code s = max(1, floor(longSide / target))} grows by one until the predicted raster fits the budget.
 */
public final class RasterPlan implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int subsampling;
  private final int decodedWidth;
  private final int decodedHeight;
  private final long predictedBytes;

  RasterPlan(int subsampling, int decodedWidth, int decodedHeight, long predictedBytes) {
    this.subsampling = subsampling;
    this.decodedWidth = decodedWidth;
    this.decodedHeight = decodedHeight;
    this.predictedBytes = predictedBytes;
  }

  /**
   * @param destinationType the type the reader decodes into (its first image type)
   * @throws ImageRefusedException when no subsampling fits the raster budget
   */
  public static RasterPlan of(ImageHeader header, ImageTypeSpecifier destinationType, ImagePreprocessingSpec spec)
      throws ImageRefusedException {
    int width = header.getWidth();
    int height = header.getHeight();
    int longSide = Math.max(width, height);
    long bytesPerPixel = bytesPerPixelUpperBound(destinationType);
    int subsampling = Math.max(1, longSide / spec.longSide());
    while (predictedBytes(width, height, subsampling, bytesPerPixel) > spec.rasterBudgetBytes()) {
      if (subsampling >= longSide) {
        throw new ImageRefusedException(ImageRefusedException.REFUSED_BEFORE_DECODE + header + ": no subsampling fits the "
            + "raster budget of " + spec.rasterBudgetBytes() + " bytes at " + bytesPerPixel + " bytes per pixel");
      }
      subsampling++;
    }
    return new RasterPlan(subsampling, ceilDiv(width, subsampling), ceilDiv(height, subsampling),
        predictedBytes(width, height, subsampling, bytesPerPixel));
  }

  /**
   * Bytes per pixel of a raster of {@code type}: data elements per pixel times the element size. Exact for the interleaved,
   * banded and single-pixel-packed models readers use; an upper bound for multi-pixel-packed (1/2/4-bit) ones.
   */
  static long bytesPerPixelUpperBound(ImageTypeSpecifier type) {
    SampleModel model = type.getSampleModel(1, 1);
    return (long) model.getNumDataElements() * (DataBuffer.getDataTypeSize(model.getDataType()) / 8);
  }

  private static long predictedBytes(int width, int height, int subsampling, long bytesPerPixel) {
    return (long) ceilDiv(width, subsampling) * ceilDiv(height, subsampling) * bytesPerPixel;
  }

  private static int ceilDiv(int value, int divisor) {
    return (value + divisor - 1) / divisor;
  }

  public int subsampling() {
    return subsampling;
  }

  public int decodedWidth() {
    return decodedWidth;
  }

  public int decodedHeight() {
    return decodedHeight;
  }

  /** The destination raster bytes the decode may allocate; asserted after the decode. */
  public long predictedBytes() {
    return predictedBytes;
  }

  @Override
  public String toString() {
    return "s=" + subsampling + " -> " + decodedWidth + "x" + decodedHeight + " (" + predictedBytes + " B)";
  }
}
