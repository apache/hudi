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

package org.apache.hudi.utilities.util;

/**
 * The three shapes a file-level disagreement can take, collapsed from {@link MismatchPattern}'s
 * nine diff patterns by the side that holds the file.
 *
 * <p>The finer grain is not lost: {@link FileDiffRecord#getSourcePattern()} carries it for a human
 * reader. Nothing switches on it, because the adjudication asks only which side has the file.
 */
public enum DiffPattern {
  /** Storage has the file, the metadata table does not. */
  FS_EXCLUSIVE,
  /** The metadata table has the file, storage does not. */
  MDT_EXCLUSIVE,
  /** Both sides hold the file and disagree about it. */
  BOTH_DISAGREE;

  public static DiffPattern of(MismatchSide side) {
    switch (side) {
      case FS:
        return FS_EXCLUSIVE;
      case MDT:
        return MDT_EXCLUSIVE;
      case BOTH:
        return BOTH_DISAGREE;
      default:
        throw new IllegalArgumentException("Unknown mismatch side: " + side);
    }
  }
}
