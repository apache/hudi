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

package org.apache.hudi.source.enumerator;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import java.io.Serializable;
import java.util.Objects;

/***
 * The position of {@link HoodieContinuousSplitEnumerator}.
 */
public class HoodieEnumeratorPosition implements Serializable {
  private final Option<String> issuedInstant;
  private final Option<String> issuedOffset;

  static HoodieEnumeratorPosition empty() {
    return new HoodieEnumeratorPosition(Option.empty(), Option.empty());
  }

  static HoodieEnumeratorPosition of(String lastInstant, String lastInstantCompletionTime) {
    return new HoodieEnumeratorPosition(lastInstant, lastInstantCompletionTime);
  }

  static HoodieEnumeratorPosition of(Option<String> lastInstant, Option<String> lastInstantCompletionTime) {
    return new HoodieEnumeratorPosition(lastInstant, lastInstantCompletionTime);
  }

  private HoodieEnumeratorPosition(String issuedInstant, String issuedOffset) {
    this.issuedInstant = StringUtils.isNullOrEmpty(issuedInstant) ? Option.empty() : Option.of(issuedInstant);
    this.issuedOffset = StringUtils.isNullOrEmpty(issuedOffset) ? Option.empty() : Option.of(issuedOffset);
  }

  private HoodieEnumeratorPosition(Option<String> issuedInstant, Option<String> issuedOffset) {
    this.issuedInstant = issuedInstant;
    this.issuedOffset = issuedOffset;
  }

  public Option<String> issuedInstant() {
    return issuedInstant;
  }

  public Option<String> issuedOffset() {
    return issuedOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieEnumeratorPosition that = (HoodieEnumeratorPosition) o;
    return Objects.equals(issuedInstant, that.issuedInstant) && Objects.equals(issuedOffset, that.issuedOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(issuedInstant, issuedOffset);
  }

  @Override
  public String toString() {
    return "HoodieEnumeratorPosition{"
        + "issuedInstant=" + issuedInstant
        + ", issuedOffset=" + issuedOffset
        + '}';
  }
}
