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

package org.apache.hudi.common.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Describes the physical field layout of an incoming columnar record when an upstream
 * reader (e.g. Hive's Parquet reader) has compacted nested struct projection — that is,
 * dropped non-selected sub-fields and shifted the survivors into low slots.
 *
 * <p>The canonical (Hudi-side) schema declares the full ordered field list. When the
 * upstream reader projects only a subset of a struct, the resulting array's slots no
 * longer correspond to canonical positions. This mask captures, per record level, the
 * physical layout, so position-based access can be remapped.
 *
 * <p>A mask carries two pieces of information for the level it describes:
 * <ol>
 *   <li>Whether THIS level is canonical-shaped or compacted. Hive's Parquet reader pads
 *       non-projected top-level columns with nulls (canonical), but compacts non-projected
 *       sub-fields of struct columns (compacted).
 *   <li>Optional per-child masks. A canonical level can still descend into a child whose
 *       sub-record is compacted — for example, the top-level row is canonical but the
 *       blob_data sub-record's interior is compacted by Hive's nested-projection pushdown.
 * </ol>
 *
 * <p>{@link #all()} is the no-op identity used everywhere outside the projected-record
 * path. {@link #canonicalWith(Map)} preserves canonical positions at this level while
 * supplying compacted descent for specific children.
 */
public final class HoodieProjectionMask {

  private static final HoodieProjectionMask ALL = new HoodieProjectionMask(false, Collections.emptyMap(), Collections.emptyMap());

  // True when THIS level's ArrayWritable is compacted to only the projected fields.
  // False when this level is canonical-shaped (full positions per schema, with nulls for unprojected).
  private final boolean compactedAtThisLevel;
  // Field name -> position in the projected ArrayWritable. Only meaningful when compactedAtThisLevel.
  private final Map<String, Integer> physicalIndex;
  // Per-child mask used during descent. May be non-empty even when this level is canonical.
  private final Map<String, HoodieProjectionMask> childMasks;

  private HoodieProjectionMask(boolean compactedAtThisLevel, Map<String, Integer> physicalIndex, Map<String, HoodieProjectionMask> childMasks) {
    this.compactedAtThisLevel = compactedAtThisLevel;
    this.physicalIndex = physicalIndex;
    this.childMasks = childMasks;
  }

  public static HoodieProjectionMask all() {
    return ALL;
  }

  /**
   * Mask whose THIS level is canonical-shaped but whose listed children carry their own
   * (typically compacted) descent masks. Use this at boundaries where the outer record
   * is full but specific sub-records have been compacted by the reader (the typical
   * Hive nested-projection-on-BLOB shape).
   */
  public static HoodieProjectionMask canonicalWith(Map<String, HoodieProjectionMask> childMasks) {
    if (childMasks == null || childMasks.isEmpty()) {
      return ALL;
    }
    return new HoodieProjectionMask(false, Collections.emptyMap(), Collections.unmodifiableMap(new LinkedHashMap<>(childMasks)));
  }

  /**
   * True when no remapping or descent override applies — the rewrite can use canonical
   * positions everywhere below this point.
   */
  public boolean isAll() {
    return !compactedAtThisLevel && childMasks.isEmpty();
  }

  /**
   * True when the level this mask describes is canonical-shaped (positions match the
   * schema). Children may still carry compacted sub-masks.
   */
  public boolean isCanonicalAtThisLevel() {
    return !compactedAtThisLevel;
  }

  /**
   * Position of {@code fieldName} in the compacted ArrayWritable at this level. Empty
   * when this level is canonical or the field was not projected.
   */
  public OptionalInt physicalIndexOf(String fieldName) {
    if (!compactedAtThisLevel) {
      return OptionalInt.empty();
    }
    Integer idx = physicalIndex.get(fieldName);
    return idx == null ? OptionalInt.empty() : OptionalInt.of(idx);
  }

  /**
   * Field names of this compacted level, in physical-position order. Empty when the
   * level is canonical.
   */
  public List<String> physicalOrder() {
    if (!compactedAtThisLevel) {
      return Collections.emptyList();
    }
    return new ArrayList<>(physicalIndex.keySet());
  }

  /**
   * Mask to use when recursing into {@code fieldName}'s sub-record. Falls back to
   * {@link #all()} when no override is registered for this child.
   */
  public HoodieProjectionMask childOrAll(String fieldName) {
    HoodieProjectionMask child = childMasks.get(fieldName);
    return child == null ? ALL : child;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds a mask describing a compacted level. Insertion order defines the physical
   * field order in the projected ArrayWritable.
   */
  public static final class Builder {
    private final LinkedHashMap<String, HoodieProjectionMask> children = new LinkedHashMap<>();

    public Builder field(String name) {
      return field(name, ALL);
    }

    public Builder field(String name, HoodieProjectionMask child) {
      children.put(name, Objects.requireNonNull(child, "child mask"));
      return this;
    }

    public HoodieProjectionMask build() {
      if (children.isEmpty()) {
        return ALL;
      }
      LinkedHashMap<String, HoodieProjectionMask> childrenCopy = new LinkedHashMap<>(children);
      LinkedHashMap<String, Integer> index = new LinkedHashMap<>(children.size());
      int i = 0;
      for (String name : childrenCopy.keySet()) {
        index.put(name, i++);
      }
      return new HoodieProjectionMask(
          true,
          Collections.unmodifiableMap(index),
          Collections.unmodifiableMap(childrenCopy));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HoodieProjectionMask)) {
      return false;
    }
    HoodieProjectionMask other = (HoodieProjectionMask) o;
    return compactedAtThisLevel == other.compactedAtThisLevel
        && physicalIndex.equals(other.physicalIndex)
        && childMasks.equals(other.childMasks);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compactedAtThisLevel, physicalIndex, childMasks);
  }

  @Override
  public String toString() {
    if (isAll()) {
      return "HoodieProjectionMask{ALL}";
    }
    return "HoodieProjectionMask{compacted=" + compactedAtThisLevel + ",children=" + childMasks + "}";
  }
}
