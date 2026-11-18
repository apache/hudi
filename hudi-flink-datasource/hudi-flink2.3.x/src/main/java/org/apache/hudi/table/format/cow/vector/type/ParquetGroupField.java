/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format.cow.vector.type;

import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Field that represent parquet's Group Field.
 *
 * <p>Note: Vendored from Apache Flink (FLINK-35702, {@code
 * org.apache.flink.formats.parquet.vector.type.ParquetGroupField}) with a Hudi-specific extension:
 * entries in the {@code children} list may be {@code null} to denote a Row child that is absent
 * from the parquet file but present in the requested logical schema (schema evolution). This
 * replaces Hudi's previous {@code EmptyColumnReader} branch for Row subtrees.
 */
public class ParquetGroupField extends ParquetField {

  private final List<ParquetField> children;

  public ParquetGroupField(
      LogicalType type,
      int repetitionLevel,
      int definitionLevel,
      boolean required,
      List<ParquetField> children) {
    super(type, repetitionLevel, definitionLevel, required);
    // Use a plain unmodifiable list (not ImmutableList) so that null entries are allowed for
    // schema-evolution missing children in ROW types.
    this.children =
        Collections.unmodifiableList(new ArrayList<>(requireNonNull(children, "children is null")));
  }

  /** Children of this group. Entries may be {@code null} for absent-in-file Row fields. */
  public List<ParquetField> getChildren() {
    return children;
  }
}
