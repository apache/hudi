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

package org.apache.hudi.sink.utils;

import org.apache.hudi.adapter.TypeInformationAdapter;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.client.model.HoodieFlinkInternalRowSerializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.util.Objects;

/**
 * Type information class for {@link HoodieFlinkInternalRow}.
 */
public class HoodieFlinkInternalRowTypeInfo extends TypeInformationAdapter<HoodieFlinkInternalRow> {

  private static final long serialVersionUID = 1L;

  private final RowType rowType;

  public HoodieFlinkInternalRowTypeInfo(RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return HoodieFlinkInternalRow.ARITY;
  }

  /**
   * Used only in Flink `CompositeType`, not used in this type
   */
  @Override
  public int getTotalFields() {
    return HoodieFlinkInternalRow.ARITY;
  }

  @Override
  public Class<HoodieFlinkInternalRow> getTypeClass() {
    return HoodieFlinkInternalRow.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<HoodieFlinkInternalRow> createSerializer() {
    return new HoodieFlinkInternalRowSerializer(this.rowType);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "< RowType: " + rowType.toString() + " >";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof HoodieFlinkInternalRowTypeInfo) {
      HoodieFlinkInternalRowTypeInfo recordTypeInfo = (HoodieFlinkInternalRowTypeInfo) obj;
      return Objects.equals(rowType, recordTypeInfo.rowType);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowType);
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof HoodieFlinkInternalRowTypeInfo;
  }
}
