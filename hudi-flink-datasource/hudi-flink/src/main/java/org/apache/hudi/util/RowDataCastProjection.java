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

package org.apache.hudi.util;

import org.apache.hudi.table.format.CastMap;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * As well as {@link RowDataProjection} projects the row data.
 * In addition, fields are converted according to the CastMap.
 */
public final class RowDataCastProjection extends RowDataProjection {
  private static final long serialVersionUID = 1L;

  private final CastMap castMap;

  public RowDataCastProjection(LogicalType[] types, int[] positions, CastMap castMap) {
    super(types, positions);
    this.castMap = castMap;
  }

  @Override
  public RowData project(RowData rowData) {
    RowData.FieldGetter[] fields = fieldGetters();
    GenericRowData genericRowData = new GenericRowData(fields.length);
    for (int i = 0; i < fields.length; i++) {
      Object val = fields[i].getFieldOrNull(rowData);
      if (val != null) {
        val = castMap.castIfNeed(i, val);
      }
      genericRowData.setField(i, val);
    }
    return genericRowData;
  }
}
