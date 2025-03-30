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

import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.util.stream.IntStream;

/**
 * This class is responsible to project row as well as {@link RowDataProjection}.
 * In addition, fields are converted according to the CastMap.
 */
public final class RowDataCastProjection extends RowDataProjection {
  private static final long serialVersionUID = 1L;

  private final CastMap castMap;

  public RowDataCastProjection(LogicalType[] types, CastMap castMap) {
    super(types, IntStream.range(0, types.length).toArray());
    this.castMap = castMap;
  }

  @Override
  protected @Nullable Object getVal(int pos, @Nullable Object val) {
    if (val == null) {
      return null;
    }
    return castMap.castIfNeeded(pos, val);
  }
}
