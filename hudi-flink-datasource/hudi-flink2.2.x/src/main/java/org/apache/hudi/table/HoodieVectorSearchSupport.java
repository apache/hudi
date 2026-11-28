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

package org.apache.hudi.table;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.VectorSearchTableSource;
import org.apache.flink.table.connector.source.search.VectorSearchFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;

/**
 * Version-specific hook that exposes a Hudi table source as a Flink {@link VectorSearchTableSource}.
 *
 * <p>Flink only added {@code VectorSearchTableSource} in 2.2. The interface lives in the version
 * module so that the shared {@code hudi-flink} module can implement a marker with the same name on
 * older Flink versions without importing the 2.2 API.
 */
public interface HoodieVectorSearchSupport extends VectorSearchTableSource {

  InputFormat<RowData, ?> getVectorSearchInputFormat();

  Configuration getTableConf();

  RowType getTableRowType();

  boolean isSnapshotRead();

  boolean isStreamingRead();

  @Override
  default VectorSearchRuntimeProvider getSearchRuntimeProvider(VectorSearchContext context) {
    if (!isSnapshotRead()) {
      throw new TableException("VECTOR_SEARCH on a Hudi table requires query.type=snapshot");
    }
    if (isStreamingRead()) {
      throw new TableException(
          "VECTOR_SEARCH on a Hudi table does not support read.streaming.enabled=true");
    }
    int[][] searchColumns = context.getSearchColumns();
    if (searchColumns.length != 1 || searchColumns[0].length != 1) {
      throw new TableException(
          "Hudi vector search supports exactly one search column, but got "
              + Arrays.deepToString(searchColumns));
    }
    return VectorSearchFunctionProvider.of(
        new HoodieVectorSearchFunction(
            this::getVectorSearchInputFormat,
            getTableConf(),
            getTableRowType(),
            searchColumns[0][0]));
  }
}
