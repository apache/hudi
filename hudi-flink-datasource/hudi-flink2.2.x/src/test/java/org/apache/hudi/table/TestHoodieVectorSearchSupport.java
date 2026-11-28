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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.VectorSearchTableSource.VectorSearchContext;
import org.apache.flink.table.connector.source.VectorSearchTableSource.VectorSearchRuntimeProvider;
import org.apache.flink.table.connector.source.search.VectorSearchFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.VectorSearchFunction;
import org.apache.flink.table.runtime.connector.source.VectorSearchRuntimeProviderContext;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for the vector search provider wiring in {@link HoodieVectorSearchSupport}. */
class TestHoodieVectorSearchSupport {

  private static final RowType TABLE_ROW_TYPE =
      (RowType)
          DataTypes.ROW(
                  DataTypes.FIELD("id", DataTypes.INT()),
                  DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())))
              .notNull()
              .getLogicalType();

  @Test
  void testProviderCreatesSyncFunction() {
    HoodieVectorSearchSupport support = support(true, false);
    VectorSearchContext context =
        new VectorSearchRuntimeProviderContext(new int[][] {{1}}, new Configuration());

    VectorSearchRuntimeProvider provider = support.getSearchRuntimeProvider(context);

    VectorSearchFunctionProvider functionProvider =
        assertInstanceOf(VectorSearchFunctionProvider.class, provider);
    VectorSearchFunction function = functionProvider.createVectorSearchFunction();
    assertInstanceOf(HoodieVectorSearchFunction.class, function);
  }

  @Test
  void testNonSnapshotReadIsRejected() {
    HoodieVectorSearchSupport support = support(false, false);
    VectorSearchContext context =
        new VectorSearchRuntimeProviderContext(new int[][] {{1}}, new Configuration());

    assertThrows(TableException.class, () -> support.getSearchRuntimeProvider(context));
  }

  @Test
  void testStreamingReadIsRejected() {
    HoodieVectorSearchSupport support = support(true, true);
    VectorSearchContext context =
        new VectorSearchRuntimeProviderContext(new int[][] {{1}}, new Configuration());

    assertThrows(TableException.class, () -> support.getSearchRuntimeProvider(context));
  }

  @Test
  void testMultipleSearchColumnsAreRejected() {
    HoodieVectorSearchSupport support = support(true, false);
    VectorSearchContext context =
        new VectorSearchRuntimeProviderContext(
            new int[][] {{1}, {0}}, new Configuration());

    assertThrows(TableException.class, () -> support.getSearchRuntimeProvider(context));
  }

  private static HoodieVectorSearchSupport support(boolean snapshotRead, boolean streamingRead) {
    return new HoodieVectorSearchSupport() {
      @Override
      public InputFormat<RowData, ?> getVectorSearchInputFormat() {
        return null;
      }

      @Override
      public Configuration getTableConf() {
        return new Configuration();
      }

      @Override
      public RowType getTableRowType() {
        return TABLE_ROW_TYPE;
      }

      @Override
      public boolean isSnapshotRead() {
        return snapshotRead;
      }

      @Override
      public boolean isStreamingRead() {
        return streamingRead;
      }

      @Override
      public DynamicTableSource copy() {
        return this;
      }

      @Override
      public String asSummaryString() {
        return "test-vector-search-source";
      }
    };
  }
}
