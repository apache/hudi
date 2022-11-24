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

package org.apache.hudi.table.format;

import org.apache.hudi.util.RowDataProjection;

import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * Decorates origin hoodie parquet reader with cast projection.
 */
public final class HoodieParquetEvolvedSplitReader implements HoodieParquetReader {
  private final HoodieParquetReader originReader;
  private final RowDataProjection castProjection;

  public HoodieParquetEvolvedSplitReader(HoodieParquetReader originReader, RowDataProjection castProjection) {
    this.originReader = originReader;
    this.castProjection = castProjection;
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return originReader.reachedEnd();
  }

  @Override
  public RowData nextRecord() {
    return castProjection.project(originReader.nextRecord());
  }

  @Override
  public void close() throws IOException {
    originReader.close();
  }
}
