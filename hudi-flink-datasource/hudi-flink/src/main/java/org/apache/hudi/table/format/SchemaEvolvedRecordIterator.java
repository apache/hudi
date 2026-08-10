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

import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.util.RowDataProjection;

import org.apache.flink.table.data.RowData;

/**
 * Decorates origin record iterator with cast projection.
 */
public final class SchemaEvolvedRecordIterator implements ClosableIterator<RowData> {
  private final ClosableIterator<RowData> nested;
  private final RowDataProjection castProjection;

  public SchemaEvolvedRecordIterator(ClosableIterator<RowData> nested, RowDataProjection castProjection) {
    this.nested = nested;
    this.castProjection = castProjection;
  }

  @Override
  public boolean hasNext() {
    return nested.hasNext();
  }

  @Override
  public RowData next() {
    return castProjection.project(nested.next());
  }

  @Override
  public void close() {
    nested.close();
  }
}
