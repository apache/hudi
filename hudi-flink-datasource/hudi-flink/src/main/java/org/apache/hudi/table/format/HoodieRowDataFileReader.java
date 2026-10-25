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

package org.apache.hudi.table.format;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.source.ExpressionPredicates;

import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.List;

/**
 * A {@link HoodieFileReader} that reads {@link RowData}s and exposes a uniform, format-agnostic
 * entry point for getting a record iterator.
 */
public interface HoodieRowDataFileReader extends HoodieFileReader<RowData> {

  /**
   * Returns an iterator over {@link RowData}s for the given schemas.
   *
   * @param dataSchema            schema of the records stored in the file.
   * @param requiredSchema        schema containing the fields to project.
   * @param internalSchemaManager schema evolution manager; {@link InternalSchemaManager#DISABLED} to skip.
   * @param predicates            filters to push down to the reader.
   */
  ClosableIterator<RowData> getRowDataIterator(
      HoodieSchema dataSchema,
      HoodieSchema requiredSchema,
      InternalSchemaManager internalSchemaManager,
      List<ExpressionPredicates.Predicate> predicates) throws IOException;
}
