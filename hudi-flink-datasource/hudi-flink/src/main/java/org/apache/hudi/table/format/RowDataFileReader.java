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

import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.source.ExpressionPredicates.Predicate;
import org.apache.hudi.storage.StoragePath;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Abstraction to assist in reading {@link RowData}s from log files and base files.
 */
public interface RowDataFileReader extends Serializable {

  /**
   * Read an individual parquet file and return iterator of RowData.
   *
   * @param fieldNames names for all fields
   * @param fieldTypes types for all fields
   * @param selectedFields positions for required fields
   * @param predicates pushed down predicates
   * @param path file path to read
   * @param start starting byte to start reading.
   * @param length bytes to read.
   * @return A closable iterator of RowDta
   */
  ClosableIterator<RowData> getRowDataIterator(
      List<String> fieldNames,
      List<DataType> fieldTypes,
      int[] selectedFields,
      List<Predicate> predicates,
      StoragePath path,
      long start,
      long length) throws IOException;
}
