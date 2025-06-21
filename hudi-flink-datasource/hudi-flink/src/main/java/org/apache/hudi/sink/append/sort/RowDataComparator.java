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

package org.apache.hudi.sink.append.sort;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hudi.sink.append.sort.FieldComparators.FieldComparator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * RowData comparator by in using sort keys. A key in the front of the keys will be used for sorting first.
 */
public class RowDataComparator implements RecordComparator {
  private final List<FieldComparator> fieldComparators;

  public RowDataComparator(RowType rowType, String sortKeys) {
    List<String> keyList = Arrays.stream(sortKeys.split(",")).collect(Collectors.toList());
    this.fieldComparators = FieldComparators.createFieldComparators(
        rowType, keyList);
  }

  @Override
  public int compare(RowData left, RowData right) {
    for (FieldComparator fieldComparator : fieldComparators) {
      int result = fieldComparator.compare(left, right);
      if (result == 0) {
        continue;
      }

      return result;
    }

    return 0;
  }
}
