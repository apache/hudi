/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.secondary.index.filter;

import org.apache.hudi.internal.schema.Types.Field;
import org.apache.hudi.secondary.index.IRowIdSet;
import org.apache.hudi.secondary.index.ISecondaryIndexReader;

import java.io.IOException;
import java.util.List;

public class TermListFilter extends IndexFilter {
  private final List<Object> values;

  public TermListFilter(ISecondaryIndexReader indexReader, Field field, List<Object> values) {
    super(indexReader, field);
    this.values = values;
  }

  @Override
  public IRowIdSet getRowIdSet() throws IOException {
    return indexReader.queryTermList(getField(), values);
  }

  @Override
  public String toString() {
    boolean first = true;
    StringBuilder builder = new StringBuilder();
    for (Object value : values) {
      if (first) {
        first = false;
      } else {
        builder.append(",");
      }

      builder.append(value);
    }
    return "in(" + getField().name() + ", list(" + builder.toString() + "))";
  }
}
