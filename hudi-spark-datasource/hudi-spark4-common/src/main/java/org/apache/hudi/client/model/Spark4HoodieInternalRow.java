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

package org.apache.hudi.client.model;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

public class Spark4HoodieInternalRow extends HoodieInternalRow {

  public Spark4HoodieInternalRow(UTF8String[] metaFields,
      InternalRow sourceRow,
      boolean sourceContainsMetaFields) {
    super(metaFields, sourceRow, sourceContainsMetaFields);
  }

  @Override
  public VariantVal getVariant(int ordinal) {
    ruleOutMetaFieldsAccess(ordinal, VariantVal.class);
    return sourceRow.getVariant(rebaseOrdinal(ordinal));
  }

  @Override
  public InternalRow copy() {
    UTF8String[] copyMetaFields = new UTF8String[metaFields.length];
    for (int i = 0; i < metaFields.length; i++) {
      copyMetaFields[i] = metaFields[i] != null ? metaFields[i].copy() : null;
    }
    return new Spark4HoodieInternalRow(copyMetaFields, sourceRow == null ? null : sourceRow.copy(), sourceContainsMetaFields);
  }
}