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

package org.apache.hudi.client.model

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

class Spark40HoodieInternalRow(
                                metaFields: Array[UTF8String],
                                sourceRow: InternalRow,
                                sourceContainsMetaFields: Boolean)
  extends HoodieInternalRow(metaFields, sourceRow, sourceContainsMetaFields) {

  override def getVariant(ordinal: Int): VariantVal = {
    ruleOutMetaFieldsAccess(ordinal, classOf[VariantVal])
    sourceRow.getVariant(rebaseOrdinal(ordinal))
  }

  override def copy(): InternalRow = {
    val copyMetaFields = metaFields.map(f => if (f != null) f.copy() else null)
    new Spark40HoodieInternalRow(
      copyMetaFields,
      if (sourceRow == null) null else sourceRow.copy(),
      sourceContainsMetaFields)
  }
}
