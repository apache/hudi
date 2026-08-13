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
import org.apache.spark.unsafe.types.{BinaryView, UTF8String}

class Spark42HoodieInternalRow(
    metaFields: Array[UTF8String],
    sourceRow: InternalRow,
    sourceContainsMetaFields: Boolean)
  extends Spark4HoodieInternalRow(metaFields, sourceRow, sourceContainsMetaFields) {

  override def getBinaryView(ordinal: Int): BinaryView = {
    ruleOutMetaFieldsAccess(ordinal, classOf[BinaryView])
    sourceRow.getBinaryView(rebaseOrdinal(ordinal))
  }

  override protected def newInternalRow(metaFields: Array[UTF8String],
                                        sourceRow: InternalRow,
                                        sourceContainsMetaFields: Boolean): Spark4HoodieInternalRow = {
    new Spark42HoodieInternalRow(metaFields, sourceRow, sourceContainsMetaFields)
  }
}
