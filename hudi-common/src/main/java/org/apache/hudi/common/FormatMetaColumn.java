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

package org.apache.hudi.common;

import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieNotSupportedException;

import java.io.Serializable;

/**
 * A per-row metadata column that an external table format declares for base files, see
 * {@link HoodieTableFormat#getFormatMetaColumns()}.
 *
 * <p>A rewrite of a base file stores the column after the columns of the table and follows the rule of the Hudi
 * meta fields: a record carried over unchanged from the base file keeps its value, resolved by
 * {@link #resolverFor}, a record that replaces a record of the base file with a changed version gets
 * {@link #valueOnUpdate}, and any other record written as new or changed gets {@link #valueOnWrite}. The column is
 * not a column of the table: it is not in the table schema, the commit metadata schema or the records a reader
 * returns.
 */
public interface FormatMetaColumn extends Serializable {

  /**
   * Returns the field of the column in the base files, a nullable long or int that defaults to null.
   */
  HoodieSchemaField field();

  /**
   * Returns the value of the column for a record that a write with the given instant writes into a base file as
   * new or changed. For compaction and clustering, the instant is the one of the compaction or clustering.
   *
   * @param instantTime instant time of the write.
   */
  Object valueOnWrite(String instantTime);

  /**
   * Returns the value of the column for a record that a write with the given instant writes into a base file as a
   * changed version of a record of the base file being rewritten, such as an update merged with the record it
   * updates. Returns {@link #valueOnWrite} unless the table format overrides it, for example to keep the value of
   * the replaced record.
   *
   * @param instantTime   instant time of the write.
   * @param replacedValue the value of the column for the replaced record, as {@link #resolverFor} resolves it.
   */
  default Object valueOnUpdate(String instantTime, Object replacedValue) {
    return valueOnWrite(instantTime);
  }

  /**
   * Returns the resolver of the values of the column for the records of a base file being rewritten, possibly
   * called from a distributed task. Throws if the table format cannot resolve the values for the base file, which
   * fails the write, as the records carried over from the file would lose their values.
   *
   * @param metaClient metaClient of the table.
   * @param baseFile   the base file being rewritten.
   */
  BaseFileColumnResolver resolverFor(HoodieTableMetaClient metaClient, HoodieBaseFile baseFile);

  /**
   * Fails if the table format declares format meta columns, for a rewrite of base files that does not carry them over.
   *
   * @param tableFormat the table format of the table
   * @param rewriter    the class that rewrites the base files
   */
  static void checkNotDeclared(HoodieTableFormat tableFormat, Class<?> rewriter) {
    if (!tableFormat.getFormatMetaColumns().isEmpty()) {
      throw new HoodieNotSupportedException("Table format " + tableFormat.getName() + " declares format meta columns, but "
          + rewriter.getSimpleName() + " does not carry them over to the base files it rewrites");
    }
  }
}
