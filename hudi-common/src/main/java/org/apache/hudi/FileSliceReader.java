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

package org.apache.hudi;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.internal.schema.InternalSchema;

import org.apache.avro.Schema;

import java.util.Iterator;
import java.util.Set;

public interface FileSliceReader extends AutoCloseable {

  /**
   * Projects output of this reader as a projection of the provided Hudi's internal schema.
   * NOTE: Provided schema could be an evolved schema.
   */
  FileSliceReader project(InternalSchema schema);

  /**
   * Projects output of this reader as a projection of the provided avro schema.
   */
  FileSliceReader project(Schema schema);

  /**
   * Pushes down filters to low-level file-format readers (if supported).
   */
  FileSliceReader pushDownFilters(Set<String> filters);

  /**
   * Specifies {@link HoodieTableQueryType} for this reader.
   */
  FileSliceReader readingMode(HoodieTableQueryType queryType);

  /**
   * Produces an iterable sequence of records from the given file slice.
   */
  Iterator<HoodieRecord> open(FileSlice fileSlice);
}
