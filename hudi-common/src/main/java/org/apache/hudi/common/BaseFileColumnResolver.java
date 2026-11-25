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

import java.io.Serializable;

/**
 * Resolves the values of a {@link FormatMetaColumn} for the records of one base file that a rewrite carries over.
 */
public interface BaseFileColumnResolver extends Serializable {

  /**
   * Returns the value of the column for the record at the given position of the base file.
   *
   * @param rowPosition position of the record in the base file, starting at 0.
   * @param storedValue the value the base file stores for the record, null if the file has no such column or
   *                    stores null.
   */
  Object valueAt(long rowPosition, Object storedValue);
}
