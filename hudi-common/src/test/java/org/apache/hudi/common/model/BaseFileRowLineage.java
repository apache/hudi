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

package org.apache.hudi.common.model;

import lombok.Value;

import java.io.Serializable;

/**
 * The values {@link org.apache.hudi.tableformat.TestTableFormat} assigns to a base file, from which it derives the
 * row lineage of every row in the file that stores none.
 */
@Value
public class BaseFileRowLineage implements Serializable {

  // Row ID of the first row in the base file; the row at position p of the file has the row ID firstRowId + p
  long firstRowId;
  // Sequence number the table format assigned to the base file
  long sequenceNumber;
}
