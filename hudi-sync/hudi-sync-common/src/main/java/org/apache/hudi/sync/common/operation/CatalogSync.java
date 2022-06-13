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

package org.apache.hudi.sync.common.operation;

import org.apache.parquet.schema.MessageType;

import java.util.Map;

public interface CatalogSync {
  /**
   * Create the table.
   *
   * @param tableName         The table name.
   * @param storageSchema     The table schema.
   * @param inputFormatClass  The input format class of this table.
   * @param outputFormatClass The output format class of this table.
   * @param serdeClass        The serde class of this table.
   * @param serdeProperties   The serde properties of this table.
   * @param tableProperties   The table properties for this table.
   */
  void createTable(String tableName, MessageType storageSchema,
                   String inputFormatClass, String outputFormatClass,
                   String serdeClass, Map<String, String> serdeProperties,
                   Map<String, String> tableProperties);

  /**
   * @deprecated Use {@link #tableExists} instead.
   */
  @Deprecated
  boolean doesTableExist(String tableName);

  boolean tableExists(String tableName);

}
