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

package org.apache.hudi.adapter;

import org.apache.flink.table.catalog.hive.util.Constants;

/**
 * Constants for Hive Catalog.
 */
public class HiveCatalogConstants {

  // -----------------------------------------------------------------------------------
  //  Constants for ALTER DATABASE
  // -----------------------------------------------------------------------------------
  public static final String ALTER_DATABASE_OP = Constants.ALTER_DATABASE_OP;

  public static final String DATABASE_LOCATION_URI = Constants.DATABASE_LOCATION_URI;

  public static final String DATABASE_OWNER_NAME = Constants.DATABASE_OWNER_NAME;

  public static final String DATABASE_OWNER_TYPE = Constants.DATABASE_OWNER_TYPE;

  public static final String ROLE_OWNER = Constants.ROLE_OWNER;

  public static final String USER_OWNER = Constants.USER_OWNER;

  /** Type of ALTER DATABASE operation. */
  public enum AlterHiveDatabaseOp {
        CHANGE_PROPS,
        CHANGE_LOCATION,
        CHANGE_OWNER
  }
}
