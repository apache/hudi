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

package org.apache.hudi.cli;

/**
 * Fields of print table header.
 */
public class HoodieTableHeaderFields {

  public static final String HEADER_PARTITION = "Partition";
  public static final String HEADER_PARTITION_PATH = HEADER_PARTITION + " Path";
  /**
   * Fields of Repair.
   */
  public static final String HEADER_METADATA_PRESENT = "Metadata Present?";
  public static final String HEADER_REPAIR_ACTION = "Action";
  public static final String HEADER_HOODIE_PROPERTY = "Property";
  public static final String HEADER_OLD_VALUE = "Old Value";
  public static final String HEADER_NEW_VALUE = "New Value";
}
