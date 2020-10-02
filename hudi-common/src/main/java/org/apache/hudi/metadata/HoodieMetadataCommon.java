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

package org.apache.hudi.metadata;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Common declarations for Hoodie Metadata functionality.
 */
public class HoodieMetadataCommon {
  private static final Logger LOG = LogManager.getLogger(HoodieMetadataCommon.class);

  // Table name suffix
  protected static final String METADATA_TABLE_NAME_SUFFIX = "_metadata";

  // Timestamp for a commit when the base dataset had not had any commits yet.
  protected static final String SOLO_COMMIT_TIMESTAMP = "00000000000000";

  // Name of partition which saves file listings
  public static final String METADATA_PARTITION_NAME = "metadata_partition";
  // List of all partitions
  public static final String[] METADATA_ALL_PARTITIONS = {METADATA_PARTITION_NAME};
  // Key for the record which saves list of all partitions
  protected static final String RECORDKEY_PARTITION_LIST = "__all_partitions__";

  // The partition name used for non-partitioned tables
  protected static final String NON_PARTITIONED_NAME = ".";

}
