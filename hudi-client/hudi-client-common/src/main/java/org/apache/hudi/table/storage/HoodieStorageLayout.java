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

package org.apache.hudi.table.storage;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import java.io.Serializable;

/**
 * Storage layout defines how the files are organized within a table.
 */
public abstract class HoodieStorageLayout implements Serializable {

  protected final HoodieWriteConfig config;

  public HoodieStorageLayout(HoodieWriteConfig config) {
    this.config = config;
  }

  /**
   * By default, layout does not directly control the total number of files.
   */
  public abstract boolean determinesNumFileGroups();

  /**
   * Return the layout specific partitioner for writing data, if any.
   */
  public abstract Option<String> layoutPartitionerClass();

  /**
   * Determines if the operation is supported by the layout.
   */
  public abstract boolean writeOperationSupported(WriteOperationType operationType);

  @EnumDescription("Determines how the files are organized within a table.")
  public enum LayoutType {

    @EnumFieldDescription("Each file group contains records of a certain set of keys, "
        + "without particular grouping criteria.")
    DEFAULT,

    @EnumFieldDescription("Each file group contains records of a set of keys which map "
        + "to a certain range of hash values, so that using the hash function can easily "
        + "identify the file group a record belongs to, based on the record key.")
    BUCKET
  }
}
